import asyncio
import aiohttp
import feedparser
import json
import logging
import re
import sys
import hashlib
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlparse
from jinja2 import Template
from readability import Document
from zoneinfo import ZoneInfo

sys.stdout.reconfigure(encoding="utf-8")

# ── Config ────────────────────────────────────────────────────────────────────

OUTPUT_DIR = Path("docs")
POSTS_DIR = OUTPUT_DIR / "posts"
LOG_FILE = OUTPUT_DIR / "log.txt"
FEEDS_FILE = Path("feeds.txt")
INDEX_TMPL = Path("index.template.html")
POST_TMPL = Path("post.template.html")
STYLES_FILE = Path("styles.css")
STORE_FILE = Path("posts_store.json")

RELEVANT_DAYS = 2
TIMEOUT_SECS = 25
MAX_FEED_CONCURRENT = 15
MAX_SCRAPE_CONCURRENT = 4
MAX_RETRIES = 2
EAT = ZoneInfo("Africa/Nairobi")

USER_AGENT = "RectifierBot/1.0 (+https://pages.dev)"

# Exact host matches only
BLOCKLIST = {
    "www.metafilter.com",
    "twitter.com",
    "x.com",
    # Add exact hosts here:
    # "www.mothersalwaysright.com",
}

# ── Logging ───────────────────────────────────────────────────────────────────

def setup_logging():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

# ── Helpers ───────────────────────────────────────────────────────────────────

def title_to_filename(title: str) -> str:
    s = re.sub(r"[^\w\d]", " ", title)
    s = s.lower()
    s = "-".join(s.split()) + ".html"
    s = s.removeprefix("show-hn-")
    s = s.removeprefix("ask-hn-")
    if len(s) > 100:
        s = s[:96] + ".html"
    return s

def strip_html(html: str) -> str:
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def url_to_id(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()

def is_recent(published: datetime) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(days=RELEVANT_DAYS)
    return published > cutoff

def load_feeds() -> list[str]:
    with open(FEEDS_FILE, encoding="utf-8") as f:
        return [
            line.strip()
            for line in f
            if line.strip() and not line.startswith("#")
        ]

def parse_host(url: str) -> str:
    try:
        return urlparse(url).netloc
    except Exception:
        return ""

def is_blocked_host(host: str) -> bool:
    return host in BLOCKLIST

def parse_entry_date(entry) -> datetime | None:
    for field in ["published_parsed", "updated_parsed"]:
        val = getattr(entry, field, None)
        if val:
            try:
                return datetime(*val[:6], tzinfo=timezone.utc)
            except Exception:
                continue
    return None

# ── Persistent Store ──────────────────────────────────────────────────────────

def load_store() -> list[dict]:
    if not STORE_FILE.exists():
        return []
    try:
        with open(STORE_FILE, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        return []
    except Exception:
        logging.warning("Could not load posts_store.json, starting fresh")
        return []

def save_store(posts: list[dict]):
    with open(STORE_FILE, "w", encoding="utf-8") as f:
        json.dump(posts, f, indent=2, ensure_ascii=False)

def prune_store(posts: list[dict]) -> list[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=RELEVANT_DAYS)
    pruned = []
    for post in posts:
        try:
            pub = datetime.fromisoformat(post["published"])
            if pub.tzinfo is None:
                pub = pub.replace(tzinfo=timezone.utc)
            if pub > cutoff:
                pruned.append(post)
        except Exception:
            continue
    return pruned

def merge_posts(old_posts: list[dict], new_posts: list[dict]) -> list[dict]:
    merged = {}

    for post in old_posts:
        link = post.get("link")
        if link:
            merged[link] = post

    for post in new_posts:
        link = post.get("link")
        if not link:
            continue

        existing = merged.get(link)
        if not existing:
            merged[link] = post
            continue

        old_content = existing.get("content", "")
        new_content = post.get("content", "")

        if len(strip_html(new_content)) > len(strip_html(old_content)):
            merged[link] = post
        else:
            # Keep the better content, but refresh metadata if needed
            updated = existing.copy()
            updated["title"] = post.get("title") or existing.get("title")
            updated["published"] = post.get("published") or existing.get("published")
            updated["host"] = post.get("host") or existing.get("host")
            updated["filename"] = post.get("filename") or existing.get("filename")
            updated["feed_url"] = post.get("feed_url") or existing.get("feed_url")
            merged[link] = updated

    posts = list(merged.values())
    posts = prune_store(posts)
    posts.sort(key=lambda p: p["published"], reverse=True)
    return posts

# ── HTTP ──────────────────────────────────────────────────────────────────────

async def fetch_text(
    session: aiohttp.ClientSession,
    url: str,
    timeout_secs: int = TIMEOUT_SECS,
) -> tuple[int, str, str]:
    headers = {"User-Agent": USER_AGENT}

    for attempt in range(MAX_RETRIES + 1):
        try:
            async with session.get(
                url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=timeout_secs),
            ) as response:
                text = await response.text(errors="ignore")
                content_type = response.headers.get("content-type", "")
                return response.status, text, content_type

        except asyncio.TimeoutError:
            if attempt < MAX_RETRIES:
                await asyncio.sleep(1 + attempt)
                continue
            logging.warning(f"Timeout: {url}")
            return 0, "", ""

        except Exception as e:
            if attempt < MAX_RETRIES:
                await asyncio.sleep(1 + attempt)
                continue
            logging.warning(f"Fetch failed {url}: {e}")
            return 0, "", ""

    return 0, "", ""

# ── Content Extraction ────────────────────────────────────────────────────────

def extract_entry_content(entry) -> str:
    for field in ["content", "summary"]:
        val = getattr(entry, field, None)
        if not val:
            continue

        if isinstance(val, list):
            value = val[0].get("value", "")
        else:
            value = str(val)

        if value:
            return value

    return ""

async def fetch_article_content(
    session: aiohttp.ClientSession,
    url: str,
    semaphore: asyncio.Semaphore,
) -> str:
    host = parse_host(url)
    if not host or is_blocked_host(host):
        return ""

    async with semaphore:
        status, html, content_type = await fetch_text(session, url)
        if status != 200:
            return ""
        if "text/html" not in content_type:
            return ""

        try:
            doc = Document(html)
            content = doc.summary()
            if not content:
                return ""
            if len(strip_html(content)) < 200:
                return ""
            return content
        except Exception as e:
            logging.info(f"Could not parse article {url}: {e}")
            return ""

async def process_entry(
    session: aiohttp.ClientSession,
    entry,
    feed_url: str,
    scrape_semaphore: asyncio.Semaphore,
) -> dict | None:
    link = getattr(entry, "link", None)
    if not link:
        return None

    host = parse_host(link)
    if not host:
        return None

    # Exact-host block only
    if is_blocked_host(host):
        return None

    published = parse_entry_date(entry)
    if not published:
        return None

    if not is_recent(published):
        return None

    title = getattr(entry, "title", "Untitled")
    title = re.sub(r"<[^>]+>", "", title).strip() or "Untitled"

    content = extract_entry_content(entry)
    if len(strip_html(content)) < 200:
        scraped = await fetch_article_content(session, link, scrape_semaphore)
        if scraped:
            content = scraped

    return {
        "id": url_to_id(link),
        "link": link,
        "title": title,
        "published": published.isoformat(),
        "host": host,
        "content": content,
        "filename": title_to_filename(title),
        "feed_url": feed_url,
    }

# ── Feed Fetching ─────────────────────────────────────────────────────────────

async def fetch_feed(
    session: aiohttp.ClientSession,
    feed_url: str,
    feed_semaphore: asyncio.Semaphore,
    scrape_semaphore: asyncio.Semaphore,
) -> list[dict]:
    async with feed_semaphore:
        status, text, _ = await fetch_text(session, feed_url)
        if status != 200:
            if status:
                logging.warning(f"HTTP {status} for {feed_url}")
            return []

        try:
            feed = feedparser.parse(text)
        except Exception as e:
            logging.warning(f"Feed parse error {feed_url}: {e}")
            return []

        posts = []
        for entry in feed.entries:
            try:
                post = await process_entry(
                    session, entry, feed_url, scrape_semaphore
                )
                if post:
                    posts.append(post)
            except Exception as e:
                logging.warning(f"Entry error in {feed_url}: {e}")

        logging.info(f"Got {len(posts)} posts from {feed_url}")
        return posts

async def fetch_all_posts(feeds: list[str]) -> list[dict]:
    feed_semaphore = asyncio.Semaphore(MAX_FEED_CONCURRENT)
    scrape_semaphore = asyncio.Semaphore(MAX_SCRAPE_CONCURRENT)
    connector = aiohttp.TCPConnector(limit=MAX_FEED_CONCURRENT)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            fetch_feed(session, feed_url, feed_semaphore, scrape_semaphore)
            for feed_url in feeds
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    posts = []
    seen_links = set()

    for result in results:
        if isinstance(result, Exception):
            logging.warning(f"Task error: {result}")
            continue

        for post in result:
            if post["link"] not in seen_links:
                posts.append(post)
                seen_links.add(post["link"])

    posts.sort(key=lambda p: p["published"], reverse=True)
    return posts

# ── HTML Generation ───────────────────────────────────────────────────────────

def build_site(posts: list[dict], total_feeds: int):
    logging.info("Building site...")

    recent_posts = prune_store(posts)
    recent_posts.sort(key=lambda p: p["published"], reverse=True)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    POSTS_DIR.mkdir(parents=True, exist_ok=True)

    if STYLES_FILE.exists():
        (OUTPUT_DIR / "styles.css").write_text(
            STYLES_FILE.read_text(encoding="utf-8"),
            encoding="utf-8",
        )

    active_filenames = set()
    for post in recent_posts:
        if post.get("content"):
            active_filenames.add(post["filename"])

    for html_file in POSTS_DIR.glob("*.html"):
        if html_file.name not in active_filenames:
            html_file.unlink()
            logging.info(f"Deleted old post: {html_file.name}")

    post_tmpl = Template(POST_TMPL.read_text(encoding="utf-8"))

    written_filenames = set()
    for post in recent_posts:
        if not post.get("content"):
            logging.info(f"Skipping post, no content: {post['link']}")
            continue

        filename = post["filename"]
        if filename in written_filenames:
            base = filename.replace(".html", "")
            filename = f"{base}-2.html"
            post["filename"] = filename

        written_filenames.add(filename)

        try:
            html = post_tmpl.render(
                title=post["title"],
                original=post["link"],
                content=post["content"],
            )
            (POSTS_DIR / filename).write_text(html, encoding="utf-8")
        except Exception as e:
            logging.warning(f"Could not write post {filename}: {e}")

    display_posts = []
    for post in recent_posts:
        p = post.copy()
        try:
            pub = datetime.fromisoformat(post["published"])
            if pub.tzinfo is None:
                pub = pub.replace(tzinfo=timezone.utc)
            p["published_iso"] = pub.isoformat()
            p["published"] = ""
        except Exception:
            p["published_iso"] = ""
            p["published"] = ""
        display_posts.append(p)

    index_tmpl = Template(INDEX_TMPL.read_text(encoding="utf-8"))
    html = index_tmpl.render(
        posts=display_posts,
        last_updated=datetime.now(EAT).strftime("%B %d, %Y · %I:%M %p EAT"),
        feeds_collected=total_feeds,
        total_feeds=total_feeds,
    )
    (OUTPUT_DIR / "index.html").write_text(html, encoding="utf-8")

    logging.info(
        f"Site built: {len(display_posts)} posts, "
        f"{len(written_filenames)} post pages written"
    )

# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    setup_logging()

    feeds = load_feeds()
    total = len(feeds)
    logging.info(f"Loaded {total} feeds")

    if total == 0:
        logging.error("No feeds found in feeds.txt - exiting")
        return

    old_posts = load_store()
    logging.info(f"Loaded {len(old_posts)} stored posts")

    new_posts = await fetch_all_posts(feeds)
    logging.info(f"Fetched {len(new_posts)} unique posts this run")

    all_posts = merge_posts(old_posts, new_posts)
    save_store(all_posts)
    logging.info(f"Saved {len(all_posts)} accumulated recent posts")

    build_site(all_posts, total)
    logging.info("Done")

if __name__ == "__main__":
    asyncio.run(main())