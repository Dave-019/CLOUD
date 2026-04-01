import asyncio
import aiohttp
import feedparser
import json
import logging
import re
import sys
import hashlib
import os
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

RELEVANT_DAYS = 2
TIMEOUT_SECS = 25
MAX_FEED_CONCURRENT = 15
MAX_SCRAPE_CONCURRENT = 4
MAX_RETRIES = 2
EAT = ZoneInfo("Africa/Nairobi")

USER_AGENT = "RectifierBot/1.0 (+https://pages.dev)"

# Cloudflare D1 config
CF_ACCOUNT_ID = os.environ.get("CLOUDFLARE_ACCOUNT_ID", "")
CF_API_TOKEN = os.environ.get("CLOUDFLARE_API_TOKEN", "")
D1_DATABASE_ID = os.environ.get("D1_DATABASE_ID", "")

# Exact host matches only
BLOCKLIST = {
    "www.metafilter.com",
    "twitter.com",
    "x.com",
    "www.mothersalwaysright.com",
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

# ── D1 Database ───────────────────────────────────────────────────────────────

def d1_url() -> str:
    return (
        f"https://api.cloudflare.com/client/v4/accounts/"
        f"{CF_ACCOUNT_ID}/d1/database/{D1_DATABASE_ID}/query"
    )

def d1_headers() -> dict:
    return {
        "Authorization": f"Bearer {CF_API_TOKEN}",
        "Content-Type": "application/json",
    }

async def d1_query(
    session: aiohttp.ClientSession,
    sql: str,
    params: list = None,
) -> list[dict]:
    if not CF_ACCOUNT_ID or not CF_API_TOKEN or not D1_DATABASE_ID:
        logging.warning("D1 credentials missing, skipping DB operation")
        return []

    body = {"sql": sql}
    if params:
        body["params"] = params

    try:
        async with session.post(
            d1_url(),
            headers=d1_headers(),
            json=body,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as response:
            data = await response.json()
            if not data.get("success"):
                errors = data.get("errors", [])
                logging.warning(f"D1 query failed: {errors}")
                return []
            results = data.get("result", [])
            if results:
                return results[0].get("results", [])
            return []
    except Exception as e:
        logging.warning(f"D1 request error: {e}")
        return []

async def save_posts_to_d1(
    session: aiohttp.ClientSession,
    posts: list[dict],
):
    if not posts:
        return

    logging.info(f"Saving {len(posts)} posts to D1...")
    saved = 0
    failed = 0

    for post in posts:
        content_text = strip_html(post.get("content", ""))
        sql = """
            INSERT INTO posts (
                id, title, url, host, published,
                feed_url, content_text, fetched_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                title = excluded.title,
                content_text = CASE
                    WHEN length(excluded.content_text) > length(posts.content_text)
                    THEN excluded.content_text
                    ELSE posts.content_text
                END,
                fetched_at = excluded.fetched_at
        """
        params = [
            post["id"],
            post["title"],
            post["link"],
            post["host"],
            post["published"],
            post["feed_url"],
            content_text,
            datetime.now(timezone.utc).isoformat(),
        ]

        result = await d1_query(session, sql, params)
        if result is not None:
            saved += 1
        else:
            failed += 1

    logging.info(f"D1 save complete: {saved} saved, {failed} failed")

async def load_recent_posts_from_d1(
    session: aiohttp.ClientSession,
) -> list[dict]:
    cutoff = (
        datetime.now(timezone.utc) - timedelta(days=RELEVANT_DAYS)
    ).isoformat()

    sql = """
        SELECT id, title, url, host, published, feed_url, content_text
        FROM posts
        WHERE published > ?
        ORDER BY published DESC
    """

    rows = await d1_query(session, sql, [cutoff])
    logging.info(f"Loaded {len(rows)} recent posts from D1")
    return rows

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
        scraped = await fetch_article_content(
            session, link, scrape_semaphore
        )
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

def build_site(posts: list[dict], d1_posts: list[dict], total_feeds: int):
    logging.info("Building site...")

    # Use freshly fetched posts for homepage
    # D1 posts are stored permanently but homepage only shows recent
    seen = set()
    all_posts = []

    for post in posts:
        if post["link"] not in seen:
            all_posts.append(post)
            seen.add(post["link"])

    cutoff = datetime.now(timezone.utc) - timedelta(days=RELEVANT_DAYS)
    recent_posts = []
    for post in all_posts:
        try:
            pub = datetime.fromisoformat(post["published"])
            if pub.tzinfo is None:
                pub = pub.replace(tzinfo=timezone.utc)
            if pub > cutoff:
                recent_posts.append(post)
        except Exception:
            continue

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
        last_updated=datetime.now(EAT).strftime(
            "%B %d, %Y · %I:%M %p EAT"
        ),
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

    # Fetch fresh posts from feeds
    new_posts = await fetch_all_posts(feeds)
    logging.info(f"Fetched {len(new_posts)} unique posts this run")

    # Save to D1 and load recent for display
    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        await save_posts_to_d1(session, new_posts)
        d1_posts = await load_recent_posts_from_d1(session)

    # Build site from fresh posts
    build_site(new_posts, d1_posts, total)
    logging.info("Done")

if __name__ == "__main__":
    asyncio.run(main())