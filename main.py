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
from zoneinfo import ZoneInfo

sys.stdout.reconfigure(encoding="utf-8")

# ── Config ────────────────────────────────────────────────────────────────────

OUTPUT_DIR = Path("docs")
LOG_FILE = OUTPUT_DIR / "log.txt"
FEEDS_FILE = Path("feeds.txt")
INDEX_TMPL = Path("index.template.html")
STYLES_FILE = Path("styles.css")

RELEVANT_DAYS = 1
TIMEOUT_SECS = 25
MAX_FEED_CONCURRENT = 20
MAX_RETRIES = 2
EAT = ZoneInfo("Africa/Nairobi")

USER_AGENT = "RectifierBot/1.0 (+https://pages.dev)"

# Cloudflare D1 config
CF_ACCOUNT_ID = os.environ.get("CLOUDFLARE_ACCOUNT_ID", "")
CF_API_TOKEN = os.environ.get("CLOUDFLARE_API_TOKEN", "")
D1_DATABASE_ID = os.environ.get("D1_DATABASE_ID", "")

BLOCKLIST = {
    "www.metafilter.com",
    "twitter.com",
    "x.com",
    "simonwillison.net",
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

async def save_posts_to_d1(session: aiohttp.ClientSession, posts: list[dict]):
    if not posts:
        return

    logging.info(f"Saving {len(posts)} posts to D1...")
    saved = 0
    failed = 0

    for post in posts:
        sql = """
            INSERT INTO posts (
                id, title, url, host, published,
                feed_url, content_text, fetched_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                title = excluded.title,
                fetched_at = excluded.fetched_at
        """
        params = [
            post["id"],
            post["title"],
            post["link"],
            post["host"],
            post["published"],
            post["feed_url"],
            post.get("summary", ""),
            datetime.now(timezone.utc).isoformat(),
        ]

        result = await d1_query(session, sql, params)
        if result is not None:
            saved += 1
        else:
            failed += 1

    logging.info(f"D1 save complete: {saved} saved, {failed} failed")

# ── HTTP & Feed Fetching ──────────────────────────────────────────────────────

async def fetch_text(
    session: aiohttp.ClientSession,
    url: str,
    timeout_secs: int = TIMEOUT_SECS,
) -> tuple[int, str]:
    headers = {"User-Agent": USER_AGENT}

    for attempt in range(MAX_RETRIES + 1):
        try:
            async with session.get(
                url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=timeout_secs),
            ) as response:
                text = await response.text(errors="ignore")
                return response.status, text

        except Exception as e:
            if attempt < MAX_RETRIES:
                await asyncio.sleep(1 + attempt)
                continue
            logging.warning(f"Fetch failed {url}: {e}")
            return 0, ""

    return 0, ""

def process_entry(entry, feed_url: str) -> dict | None:
    link = getattr(entry, "link", None)
    if not link:
        return None

    host = parse_host(link)
    if not host or is_blocked_host(host):
        return None

    published = parse_entry_date(entry)
    if not published or not is_recent(published):
        return None

    title = getattr(entry, "title", "Untitled")
    title = re.sub(r"<[^>]+>", "", title).strip() or "Untitled"

    # Grab short summary if available
    summary = strip_html(getattr(entry, "summary", ""))

    return {
        "id": url_to_id(link),
        "link": link,
        "title": title,
        "published": published.isoformat(),
        "host": host,
        "summary": summary,
        "feed_url": feed_url,
    }

async def fetch_feed(
    session: aiohttp.ClientSession,
    feed_url: str,
    feed_semaphore: asyncio.Semaphore,
) -> list[dict]:
    async with feed_semaphore:
        status, text = await fetch_text(session, feed_url)
        if status != 200:
            return []

        try:
            feed = feedparser.parse(text)
        except Exception as e:
            logging.warning(f"Feed parse error {feed_url}: {e}")
            return []

        posts = []
        for entry in feed.entries:
            try:
                post = process_entry(entry, feed_url)
                if post:
                    posts.append(post)
            except Exception as e:
                logging.warning(f"Entry error in {feed_url}: {e}")

        return posts

async def fetch_all_posts(feeds: list[str]) -> list[dict]:
    feed_semaphore = asyncio.Semaphore(MAX_FEED_CONCURRENT)
    connector = aiohttp.TCPConnector(limit=MAX_FEED_CONCURRENT)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_feed(session, feed_url, feed_semaphore) for feed_url in feeds]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    posts = []
    seen_links = set()

    for result in results:
        if isinstance(result, Exception):
            continue
        for post in result:
            if post["link"] not in seen_links:
                posts.append(post)
                seen_links.add(post["link"])

    posts.sort(key=lambda p: p["published"], reverse=True)
    return posts

# ── HTML Generation ───────────────────────────────────────────────────────────

def build_site(posts: list[dict], total_feeds: int):
    logging.info("Building homepage...")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Copy CSS
    if STYLES_FILE.exists():
        (OUTPUT_DIR / "styles.css").write_text(
            STYLES_FILE.read_text(encoding="utf-8"),
            encoding="utf-8",
        )

    # Filter for last 24h
    cutoff = datetime.now(timezone.utc) - timedelta(days=RELEVANT_DAYS)
    recent_posts = []
    for post in posts:
        try:
            pub = datetime.fromisoformat(post["published"])
            if pub.tzinfo is None:
                pub = pub.replace(tzinfo=timezone.utc)
            if pub > cutoff:
                recent_posts.append(post)
        except Exception:
            continue

    recent_posts.sort(key=lambda p: p["published"], reverse=True)

    # Render index.html
    index_tmpl = Template(INDEX_TMPL.read_text(encoding="utf-8"))
    html = index_tmpl.render(
        posts=recent_posts,
        last_updated=datetime.now(EAT).strftime("%B %d, %Y · %I:%M %p EAT"),
        feeds_collected=total_feeds,
        total_feeds=total_feeds,
    )
    (OUTPUT_DIR / "index.html").write_text(html, encoding="utf-8")
    logging.info(f"Site built: {len(recent_posts)} posts displayed.")

# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    setup_logging()

    feeds = load_feeds()
    total = len(feeds)
    logging.info(f"Loaded {total} feeds")

    if total == 0:
        return

    new_posts = await fetch_all_posts(feeds)
    logging.info(f"Fetched {len(new_posts)} posts")

    # Save to D1
    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        await save_posts_to_d1(session, new_posts)

    # Build homepage only
    build_site(new_posts, total)
    logging.info("Done")

if __name__ == "__main__":
    asyncio.run(main())
