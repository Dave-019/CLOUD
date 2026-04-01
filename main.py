import asyncio
import aiohttp
import feedparser
import json
import logging
import os
import re
import math
import sys
import hashlib
import base64
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlparse
from jinja2 import Template
from readability import Document
from zoneinfo import ZoneInfo

# Fix Windows encoding
sys.stdout.reconfigure(encoding='utf-8')

# ── Config ────────────────────────────────────────────────────────────────────

OUTPUT_DIR    = Path("docs")
POSTS_DIR     = OUTPUT_DIR / "posts"
BATCH_DIR     = Path("batches")
LOG_FILE      = OUTPUT_DIR / "log.txt"
HISTORY_FILE  = OUTPUT_DIR / "history.txt"
FEEDS_FILE    = Path("feeds.txt")
INDEX_TMPL    = Path("index.template.html")
POST_TMPL     = Path("post.template.html")
STYLES_FILE   = Path("styles.css")

BATCH_SIZE     = 500
RELEVANT_DAYS  = 2      # show posts max 2 days old
TIMEOUT_SECS   = 30
MAX_CONCURRENT = 50
EAT            = ZoneInfo("Africa/Nairobi")

# Content repo config
CONTENT_REPO   = "Dave-019/content"
GITHUB_TOKEN   = os.environ.get("GITHUB_TOKEN", "")

BLOCKLIST = {
    "www.metafilter.com",
    "twitter.com",
    "x.com",
}

# ── Logging ───────────────────────────────────────────────────────────────────

def setup_logging():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding='utf-8'),
            logging.StreamHandler(sys.stdout),
        ]
    )

# ── Helpers ───────────────────────────────────────────────────────────────────

def title_to_filename(title: str) -> str:
    s = re.sub(r'[^\w\d]', ' ', title)
    s = s.lower()
    s = '-'.join(s.split()) + '.html'
    s = s.removeprefix('show-hn-')
    s = s.removeprefix('ask-hn-')
    if len(s) > 100:
        s = s[:100] + '.html'
    return s

def url_to_id(url: str) -> str:
    """Create unique ID from URL using hash."""
    return hashlib.md5(url.encode()).hexdigest()

def is_recent(published: datetime) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(days=RELEVANT_DAYS)
    return published > cutoff

def load_feeds() -> list[str]:
    with open(FEEDS_FILE, encoding='utf-8') as f:
        return [
            line.strip()
            for line in f
            if line.strip() and not line.startswith('#')
        ]

def get_batch_number(total_feeds: int) -> int:
    hour = datetime.now().hour
    return hour % 6

def get_batch_file(batch_num: int) -> Path:
    return BATCH_DIR / f"batch{batch_num}.json"

def strip_html(html: str) -> str:
    """Strip HTML tags to get plain text for search."""
    text = re.sub(r'<[^>]+>', ' ', html)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

# ── History ───────────────────────────────────────────────────────────────────

def read_history() -> set[str]:
    if not HISTORY_FILE.exists():
        return set()
    with open(HISTORY_FILE, encoding='utf-8') as f:
        return {line.strip() for line in f if line.strip()}

def write_history(history: set[str]):
    with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
        f.write('\n'.join(sorted(history)) + '\n')

# ── Batch Storage ─────────────────────────────────────────────────────────────

def load_all_batches() -> list[dict]:
    """Load metadata only from all batch files."""
    BATCH_DIR.mkdir(exist_ok=True)
    all_posts = []
    for i in range(6):
        batch_file = get_batch_file(i)
        if batch_file.exists():
            with open(batch_file, encoding='utf-8') as f:
                try:
                    data = json.load(f)
                    all_posts.extend(data.get('posts', []))
                    logging.info(
                        f"Loaded {len(data.get('posts', []))} posts "
                        f"from {batch_file}"
                    )
                except json.JSONDecodeError:
                    logging.warning(f"Could not parse {batch_file}")
    return all_posts

def save_batch(batch_num: int, posts: list[dict]):
    """Save metadata only to batch file."""
    BATCH_DIR.mkdir(exist_ok=True)
    batch_file = get_batch_file(batch_num)

    # Strip content before saving to keep batch files small
    metadata_posts = []
    for post in posts:
        p = post.copy()
        p.pop('content', None)  # remove content
        metadata_posts.append(p)

    with open(batch_file, 'w', encoding='utf-8') as f:
        json.dump({
            'batch':   batch_num,
            'updated': datetime.now(timezone.utc).isoformat(),
            'posts':   metadata_posts
        }, f, indent=2, default=str, ensure_ascii=False)
    logging.info(f"Saved {len(metadata_posts)} posts to {batch_file}")

# ── Content Repo ──────────────────────────────────────────────────────────────

async def push_to_content_repo(
    session: aiohttp.ClientSession,
    post: dict
):
    """Push full content to Dave-019/content repo."""
    if not GITHUB_TOKEN:
        logging.warning("No GITHUB_TOKEN, skipping content push")
        return

    if not post.get('content'):
        return

    file_id      = url_to_id(post['link'])
    filename     = f"{file_id}.json"
    api_url      = (
        f"https://api.github.com/repos/{CONTENT_REPO}"
        f"/contents/{filename}"
    )

    # Content to store
    content_data = {
        "id":           file_id,
        "title":        post['title'],
        "description":  post.get('description', ''),
        "content_text": strip_html(post.get('content', '')),
        "url":          post['link'],
        "host":         post['host'],
        "published":    post['published'],
        "feed_url":     post['feed_url'],
        "fetched_at":   datetime.now(timezone.utc).isoformat(),
    }

    # Base64 encode for GitHub API
    content_json    = json.dumps(
        content_data, indent=2, ensure_ascii=False
    )
    content_encoded = base64.b64encode(
        content_json.encode('utf-8')
    ).decode('utf-8')

    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept":        "application/vnd.github.v3+json",
        "Content-Type":  "application/json",
    }

    # Check if file exists (need SHA to update)
    sha = None
    try:
        async with session.get(
            api_url,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                sha  = data.get('sha')
    except Exception:
        pass

    # Create or update file
    payload = {
        "message": f"Add content: {post['title'][:50]}",
        "content": content_encoded,
    }
    if sha:
        payload["sha"] = sha

    try:
        async with session.put(
            api_url,
            headers=headers,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=30)
        ) as resp:
            if resp.status in (200, 201):
                logging.info(
                    f"Pushed to content repo: {post['title'][:40]}"
                )
            else:
                logging.warning(
                    f"Content repo push failed {resp.status}: "
                    f"{post['title'][:40]}"
                )
    except Exception as e:
        logging.warning(f"Content repo error: {e}")

# ── Feed Fetching ─────────────────────────────────────────────────────────────

async def fetch_feed(
    session: aiohttp.ClientSession,
    feed_url: str,
    history: set[str],
    semaphore: asyncio.Semaphore
) -> list[dict]:
    async with semaphore:
        try:
            headers = {
                'User-Agent': (
                    'Mozilla/5.0 (compatible; Googlebot/2.1; '
                    '+http://www.google.com/bot.html)'
                )
            }
            async with session.get(
                feed_url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECS),
                ssl=False
            ) as response:
                if response.status != 200:
                    logging.warning(
                        f"HTTP {response.status} for {feed_url}"
                    )
                    return []

                content = await response.text()
                feed    = feedparser.parse(content)

                posts = []
                for entry in feed.entries:
                    post = await process_entry(
                        session, entry, feed_url, history
                    )
                    if post:
                        posts.append(post)

                logging.info(
                    f"Got {len(posts)} posts from {feed_url}"
                )
                return posts

        except asyncio.TimeoutError:
            logging.warning(f"Timeout fetching {feed_url}")
            return []
        except Exception as e:
            logging.warning(f"Error fetching {feed_url}: {e}")
            return []

async def process_entry(
    session: aiohttp.ClientSession,
    entry,
    feed_url: str,
    history: set[str]
) -> dict | None:

    link = getattr(entry, 'link', None)
    if not link:
        return None

    try:
        parsed = urlparse(link)
        host   = parsed.netloc
    except Exception:
        return None

    if host in BLOCKLIST:
        return None

    published = None
    for date_field in ['published_parsed', 'updated_parsed']:
        val = getattr(entry, date_field, None)
        if val:
            try:
                published = datetime(*val[:6], tzinfo=timezone.utc)
                break
            except Exception:
                continue

    if not published:
        published = datetime.now(timezone.utc)

    if not is_recent(published):
        return None

    title = getattr(entry, 'title', 'Untitled')
    title = re.sub(r'<[^>]+>', '', title).strip()
    if not title:
        title = 'Untitled'

    # Get content
    content = ''
    for content_field in ['content', 'summary']:
        val = getattr(entry, content_field, None)
        if val:
            if isinstance(val, list):
                content = val[0].get('value', '')
            else:
                content = str(val)
            if content:
                break

    if not content:
        content = await fetch_article_content(session, link)

    filename = title_to_filename(title)

    return {
        'link':      link,
        'title':     title,
        'published': published.isoformat(),
        'host':      host,
        'content':   content,
        'filename':  filename,
        'feed_url':  feed_url,
    }

async def fetch_article_content(
    session: aiohttp.ClientSession,
    url: str
) -> str:
    try:
        headers = {
            'User-Agent': (
                'Mozilla/5.0 (compatible; Googlebot/2.1; '
                '+http://www.google.com/bot.html)'
            )
        }
        async with session.get(
            url,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECS),
            ssl=False
        ) as response:
            content_type = response.headers.get('content-type', '')
            if not content_type.startswith('text/html'):
                return ''

            html     = await response.text()
            doc      = Document(html)
            return doc.summary()

    except Exception as e:
        logging.warning(f"Could not fetch content from {url}: {e}")
        return ''

# ── Batch Runner ──────────────────────────────────────────────────────────────

async def run_batch(
    batch_num: int,
    feeds: list[str],
    history: set[str]
) -> list[dict]:
    logging.info(
        f"Running batch {batch_num} with {len(feeds)} feeds"
    )

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT)

    async with aiohttp.ClientSession(connector=connector) as session:
        # Fetch all feeds
        tasks   = [
            fetch_feed(session, url, history, semaphore)
            for url in feeds
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Flatten results
        posts      = []
        seen_links = set()
        for result in results:
            if isinstance(result, Exception):
                logging.warning(f"Batch task error: {result}")
                continue
            for post in result:
                if post['link'] not in seen_links:
                    posts.append(post)
                    seen_links.add(post['link'])

        # Push content to content repo concurrently
        if GITHUB_TOKEN:
            content_tasks = [
                push_to_content_repo(session, post)
                for post in posts
                if post.get('content')
            ]
            await asyncio.gather(
                *content_tasks,
                return_exceptions=True
            )

    # Save metadata only to batch file
    save_batch(batch_num, posts)
    logging.info(
        f"Batch {batch_num} complete: {len(posts)} unique posts"
    )
    return posts

# ── HTML Generation ───────────────────────────────────────────────────────────

def build_site(total_feeds: int):
    logging.info("Building site...")

    all_posts = load_all_batches()

    # Filter to 2 days only
    cutoff      = datetime.now(timezone.utc) - timedelta(days=RELEVANT_DAYS)
    recent_posts = []
    for post in all_posts:
        try:
            pub = datetime.fromisoformat(post['published'])
            if pub.tzinfo is None:
                pub = pub.replace(tzinfo=timezone.utc)
            if pub > cutoff:
                recent_posts.append(post)
        except Exception as e:
            logging.warning(f"Could not parse date: {e}")
            continue

    # Deduplicate
    seen         = set()
    unique_posts = []
    for post in recent_posts:
        if post['link'] not in seen:
            unique_posts.append(post)
            seen.add(post['link'])

    # Sort newest first
    unique_posts.sort(
        key=lambda p: p['published'],
        reverse=True
    )

    logging.info(f"Building site with {len(unique_posts)} posts")

    OUTPUT_DIR.mkdir(exist_ok=True)
    POSTS_DIR.mkdir(exist_ok=True)

    # Copy styles
    if STYLES_FILE.exists():
        (OUTPUT_DIR / "styles.css").write_text(
            STYLES_FILE.read_text(encoding='utf-8'),
            encoding='utf-8'
        )

    # Delete post HTML files older than 2 days
    active_filenames = {
        post['filename'] for post in unique_posts
    }
    if POSTS_DIR.exists():
        for html_file in POSTS_DIR.glob('*.html'):
            if html_file.name not in active_filenames:
                html_file.unlink()
                logging.info(f"Deleted old post: {html_file.name}")

    # Write post HTML files
    post_tmpl_str = POST_TMPL.read_text(encoding='utf-8')
    post_tmpl     = Template(post_tmpl_str)

    written_filenames = set()
    for post in unique_posts:
        if not post.get('content'):
            logging.info(
                f"Skipping post, no content: {post['link']}"
            )
            continue

        filename = post['filename']
        if filename in written_filenames:
            base     = filename.replace('.html', '')
            filename = f"{base}-2.html"
            post['filename'] = filename

        written_filenames.add(filename)

        try:
            html      = post_tmpl.render(
                title    = post['title'],
                original = post['link'],
                content  = post['content'],
            )
            post_file = POSTS_DIR / filename
            post_file.write_text(html, encoding='utf-8')
        except Exception as e:
            logging.warning(f"Could not write post {filename}: {e}")
            continue

    # Count batches
    batch_count     = sum(
        1 for i in range(6)
        if get_batch_file(i).exists()
    )
    feeds_collected = min(batch_count * BATCH_SIZE, total_feeds)

    # Format dates for display
    display_posts = []
    for post in unique_posts:
        p = post.copy()
        try:
            pub     = datetime.fromisoformat(post['published'])
            if pub.tzinfo is None:
                pub = pub.replace(tzinfo=timezone.utc)
            p['published_iso'] = pub.isoformat()
            p['published']     = ''
        except Exception:
            p['published_iso'] = ''
            p['published']     = ''
        display_posts.append(p)

    # Write index.html
    index_tmpl_str = INDEX_TMPL.read_text(encoding='utf-8')
    index_tmpl     = Template(index_tmpl_str)
    html           = index_tmpl.render(
        posts           = display_posts,
        last_updated    = datetime.now(EAT).strftime(
            '%B %d, %Y · %I:%M %p EAT'
        ),
        feeds_collected = feeds_collected,
        total_feeds     = total_feeds,
    )
    (OUTPUT_DIR / "index.html").write_text(html, encoding='utf-8')
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

    batch_num  = get_batch_number(total)
    batch_size = math.ceil(total / 6)
    start      = batch_num * batch_size
    end        = min(start + batch_size, total)
    batch_feeds = feeds[start:end]

    logging.info(
        f"Batch {batch_num}: feeds {start}-{end} "
        f"({len(batch_feeds)} feeds)"
    )

    history  = read_history()
    logging.info(f"Loaded {len(history)} links from history")

    new_posts = await run_batch(batch_num, batch_feeds, history)

    for post in new_posts:
        history.add(post['link'])
    write_history(history)

    build_site(total)

if __name__ == "__main__":
    asyncio.run(main())