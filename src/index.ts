import { XMLParser } from "fast-xml-parser";
import { FEED_LIST } from "./feeds";

export interface Env {
  ASSETS: Fetcher;
  FEED_CACHE: KVNamespace;
  USER_AGENT: string;
  RELEVANT_DAYS: string;
  MAX_CONCURRENCY: string;
}

interface Post {
  id: string;
  link: string;
  title: string;
  published: string; // ISO
  host: string;
  summary: string;
  feedUrl: string;
}

const BLOCKLIST = new Set([
  "www.metafilter.com",
  "twitter.com",
  "x.com",
  "simonwillison.net",
]);

const FEEDS: string[] = FEED_LIST;


const PAGE_KEY = "page:index";

/* ── Helpers ─────────────────────────────────────────────────────────── */

function stripHtml(html: string): string {
  return html.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
}

function escapeHtml(s: string): string {
  return s
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

async function md5Hex(input: string): Promise<string> {
  const digest = await crypto.subtle.digest("MD5", new TextEncoder().encode(input)).catch(() => null);
  if (digest) {
    return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
  }
  // MD5 is not in Workers WebCrypto; fall back to a deterministic hash.
  const sha = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(input));
  return [...new Uint8Array(sha)].slice(0, 16).map((b) => b.toString(16).padStart(2, "0")).join("");
}

function hostOf(url: string): string {
  try {
    return new URL(url).host;
  } catch {
    return "";
  }
}

function relevantDays(env: Env): number {
  const n = parseInt(env.RELEVANT_DAYS, 10);
  return Number.isFinite(n) && n > 0 ? n : 1;
}

function maxConcurrency(env: Env): number {
  const n = parseInt(env.MAX_CONCURRENCY, 10);
  return Number.isFinite(n) && n > 0 ? n : 20;
}

/* ── Feed parsing (RSS 2.0 + Atom) via fast-xml-parser ──────────────── */

const xml = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "@_",
  trimValues: true,
  processEntities: true,
  parseTagValue: true,
});

function asArray<T>(v: T | T[] | undefined | null): T[] {
  if (v === undefined || v === null) return [];
  return Array.isArray(v) ? v : [v];
}

function textOf(node: unknown): string {
  if (node === null || node === undefined) return "";
  if (typeof node === "string") return node;
  if (typeof node === "number") return String(node);
  if (typeof node === "object") {
    const obj = node as Record<string, unknown>;
    if (typeof obj["#text"] === "string") return obj["#text"];
    if (typeof obj["@_href"] === "string") return obj["@_href"];
  }
  return "";
}

function parseDate(raw: string): Date | null {
  if (!raw) return null;
  const iso = new Date(raw);
  if (!isNaN(iso.getTime())) return iso;
  return null;
}

function extractEntries(doc: Record<string, unknown>): Record<string, unknown>[] {
  const rss = doc["rss"] as Record<string, unknown> | undefined;
  const rdf = doc["rdf:RDF"] as Record<string, unknown> | undefined;
  const atom = doc["feed"] as Record<string, unknown> | undefined;

  if (rss) {
    const channel = rss["channel"] as Record<string, unknown> | undefined;
    if (channel) return asArray(channel["item"]) as Record<string, unknown>[];
  }
  if (rdf) {
    return asArray(rdf["item"]) as Record<string, unknown>[];
  }
  if (atom) {
    return asArray(atom["entry"]) as Record<string, unknown>[];
  }
  return [];
}

function entryLink(entry: Record<string, unknown>): string {
  const link = entry["link"];
  // Atom: link can be object(s) with rel/href, or array of them
  if (Array.isArray(link)) {
    const alternate = link.find(
      (l) => typeof l === "object" && (l as Record<string, unknown>)["@_rel"] !== "self"
    );
    const first = alternate ?? link[0];
    return textOf(first);
  }
  const direct = textOf(link); // RSS <link>text</link> or Atom <link href>
  if (direct) return direct;
  return textOf(entry["guid"]);
}

function entryTitle(entry: Record<string, unknown>): string {
  const t = stripHtml(textOf(entry["title"]));
  return t || "Untitled";
}

function entryDate(entry: Record<string, unknown>): Date | null {
  for (const field of ["published", "updated", "pubDate", "dc:date", "date"]) {
    const d = parseDate(textOf(entry[field]));
    if (d) return d;
  }
  return null;
}

function entrySummary(entry: Record<string, unknown>): string {
  for (const field of ["summary", "description", "content", "content:encoded"]) {
    const raw = textOf(entry[field]);
    if (raw) {
      const s = stripHtml(raw);
      if (s) return s.length > 300 ? s.slice(0, 300) + "…" : s;
    }
  }
  return "";
}

function processEntry(entry: Record<string, unknown>, feedUrl: string): Post | null {
  const link = entryLink(entry);
  if (!link || !/^https?:\/\//.test(link)) return null;

  const host = hostOf(link);
  if (!host || BLOCKLIST.has(host)) return null;

  const published = entryDate(entry);
  if (!published) return null;

  const cutoff = Date.now() - relevantDays(envRef!) * 24 * 60 * 60 * 1000;
  if (published.getTime() < cutoff) return null;

  return {
    id: "", // filled by caller
    link,
    title: entryTitle(entry),
    published: published.toISOString(),
    host,
    summary: entrySummary(entry),
    feedUrl,
  };
}

/* ── Fetching ────────────────────────────────────────────────────────── */

let envRef: Env | null = null;

async function fetchFeed(url: string): Promise<Post[]> {
  try {
    const res = await fetch(url, {
      headers: { "User-Agent": envRef!.USER_AGENT, Accept: "application/rss+xml, application/atom+xml, application/xml, text/xml, */*" },
      redirect: "follow",
      cf: { cacheTtl: 300, cacheEverything: true },
    } as RequestInit<RequestInitCfProperties>);
    if (!res.ok) return [];
    const body = await res.text();
    const doc = xml.parse(body) as Record<string, unknown>;
    const posts: Post[] = [];
    for (const entry of extractEntries(doc)) {
      try {
        const post = processEntry(entry, url);
        if (post) posts.push(post);
      } catch {
        // skip malformed entry
      }
    }
    return posts;
  } catch {
    return [];
  }
}

async function mapLimit<T, R>(items: T[], limit: number, fn: (item: T) => Promise<R>): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let next = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) {
      const i = next++;
      results[i] = await fn(items[i]);
    }
  });
  await Promise.all(workers);
  return results;
}

async function fetchAllPosts(feeds: string[]): Promise<Post[]> {
  const results = await mapLimit(feeds, maxConcurrency(envRef!), fetchFeed);

  const seen = new Set<string>();
  const posts: Post[] = [];
  for (const feedPosts of results) {
    for (const post of feedPosts) {
      if (!seen.has(post.link)) {
        post.id = await md5Hex(post.link);
        posts.push(post);
        seen.add(post.link);
      }
    }
  }

  posts.sort((a, b) => b.published.localeCompare(a.published));
  return posts;
}

/* ── HTML rendering ──────────────────────────────────────────────────── */

function renderIndex(posts: Post[], totalFeeds: number): string {
  const now = new Date().toLocaleString("en-KE", {
    timeZone: "Africa/Nairobi",
    month: "long",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
    hour12: true,
  });

  const skeleton = Array.from({ length: 10 }, (_, i) => `    <li>
      <div class="skeleton-line ${i % 2 === 0 ? "skeleton-title" : "skeleton-title-short"}"></div>
      <div class="skeleton-line skeleton-meta"></div>
    </li>`).join("\n");

  const items = posts
    .map((post) => `    <li class="post-item">
      <a class="post-title" href="${escapeHtml(post.link)}" target="_blank" rel="noopener noreferrer">${escapeHtml(post.title)}</a>
    </li>`)
    .join("\n");

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta http-equiv="X-UA-Compatible" content="IE=edge,chrome=1" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta name="description" content="A personal RSS feed aggregator" />
  <title>RECTIFIER</title>
  <link rel="stylesheet" type="text/css" href="/styles.css" media="screen" />
</head>
<body>

  <header class="site-header">
    <div class="site-meta">
      Last updated: ${now} EAT
    </div>
  </header>

  <ul class="skeleton-list" id="skeleton" style="display:none">
${skeleton}
  </ul>

  <ol class="post-list" id="post-list" style="display:flex">
${items}
  </ol>

  <footer class="site-footer">
    <p>
      <a href="https://github.com/Dave-019/my-rss-feeds">source</a>
    </p>
  </footer>

  <script>
    window.addEventListener('DOMContentLoaded', function () {
      document.getElementById('skeleton').style.display = 'none';
      document.getElementById('post-list').style.display = 'flex';
      document.querySelectorAll('img').forEach(function (img) {
        img.loading = 'lazy';
        img.addEventListener('error', function () { img.style.display = 'none'; });
      });
    });
  </script>

</body>
</html>`;
}

/* ── Build & cache ───────────────────────────────────────────────────── */

async function buildAndCache(env: Env): Promise<{ posts: number; feeds: number }> {
  envRef = env;
  const posts = await fetchAllPosts(FEEDS);
  const html = renderIndex(posts, FEEDS.length);
  // Cache rendered page for 8+ hours (rebuilt by cron)
  await env.FEED_CACHE.put(PAGE_KEY, html, { expirationTtl: 60 * 60 * 12 });
  return { posts: posts.length, feeds: FEEDS.length };
}

/* ── Worker entry ────────────────────────────────────────────────────── */

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    envRef = env;
    const url = new URL(request.url);

    // Manual refresh endpoints
    if (url.pathname === "/__refresh" && request.method === "POST") {
      const stats = await buildAndCache(env);
      return Response.json({ ok: true, ...stats });
    }

    // Serve the cached homepage
    if (url.pathname === "/" || url.pathname === "/index.html") {
      const cached = await env.FEED_CACHE.get(PAGE_KEY);
      if (cached) {
        return new Response(cached, {
          headers: { "Content-Type": "text/html;charset=utf-8", "Cache-Control": "public, max-age=300" },
        });
      }
      // First run: build synchronously
      const stats = await buildAndCache(env);
      const html = await env.FEED_CACHE.get(PAGE_KEY);
      return new Response(html ?? `building... (${stats.posts} posts)`, {
        headers: { "Content-Type": "text/html;charset=utf-8" },
      });
    }

    // Everything else (styles.css etc.) from static assets
    return env.ASSETS.fetch(request);
  },

  async scheduled(_event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    ctx.waitUntil(buildAndCache(env));
  },
};
