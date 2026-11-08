# RECTIFIER — Cloudflare Worker

An RSS feed aggregator running entirely on Cloudflare Workers (TypeScript). No GitHub Actions, no Pages build hook — a cron trigger inside the Worker rebuilds the page every 8 hours.

## Architecture

| Before (GitHub) | After (Worker) |
|---|---|
| GitHub Actions cron → curl Pages build hook | Worker `scheduled` cron (`0 */8 * * *`) |
| Python + aiohttp + feedparser + jinja2 | TypeScript + `fetch` + `fast-xml-parser` |
| Static files in `docs/` | Rendered page in **Workers KV**, static assets served by the Worker |
| Optional D1 REST writes | Removed (page is rebuilt from feeds each run) |

## Files

- `src/index.ts` — the Worker: fetches feeds, parses RSS/Atom/RDF, filters to last 24 h, renders HTML, caches in KV. Serves `/` from KV, `styles.css` etc. from `public/` assets.
- `src/feeds.ts` — generated from `feeds.txt` (run `node scripts/gen-feeds.mjs` after editing `feeds.txt`).
- `public/` — static assets (styles.css).
- `wrangler.jsonc` — Worker config, cron, KV binding, vars.

## Setup

```bash
npm install

# 1. Create the KV namespace and put its id in wrangler.jsonc
npx wrangler kv namespace create FEED_CACHE

# 2. Local dev (test the cron with a request):
npm run dev
curl "http://localhost:8787/__scheduled"   # runs the scheduled handler
curl "http://localhost:8787/"               # view page

# 3. Deploy
npx wrangler deploy
```

## Custom domain

In the Cloudflare dashboard → Workers → `rectifier` → Settings → Domains & Routes → add `feed.roma.co.ke` (the zone must be on this Cloudflare account). The `CNAME` file is no longer needed.

## Endpoints

- `GET /` — aggregated page from KV cache (builds on first hit)
- `GET /styles.css` — static asset
- `POST /__refresh` — manual rebuild (optionally guard with a secret check)

## Updating feeds

Edit `feeds.txt`, then:

```bash
node scripts/gen-feeds.mjs
npx wrangler deploy
```
