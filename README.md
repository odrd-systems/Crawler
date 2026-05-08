# Crawler

Foundation Crawler — Python port of [firecrawl/firecrawl](https://github.com/firecrawl/firecrawl) core crawl engine.

## Overview

This is a Python implementation of Firecrawl's core crawling and scraping pipeline, ported from TypeScript. It provides:

- **`CrawlMemory`** — Built-in in-memory crawl state (no extra software needed, great for local dev)
- **`CrawlRedis`** — Redis-backed crawl state management (optional, for production / persistent state)
- **`Crawler`** — Async multi-page web crawler with bounded concurrency
- **`scrape_url()`** — Single-URL scraper with waterfall engine fallback
- **`UrlFilter`** — Include/exclude regex filtering, depth limiting, subdomain control
- **`build_feature_flags()`** — Feature detection (PDF, screenshots, stealth proxy, etc.)
- **`generate_url_permutations()`** — Smart URL deduplication (www/http/index.html variants)

## Requirements

- Python 3.11+
- No external services required for local development (Redis is optional)

---

## Local Setup

### macOS / Linux

```bash
# 1. Clone and enter the repo
git clone https://github.com/odrd-systems/Crawler.git
cd Crawler

# 2. Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. (Optional) Copy the example config
cp .env.example .env

# 5. Run the example crawl
python firecrawl_crawler.py
```

### Windows (Command Prompt / PowerShell)

```bat
:: 1. Clone and enter the repo
git clone https://github.com/odrd-systems/Crawler.git
cd Crawler

:: 2. Create and activate a virtual environment
python -m venv .venv
.venv\Scripts\activate

:: 3. Install dependencies
pip install -r requirements.txt

:: 4. (Optional) Copy the example config
copy .env.example .env

:: 5. Run the example crawl
python firecrawl_crawler.py
```

---

## Quick Start

```python
import asyncio
from firecrawl_crawler import Crawler, CrawlerOptions, ScrapeOptions

async def main():
    # No Redis needed — uses in-memory store by default.
    crawler = Crawler(max_concurrency=3)

    crawl_id = await crawler.start_crawl(
        origin_url="https://example.com",
        crawler_options=CrawlerOptions(max_depth=2, limit=10),
        scrape_options=ScrapeOptions(formats=["markdown"]),
        team_id="my-team",
    )

    docs = await crawler.run_crawl(crawl_id)
    print(f"Scraped {len(docs)} pages")

asyncio.run(main())
```

---

## Local Mode vs Redis Mode

| | Local mode (default) | Redis mode |
|---|---|---|
| **Setup** | Just `pip install -r requirements.txt` | Also needs a running Redis server |
| **State persistence** | In-memory, lost on exit | Persisted in Redis with 24 h TTL |
| **Best for** | Development, testing, one-off crawls | Production, long-running crawls |

### Switching to Redis

Set the `REDIS_URL` environment variable before running:

```bash
# macOS / Linux
export REDIS_URL=redis://localhost:6379
python firecrawl_crawler.py

# Windows CMD
set REDIS_URL=redis://localhost:6379
python firecrawl_crawler.py
```

Install the Redis client libraries when using Redis mode:

```bash
pip install redis aioredis
```

You can also set `REDIS_URL` in your `.env` file (see `.env.example`).

---

## Architecture

```
Crawler.start_crawl()         → saves StoredCrawl to store (memory or Redis)
Crawler.run_crawl()           → async queue + semaphore worker loop
  └─ scrape_url()             → waterfall engine loop
       └─ scrape_with_engine() → HTTP fetch (stub: add Playwright / Fire Engine)
  └─ extract_links()          → parse <a href> from HTML
  └─ store.lock_url()         → dedup check (memory set or Redis SET)
```

## Redis Key Schema

> Only relevant when running in Redis mode (`REDIS_URL` is set).

| Key | Type | Description |
|-----|------|-------------|
| `crawl:<id>` | STRING | StoredCrawl JSON |
| `crawl:<id>:jobs` | SET | All job IDs |
| `crawl:<id>:jobs_done` | SET | Completed job IDs |
| `crawl:<id>:jobs_donez_ordered` | ZSET | Done jobs ordered by time |
| `crawl:<id>:visited` | SET | Deduped visited URLs |
| `crawl:<id>:visited_unique` | SET | Canonical visited URLs |
| `crawl:<id>:finish` | STRING | Crawl completion flag |
| `active_crawls` | SET | Currently active crawl IDs |

## Extending

- **Add real engines**: Replace `scrape_with_engine()` with Playwright, httpx, or Fire Engine calls
- **Add HTML→Markdown**: Use `markdownify` or `html2text` in `scrape_url()` after scraping
- **Add robots.txt**: Use Python's `urllib.robotparser` before calling `scrape_url()`
- **Add API server**: Wrap `Crawler` with FastAPI endpoints (`/crawl`, `/scrape`, `/map`)

## Origin

Ported from [`firecrawl/firecrawl`](https://github.com/firecrawl/firecrawl) — Apache 2.0 licensed.
Key source files:
- `apps/api/src/scraper/scrapeURL/index.ts`
- `apps/api/src/lib/crawl-redis.ts`
- `apps/api/src/scraper/WebScraper/crawler.ts`
