# Crawler

Foundation Crawler — Python port of [firecrawl/firecrawl](https://github.com/firecrawl/firecrawl) core crawl engine.

## Overview

This is a Python implementation of Firecrawl's core crawling and scraping pipeline, ported from TypeScript. It provides:

- **`CrawlRedis`** — Redis-backed crawl state management (job tracking, URL deduplication, TTL)
- **`Crawler`** — Async multi-page web crawler with bounded concurrency
- **`scrape_url()`** — Single-URL scraper with waterfall engine fallback
- **`UrlFilter`** — Include/exclude regex filtering, depth limiting, subdomain control
- **`build_feature_flags()`** — Feature detection (PDF, screenshots, stealth proxy, etc.)
- **`generate_url_permutations()`** — Smart URL deduplication (www/http/index.html variants)

## Requirements

- Python 3.11+
- Redis (local or remote)

## Installation

```bash
pip install -r requirements.txt
```

## Quick Start

```python
import asyncio
from firecrawl_crawler import Crawler, CrawlerOptions, ScrapeOptions

async def main():
    crawler = Crawler(redis_url="redis://localhost:6379", max_concurrency=3)

    crawl_id = await crawler.start_crawl(
        origin_url="https://example.com",
        crawler_options=CrawlerOptions(max_depth=2, limit=50),
        scrape_options=ScrapeOptions(formats=["markdown"]),
        team_id="my-team",
    )

    docs = await crawler.run_crawl(crawl_id)
    print(f"Scraped {{len(docs)}} pages")

asyncio.run(main())
```

## Architecture

```
Crawler.start_crawl()         → saves StoredCrawl to Redis
Crawler.run_crawl()           → async queue + semaphore worker loop
  └─ scrape_url()             → waterfall engine loop
       └─ scrape_with_engine() → HTTP fetch (stub: add Playwright / Fire Engine)
  └─ extract_links()          → parse <a href> from HTML
  └─ CrawlRedis.lock_url()    → Redis SET dedup check
```

## Redis Key Schema

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
