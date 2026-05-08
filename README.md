# Crawler

Foundation Crawler — local-first Python crawler inspired by Firecrawl.

## Features

- **No Docker required**
- **Works in a Python virtual environment**
- **Uses in-memory state by default**
- **Uses Redis automatically if `REDIS_URL` is set**
- Firecrawl-style crawl/job/dedup structure
- Optional markdown conversion via `markdownify`

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/odrd-systems/Crawler.git
cd Crawler
```

### 2. Create a virtual environment

#### macOS / Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
```

#### Windows PowerShell

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

#### Windows CMD

```cmd
python -m venv .venv
.venv\Scripts\activate.bat
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

## Run locally

```bash
python firecrawl_crawler.py
```

If `REDIS_URL` is **not** set, the crawler runs in **local in-memory mode**.

If `REDIS_URL` **is** set, it will use Redis automatically.

## Optional `.env`

Create a `.env` if you want Redis mode:

```env
REDIS_URL=redis://localhost:6379
```

If you do not set this, no Redis is needed.

## Current example

The built-in example crawls:

```python
origin_url="https://example.com"
```

You can change that inside `firecrawl_crawler.py` to something real, for example:

```python
origin_url="https://docs.python.org/3/"
```

## Example usage in your own script

```python
import asyncio
from firecrawl_crawler import Crawler, CrawlerOptions, ScrapeOptions

async def main():
    crawler = Crawler(max_concurrency=3)

    crawl_id = await crawler.start_crawl(
        origin_url="https://docs.python.org/3/",
        crawler_options=CrawlerOptions(
            max_depth=2,
            limit=20,
            excludes=[r"\.pdf$", r"/logout"],
        ),
        scrape_options=ScrapeOptions(
            formats=["markdown", "html"],
            timeout=30000,
            headers={"User-Agent": "FoundationCrawler/0.1"},
        ),
        team_id="local-dev",
    )

    async def on_doc(url, doc):
        print(url, doc.metadata)

    docs = await crawler.run_crawl(crawl_id, on_document=on_doc)
    print(f"Scraped {len(docs)} pages")

asyncio.run(main())
```

## Architecture

- `Crawler` → main crawl orchestrator
- `scrape_url()` → single-page scraping pipeline
- `InMemoryCrawlStore` → default local state backend
- `RedisCrawlStore` → optional Redis backend
- `UrlFilter` → include/exclude/depth/subdomain checks
- `generate_url_permutations()` → dedupes `www`, `http/https`, `index.html`, etc.

## Current limitations

- `fetch` is the only real engine currently implemented
- `playwright` and `fire-engine` are placeholders/stubs for future work
- robots.txt is not yet wired in
- screenshots / actions / advanced Firecrawl engines are not yet implemented

## Next upgrades

Recommended next steps:
1. Add Playwright support
2. Add robots.txt support
3. Add FastAPI endpoints (`/crawl`, `/scrape`, `/map`)
4. Add file output / persistence layer
5. Add page content extraction utilities
