# Crawler

Foundation Crawler — local-first Python crawler inspired by Firecrawl.

## Features

- **No Docker required**
- **Works in a Python virtual environment**
- **Uses in-memory state by default**
- **Uses Redis automatically if `REDIS_URL` is set**
- Firecrawl-style crawl/job/dedup structure
- Structured per-page JSON artifacts saved during crawling
- AI-ready page schema with separated text/markdown/html/links/media/documents/metadata
- DuckDuckGo search → crawl workflow with structured search result JSON output
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

Structured artifacts are saved automatically under:

- `output/pages/<crawl_id>/*.json` (one JSON file per crawled page)
- `output/search/*.json` (DuckDuckGo search result payloads)

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
        print(url, doc.structured_data["metadata"].get("title"))

    docs = await crawler.run_crawl(crawl_id, on_document=on_doc)
    print(f"Scraped {len(docs)} pages")

asyncio.run(main())
```

## AI-ready per-page JSON schema

Each crawled page is represented as a structured artifact:

```json
{
  "url": "https://example.com/page",
  "text": "cleaned plain text",
  "markdown": "# markdown content",
  "html": "<html>...</html>",
  "internal_links": ["https://example.com/about"],
  "external_links": ["https://external.site/doc"],
  "images": [{"url": "https://example.com/img.jpg", "type": "image"}],
  "audio": [{"url": "https://example.com/audio.mp3", "type": "audio"}],
  "video": [{"url": "https://example.com/video.mp4", "type": "video"}],
  "documents": [{"url": "https://example.com/report.pdf", "type": "document"}],
  "metadata": {
    "url": "https://example.com/page",
    "status_code": 200,
    "engine": "fetch",
    "content_type": "text/html; charset=utf-8"
  }
}
```

## Search → crawl workflow (DuckDuckGo)

```python
import asyncio
from firecrawl_crawler import Crawler

async def main():
    crawler = Crawler(max_concurrency=3)
    result = await crawler.search_and_crawl(
        query="python asyncio queues",
        max_results=3,
    )
    print("Search results:", len(result["search"]["results"]))
    print("Crawled pages:", len(result["documents"]))

asyncio.run(main())
```

`search_and_crawl()` saves search results JSON in `output/search/` and then crawls each search result URL.

## Architecture

- `Crawler` → main crawl orchestrator
- `scrape_url()` → single-page scraping pipeline
- `build_structured_page()` → AI-ready page extraction (text, markdown, html, links, media, docs, metadata)
- `search_web()` / `search_and_crawl()` → DuckDuckGo search plus crawl orchestration
- `InMemoryCrawlStore` → default local state backend
- `RedisCrawlStore` → optional Redis backend
- `UrlFilter` → include/exclude/depth/subdomain checks
- `generate_url_permutations()` → dedupes `www`, `http/https`, `index.html`, etc.

## Current limitations

- `fetch` is the only real engine currently implemented
- `playwright` and `fire-engine` remain placeholders/stubs for future browser rendering work
- robots.txt is not yet wired in
- screenshots / actions / advanced Firecrawl engines are not yet implemented

## Next upgrades

Recommended next steps:
1. Add Playwright support
2. Add robots.txt support
3. Add FastAPI endpoints (`/crawl`, `/scrape`, `/map`)
4. Add file output / persistence layer
5. Add page content extraction utilities
