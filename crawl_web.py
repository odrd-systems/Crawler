import hashlib
import os
import uuid
from typing import Optional

from firecrawl_crawler import ScrapeOptions, safe_filename, safe_output_dir, save_json, scrape_url


def page_artifact_path(url: str, crawl_id: str, output_dir: str = "output") -> str:
    page_output_dir = os.path.join(safe_output_dir(output_dir, default="output"), "pages")
    base = safe_filename(url, default="page")
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:10]
    safe_crawl_id = safe_filename(crawl_id, default="crawl")
    return os.path.join(page_output_dir, safe_crawl_id, f"{base}-{digest}.json")


def save_page_artifact(page: dict, crawl_id: str, output_dir: str = "output") -> str:
    path = page_artifact_path(url=page.get("url", ""), crawl_id=crawl_id, output_dir=output_dir)
    save_json(path, page)
    return path


async def crawl_page(
    url: str,
    scrape_options: Optional[ScrapeOptions] = None,
    team_id: str = "local-dev",
    crawl_id: Optional[str] = None,
    output_dir: str = "output",
) -> dict:
    effective_crawl_id = crawl_id or str(uuid.uuid4())
    scrape_opts = scrape_options or ScrapeOptions(formats=["markdown", "html"])
    result = await scrape_url(
        scrape_id=str(uuid.uuid4()),
        url=url,
        options=scrape_opts,
        team_id=team_id,
        crawl_id=effective_crawl_id,
    )

    if not result.success or not result.document:
        return {
            "success": False,
            "crawl_id": effective_crawl_id,
            "url": url,
            "error": "crawl_failed",
            "error_type": type(result.error).__name__ if result.error else "UnknownError",
        }

    page = dict(result.document.structured_data)

    artifact_path = save_page_artifact(page=page, crawl_id=effective_crawl_id, output_dir=output_dir)
    response = {
        "success": True,
        "crawl_id": effective_crawl_id,
        "artifact_path": artifact_path,
        **page,
    }
    if result.document.warning:
        response["warning"] = result.document.warning
    return response
