import logging
from pathlib import Path
import uuid
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.openapi.docs import get_swagger_ui_html, get_swagger_ui_oauth2_redirect_html
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel, Field
import swagger_ui_bundle

from crawl_web import crawl_page
from firecrawl_crawler import ScrapeOptions
from search_web import search_web


logger = logging.getLogger(__name__)
SWAGGER_UI_DIR = Path(swagger_ui_bundle.swagger_ui_path).resolve()

app = FastAPI(
    title="Crawler API",
    description="Local-first FastAPI wrapper for structured web search and page crawling.",
    version="0.1.0",
    docs_url=None,
    redoc_url=None,
)


class ScrapeOptionsModel(BaseModel):
    formats: list[str] = Field(default_factory=lambda: ["markdown", "html"])
    actions: list[dict] = Field(default_factory=list)
    wait_for: int = 0
    timeout: Optional[int] = 30000
    proxy: Optional[str] = None
    location: Optional[str] = None
    mobile: bool = False
    skip_tls_verification: bool = True
    block_ads: bool = True
    max_age: int = 0
    headers: dict[str, str] = Field(default_factory=dict)

    def to_scrape_options(self) -> ScrapeOptions:
        return ScrapeOptions(**self.model_dump())


class SearchRequest(BaseModel):
    query: str
    max_results: int = 10
    output_dir: str = "output"


class CrawlRequest(BaseModel):
    url: str
    team_id: str = "local-dev"
    crawl_id: Optional[str] = None
    output_dir: str = "output"
    scrape_options: ScrapeOptionsModel = Field(default_factory=ScrapeOptionsModel)


class SearchAndCrawlRequest(BaseModel):
    query: str
    max_results: int = 5
    team_id: str = "local-dev"
    crawl_id: Optional[str] = None
    output_dir: str = "output"
    scrape_options: ScrapeOptionsModel = Field(default_factory=ScrapeOptionsModel)


def _swagger_asset_path(filename: str) -> Path:
    path = (SWAGGER_UI_DIR / filename).resolve()
    path.relative_to(SWAGGER_UI_DIR)
    return path


@app.get("/_swagger/swagger-ui-bundle.js", include_in_schema=False)
async def swagger_ui_bundle_js() -> FileResponse:
    return FileResponse(_swagger_asset_path("swagger-ui-bundle.js"), media_type="text/javascript")


@app.get("/_swagger/swagger-ui.css", include_in_schema=False)
async def swagger_ui_css() -> FileResponse:
    return FileResponse(_swagger_asset_path("swagger-ui.css"), media_type="text/css")


@app.get("/_swagger/favicon-32x32.png", include_in_schema=False)
async def swagger_ui_favicon() -> FileResponse:
    return FileResponse(_swagger_asset_path("favicon-32x32.png"), media_type="image/png")


@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui() -> HTMLResponse:
    return get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title=f"{app.title} - Swagger UI",
        oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
        swagger_js_url="/_swagger/swagger-ui-bundle.js",
        swagger_css_url="/_swagger/swagger-ui.css",
        swagger_favicon_url="/_swagger/favicon-32x32.png",
    )


@app.get(app.swagger_ui_oauth2_redirect_url, include_in_schema=False)
async def swagger_ui_redirect() -> HTMLResponse:
    return get_swagger_ui_oauth2_redirect_html()


@app.post("/search")
async def search_endpoint(request: SearchRequest) -> dict:
    try:
        return await search_web(
            query=request.query,
            max_results=request.max_results,
            output_dir=request.output_dir,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception:
        logger.exception("Search request failed")
        raise HTTPException(status_code=500, detail="Search request failed")


@app.post("/crawl")
async def crawl_endpoint(request: CrawlRequest) -> dict:
    try:
        return await crawl_page(
            url=request.url,
            scrape_options=request.scrape_options.to_scrape_options(),
            team_id=request.team_id,
            crawl_id=request.crawl_id,
            output_dir=request.output_dir,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception:
        logger.exception("Crawl request failed")
        raise HTTPException(status_code=500, detail="Crawl request failed")


@app.post("/search-and-crawl")
async def search_and_crawl_endpoint(request: SearchAndCrawlRequest) -> dict:
    try:
        search_payload = await search_web(
            query=request.query,
            max_results=request.max_results,
            output_dir=request.output_dir,
        )

        pages = []
        base_crawl_id = request.crawl_id or str(uuid.uuid4())
        scrape_options = request.scrape_options.to_scrape_options()
        for item in search_payload.get("results", []):
            target_url = item.get("url")
            if not target_url:
                continue
            pages.append(
                await crawl_page(
                    url=target_url,
                    scrape_options=scrape_options,
                    team_id=request.team_id,
                    crawl_id=base_crawl_id,
                    output_dir=request.output_dir,
                )
            )

        return {
            "search": search_payload,
            "pages": pages,
            "metadata": {
                "query": request.query,
                "result_count": len(search_payload.get("results", [])),
                "page_count": len(pages),
                "crawl_id": base_crawl_id,
            },
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception:
        logger.exception("Search-and-crawl request failed")
        raise HTTPException(status_code=500, detail="Search-and-crawl request failed")
