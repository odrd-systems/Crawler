from datetime import datetime, timezone
import os
from typing import Optional

import httpx

from firecrawl_crawler import (
    DEFAULT_USER_AGENT,
    parse_duckduckgo_results,
    safe_filename,
    safe_output_dir,
    save_json,
)


def search_artifact_path(query: str, output_dir: str = "output", stamp: Optional[str] = None) -> str:
    search_output_dir = os.path.join(safe_output_dir(output_dir, default="output"), "search")
    timestamp = stamp or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return os.path.join(search_output_dir, f"{safe_filename(query, 'search')}-{timestamp}.json")


def save_search_results(payload: dict, query: str, output_dir: str = "output") -> str:
    path = search_artifact_path(query=query, output_dir=output_dir)
    save_json(path, payload)
    return path


async def search_web(query: str, max_results: int = 10, output_dir: str = "output") -> dict:
    search_url = "https://duckduckgo.com/html/"
    headers = {"User-Agent": f"{DEFAULT_USER_AGENT} (+web-search)"}
    results: list[dict] = []
    error: Optional[str] = None

    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            response = await client.get(search_url, params={"q": query}, headers=headers)
            response.raise_for_status()
        results = parse_duckduckgo_results(response.text, max_results=max_results)
    except Exception as exc:
        error = f"search_failed:{type(exc).__name__}"

    payload = {
        "query": query,
        "engine": "duckduckgo",
        "results": results,
        "metadata": {
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "result_count": len(results),
            "error": error,
        },
    }
    payload["artifact_path"] = save_search_results(payload=payload, query=query, output_dir=output_dir)
    return payload
