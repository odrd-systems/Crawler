"""
Foundation crawler inspired by Firecrawl.

Local-first:
- Works without Docker
- Uses in-memory state if REDIS_URL is not set
- Uses Redis automatically if REDIS_URL is provided

Current status:
- fetch engine works
- Playwright / Fire Engine are stubs for future extension
- markdown conversion is optional (enabled if markdownify is installed)
"""

import asyncio
import hashlib
import json
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from html import unescape
from enum import Enum
from typing import Optional, Protocol
from urllib.parse import parse_qs, urljoin, urlparse, urlunparse

import httpx

try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None

try:
    from markdownify import markdownify as md
except Exception:
    md = None


# ---------------------------------------------------------------------------
# Types / Dataclasses
# ---------------------------------------------------------------------------

class FeatureFlag(str, Enum):
    ACTIONS = "actions"
    SCREENSHOT = "screenshot"
    SCREENSHOT_FULLSCREEN = "screenshot@fullScreen"
    PDF = "pdf"
    DOCUMENT = "document"
    WAIT_FOR = "waitFor"
    STEALTH_PROXY = "stealthProxy"
    LOCATION = "location"
    MOBILE = "mobile"
    SKIP_TLS = "skipTlsVerification"
    BRANDING = "branding"
    AUDIO = "audio"
    DISABLE_ADBLOCK = "disableAdblock"


@dataclass
class ScrapeOptions:
    formats: list[str] = field(default_factory=lambda: ["markdown"])
    actions: list[dict] = field(default_factory=list)
    wait_for: int = 0
    timeout: Optional[int] = 30000
    proxy: Optional[str] = None
    location: Optional[str] = None
    mobile: bool = False
    skip_tls_verification: bool = True
    block_ads: bool = True
    max_age: int = 0
    headers: dict[str, str] = field(default_factory=dict)


@dataclass
class CrawlerOptions:
    max_crawled_links: int = 1000
    max_depth: int = 2
    limit: int = 100
    includes: list[str] = field(default_factory=list)
    excludes: list[str] = field(default_factory=list)
    allow_backward_crawling: bool = False
    allow_subdomains: bool = False
    ignore_query_parameters: bool = False
    ignore_robots_txt: bool = False
    deduplicate_similar_urls: bool = False
    max_discovery_depth: Optional[int] = None


@dataclass
class StoredCrawl:
    origin_url: str
    crawler_options: CrawlerOptions
    scrape_options: ScrapeOptions
    team_id: str
    robots: Optional[str] = None
    cancelled: bool = False
    created_at: float = field(default_factory=time.time)
    max_concurrency: Optional[int] = None
    zero_data_retention: bool = False


@dataclass
class Document:
    markdown: Optional[str] = None
    text: Optional[str] = None
    raw_html: Optional[str] = None
    screenshot: Optional[str] = None
    internal_links: list[str] = field(default_factory=list)
    external_links: list[str] = field(default_factory=list)
    images: list[dict] = field(default_factory=list)
    audio: list[dict] = field(default_factory=list)
    video: list[dict] = field(default_factory=list)
    documents: list[dict] = field(default_factory=list)
    structured_data: dict = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)
    warning: Optional[str] = None


@dataclass
class ScrapeResult:
    success: bool
    document: Optional[Document] = None
    error: Optional[Exception] = None


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class CrawlDenialError(Exception):
    pass


class NoEnginesLeftError(Exception):
    pass


class EngineError(Exception):
    pass


class EngineUnsuccessfulError(EngineError):
    def __init__(self, engine: str):
        self.engine = engine
        super().__init__(f"Engine {engine} returned empty content")


class AddFeatureError(Exception):
    def __init__(self, flags: list[FeatureFlag]):
        self.flags = flags
        super().__init__(f"Need additional features: {flags}")


class ScrapeTimeoutError(Exception):
    pass


# ---------------------------------------------------------------------------
# Store protocol
# ---------------------------------------------------------------------------

TTL = 24 * 60 * 60


class CrawlStore(Protocol):
    async def save_crawl(self, crawl_id: str, crawl: StoredCrawl): ...
    async def get_crawl(self, crawl_id: str) -> Optional[dict]: ...
    async def finish_crawl(self, crawl_id: str): ...
    async def set_crawl_error(self, crawl_id: str, error: str): ...
    async def mark_crawl_active(self, crawl_id: str): ...
    async def add_crawl_job(self, crawl_id: str, job_id: str): ...
    async def add_crawl_jobs(self, crawl_id: str, job_ids: list[str]): ...
    async def add_crawl_job_done(self, crawl_id: str, job_id: str, success: bool): ...
    async def get_done_jobs_ordered(self, crawl_id: str, start=0, end=-1) -> list[str]: ...
    async def is_crawl_finished(self, crawl_id: str) -> bool: ...
    async def finish_kickoff(self, crawl_id: str): ...
    async def lock_url(
        self,
        crawl_id: str,
        url: str,
        limit: Optional[int],
        ignore_query_params: bool = False,
        deduplicate_similar: bool = False,
    ) -> bool: ...
    async def record_robots_blocked(self, crawl_id: str, url: str): ...


# ---------------------------------------------------------------------------
# Shared URL helpers
# ---------------------------------------------------------------------------

def normalize_url(url: str, ignore_query_params: bool = False) -> str:
    parsed = urlparse(url)
    query = "" if ignore_query_params else parsed.query

    fragment = parsed.fragment
    if not fragment or len(fragment) <= 1 or not (
        fragment.startswith("/") or fragment.startswith("!/")):
        fragment = ""

    return urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        query,
        fragment,
    ))


def generate_url_permutations(url: str) -> list[str]:
    parsed = urlparse(url)
    hostname = parsed.hostname or ""

    if hostname.startswith("www."):
        hosts = [hostname, hostname[4:]]
    else:
        hosts = ["www." + hostname, hostname]

    schemes = ["http", "https"] if parsed.scheme in ("http", "https") else [parsed.scheme]

    path = parsed.path or "/"
    if path.endswith("/"):
        bare = path.rstrip("/") or "/"
        paths = [path + "index.html", path + "index.php", path, bare]
    elif path.endswith("/index.html"):
        base = path[:-len("index.html")]
        bare = path[:-len("/index.html")] or "/"
        paths = [path, base + "index.php", base, bare]
    elif path.endswith("/index.php"):
        base = path[:-len("index.php")]
        bare = path[:-len("/index.php")] or "/"
        paths = [base + "index.html", path, base, bare]
    else:
        paths = [path + "/index.html", path + "/index.php", path + "/", path]

    permutations = set()
    for scheme in schemes:
        for host in hosts:
            port = f":{parsed.port}" if parsed.port else ""
            netloc = host + port
            for p in paths:
                permutations.add(
                    urlunparse((scheme, netloc, p, "", parsed.query, ""))
                )

    return list(permutations)


# ---------------------------------------------------------------------------
# In-memory store
# ---------------------------------------------------------------------------

class InMemoryCrawlStore:
    def __init__(self):
        self.crawls: dict[str, dict] = {}
        self.active_crawls: set[str] = set()
        self.crawls_by_team: dict[str, set[str]] = {}
        self.jobs: dict[str, set[str]] = {}
        self.jobs_qualified: dict[str, set[str]] = {}
        self.jobs_done: dict[str, set[str]] = {}
        self.jobs_done_ordered: dict[str, list[tuple[float, str]]] = {}
        self.visited: dict[str, set[str]] = {}
        self.visited_unique: dict[str, set[str]] = {}
        self.robots_blocked: dict[str, set[str]] = {}
        self.kickoff_finished: set[str] = set()
        self.finished: set[str] = set()
        self.errors: dict[str, str] = {}

    async def save_crawl(self, crawl_id: str, crawl: StoredCrawl):
        self.crawls[crawl_id] = {
            "origin_url": crawl.origin_url,
            "crawler_options": crawl.crawler_options.__dict__,
            "scrape_options": crawl.scrape_options.__dict__,
            "team_id": crawl.team_id,
            "robots": crawl.robots,
            "cancelled": crawl.cancelled,
            "created_at": crawl.created_at,
            "zero_data_retention": crawl.zero_data_retention,
        }
        self.crawls_by_team.setdefault(crawl.team_id, set()).add(crawl_id)

    async def get_crawl(self, crawl_id: str) -> Optional[dict]:
        return self.crawls.get(crawl_id)

    async def finish_crawl(self, crawl_id: str):
        self.finished.add(crawl_id)
        self.active_crawls.discard(crawl_id)
        crawl = self.crawls.get(crawl_id)
        if crawl:
            team_id = crawl.get("team_id")
            if team_id in self.crawls_by_team:
                self.crawls_by_team[team_id].discard(crawl_id)

    async def set_crawl_error(self, crawl_id: str, error: str):
        self.errors[crawl_id] = error

    async def mark_crawl_active(self, crawl_id: str):
        self.active_crawls.add(crawl_id)

    async def add_crawl_job(self, crawl_id: str, job_id: str):
        self.jobs.setdefault(crawl_id, set()).add(job_id)
        self.jobs_qualified.setdefault(crawl_id, set()).add(job_id)

    async def add_crawl_jobs(self, crawl_id: str, job_ids: list[str]):
        self.jobs.setdefault(crawl_id, set()).update(job_ids)
        self.jobs_qualified.setdefault(crawl_id, set()).update(job_ids)

    async def add_crawl_job_done(self, crawl_id: str, job_id: str, success: bool):
        self.jobs_done.setdefault(crawl_id, set()).add(job_id)
        if success:
            self.jobs_done_ordered.setdefault(crawl_id, []).append((time.time(), job_id))

    async def get_done_jobs_ordered(self, crawl_id: str, start=0, end=-1) -> list[str]:
        ordered = sorted(self.jobs_done_ordered.get(crawl_id, []), key=lambda x: x[0])
        ids = [job_id for _, job_id in ordered]
        if end == -1:
            return ids[start:]
        return ids[start:end + 1]

    async def is_crawl_finished(self, crawl_id: str) -> bool:
        done = len(self.jobs_done.get(crawl_id, set()))
        total = len(self.jobs.get(crawl_id, set()))
        kickoff = crawl_id in self.kickoff_finished
        return done == total and kickoff

    async def finish_kickoff(self, crawl_id: str):
        self.kickoff_finished.add(crawl_id)

    async def lock_url(
        self,
        crawl_id: str,
        url: str,
        limit: Optional[int],
        ignore_query_params: bool = False,
        deduplicate_similar: bool = False,
    ) -> bool:
        normalized = normalize_url(url, ignore_query_params)

        if limit is not None:
            count = len(self.visited_unique.get(crawl_id, set()))
            if count >= limit:
                return False

        if deduplicate_similar:
            visit_key = generate_url_permutations(normalized)[0]
        else:
            visit_key = normalized

        crawl_visited = self.visited.setdefault(crawl_id, set())
        if visit_key in crawl_visited:
            return False

        crawl_visited.add(visit_key)
        self.visited_unique.setdefault(crawl_id, set()).add(normalized)
        return True

    async def record_robots_blocked(self, crawl_id: str, url: str):
        self.robots_blocked.setdefault(crawl_id, set()).add(url)


# ---------------------------------------------------------------------------
# Redis store
# ---------------------------------------------------------------------------

class RedisCrawlStore:
    def __init__(self, redis_url: str):
        if aioredis is None:
            raise RuntimeError("redis package is not installed")
        self.redis = aioredis.from_url(redis_url, decode_responses=True)

    async def save_crawl(self, crawl_id: str, crawl: StoredCrawl):
        data = {
            "origin_url": crawl.origin_url,
            "crawler_options": crawl.crawler_options.__dict__,
            "scrape_options": crawl.scrape_options.__dict__,
            "team_id": crawl.team_id,
            "robots": crawl.robots,
            "cancelled": crawl.cancelled,
            "created_at": crawl.created_at,
            "zero_data_retention": crawl.zero_data_retention,
        }
        await self.redis.set(f"crawl:{crawl_id}", json.dumps(data), ex=TTL)
        await self.redis.sadd(f"crawls_by_team_id:{crawl.team_id}", crawl_id)
        await self.redis.expire(f"crawls_by_team_id:{crawl.team_id}", TTL)

    async def get_crawl(self, crawl_id: str) -> Optional[dict]:
        raw = await self.redis.get(f"crawl:{crawl_id}")
        if raw is None:
            return None
        await self.redis.expire(f"crawl:{crawl_id}", TTL)
        return json.loads(raw)

    async def finish_crawl(self, crawl_id: str):
        await self.redis.set(f"crawl:{crawl_id}:finish", "yes", ex=TTL)
        await self.redis.srem("active_crawls", crawl_id)
        crawl = await self.get_crawl(crawl_id)
        if crawl and crawl.get("team_id"):
            await self.redis.srem(f"crawls_by_team_id:{crawl['team_id']}", crawl_id)
        await self.redis.delete(f"crawl:{crawl_id}:visited")
        await self.redis.delete(f"crawl:{crawl_id}:visited_unique")

    async def set_crawl_error(self, crawl_id: str, error: str):
        await self.redis.set(f"crawl:{crawl_id}:error", error, ex=TTL)

    async def mark_crawl_active(self, crawl_id: str):
        await self.redis.sadd("active_crawls", crawl_id)

    async def add_crawl_job(self, crawl_id: str, job_id: str):
        pipe = self.redis.pipeline()
        pipe.sadd(f"crawl:{crawl_id}:jobs", job_id)
        pipe.expire(f"crawl:{crawl_id}:jobs", TTL)
        pipe.sadd(f"crawl:{crawl_id}:jobs_qualified", job_id)
        pipe.expire(f"crawl:{crawl_id}:jobs_qualified", TTL)
        await pipe.execute()

    async def add_crawl_jobs(self, crawl_id: str, job_ids: list[str]):
        if not job_ids:
            return
        pipe = self.redis.pipeline()
        pipe.sadd(f"crawl:{crawl_id}:jobs", *job_ids)
        pipe.expire(f"crawl:{crawl_id}:jobs", TTL)
        pipe.sadd(f"crawl:{crawl_id}:jobs_qualified", *job_ids)
        pipe.expire(f"crawl:{crawl_id}:jobs_qualified", TTL)
        await pipe.execute()

    async def add_crawl_job_done(self, crawl_id: str, job_id: str, success: bool):
        pipe = self.redis.pipeline()
        pipe.sadd(f"crawl:{crawl_id}:jobs_done", job_id)
        pipe.expire(f"crawl:{crawl_id}:jobs_done", TTL)
        if success:
            pipe.zadd(f"crawl:{crawl_id}:jobs_donez_ordered", {job_id: time.time()})
        else:
            pipe.zrem(f"crawl:{crawl_id}:jobs_donez_ordered", job_id)
        pipe.expire(f"crawl:{crawl_id}:jobs_donez_ordered", TTL)
        await pipe.execute()

    async def get_done_jobs_ordered(self, crawl_id: str, start=0, end=-1) -> list[str]:
        await self.redis.expire(f"crawl:{crawl_id}:jobs_donez_ordered", TTL)
        return await self.redis.zrange(f"crawl:{crawl_id}:jobs_donez_ordered", start, end)

    async def is_crawl_finished(self, crawl_id: str) -> bool:
        done = await self.redis.scard(f"crawl:{crawl_id}:jobs_done")
        total = await self.redis.scard(f"crawl:{crawl_id}:jobs")
        kickoff = await self.redis.get(f"crawl:{crawl_id}:kickoff:finish")
        return done == total and kickoff is not None

    async def finish_kickoff(self, crawl_id: str):
        await self.redis.set(f"crawl:{crawl_id}:kickoff:finish", "yes", ex=TTL)

    async def lock_url(
        self,
        crawl_id: str,
        url: str,
        limit: Optional[int],
        ignore_query_params: bool = False,
        deduplicate_similar: bool = False,
    ) -> bool:
        normalized = normalize_url(url, ignore_query_params)

        if limit is not None:
            count = await self.redis.scard(f"crawl:{crawl_id}:visited_unique")
            if count >= limit:
                return False

        if deduplicate_similar:
            visit_key = generate_url_permutations(normalized)[0]
        else:
            visit_key = normalized

        pipe = self.redis.pipeline()
        pipe.sadd(f"crawl:{crawl_id}:visited", visit_key)
        pipe.expire(f"crawl:{crawl_id}:visited", TTL)
        results = await pipe.execute()

        added = results[0]
        if added:
            upipe = self.redis.pipeline()
            upipe.sadd(f"crawl:{crawl_id}:visited_unique", normalized)
            upipe.expire(f"crawl:{crawl_id}:visited_unique", TTL)
            await upipe.execute()
            return True

        return False

    async def record_robots_blocked(self, crawl_id: str, url: str):
        await self.redis.sadd(f"crawl:{crawl_id}:robots_blocked", url)
        await self.redis.expire(f"crawl:{crawl_id}:robots_blocked", TTL)


# ---------------------------------------------------------------------------
# URL filtering
# ---------------------------------------------------------------------------

class UrlFilter:
    def __init__(self, options: CrawlerOptions, origin_url: str):
        self.options = options
        self.origin = urlparse(origin_url)

    def is_allowed(self, url: str, current_depth: int = 0) -> bool:
        parsed = urlparse(url)

        if parsed.scheme not in ("http", "https"):
            return False

        if current_depth > self.options.max_depth:
            return False

        origin_host = self.origin.hostname or ""
        target_host = parsed.hostname or ""

        if not self.options.allow_subdomains:
            if target_host != origin_host and target_host != f"www.{origin_host}":
                return False

        if self.options.includes:
            if not any(re.search(pattern, parsed.path) for pattern in self.options.includes):
                return False

        if self.options.excludes:
            if any(re.search(pattern, parsed.path) for pattern in self.options.excludes):
                return False

        return True


# ---------------------------------------------------------------------------
# Feature flags
# ---------------------------------------------------------------------------

DOCUMENT_EXTS = {".docx", ".doc", ".odt", ".rtf", ".xlsx", ".xls"}
DOCUMENT_LINK_EXTS = DOCUMENT_EXTS.union({".pdf", ".ppt", ".pptx", ".csv", ".txt", ".zip"})
AUDIO_EXTS = {".mp3", ".wav", ".ogg", ".m4a", ".aac", ".flac"}
VIDEO_EXTS = {".mp4", ".webm", ".mov", ".mkv", ".m3u8"}
MAX_FILENAME_LENGTH = 180
DEFAULT_USER_AGENT = "FoundationCrawler/0.1"


def build_feature_flags(url: str, options: ScrapeOptions) -> set[FeatureFlag]:
    flags: set[FeatureFlag] = set()

    if options.actions:
        flags.add(FeatureFlag.ACTIONS)
    if "screenshot" in options.formats:
        flags.add(FeatureFlag.SCREENSHOT)
    if "screenshot@fullPage" in options.formats:
        flags.add(FeatureFlag.SCREENSHOT_FULLSCREEN)
    if "branding" in options.formats:
        flags.add(FeatureFlag.BRANDING)
    if "audio" in options.formats:
        flags.add(FeatureFlag.AUDIO)
    if options.wait_for != 0:
        flags.add(FeatureFlag.WAIT_FOR)
    if options.location:
        flags.add(FeatureFlag.LOCATION)
    if options.mobile:
        flags.add(FeatureFlag.MOBILE)
    if options.skip_tls_verification:
        flags.add(FeatureFlag.SKIP_TLS)
    if options.proxy in ("stealth", "enhanced"):
        flags.add(FeatureFlag.STEALTH_PROXY)

    parsed_path = urlparse(url).path.lower()
    is_document = any(parsed_path.endswith(ext) for ext in DOCUMENT_EXTS)
    if is_document:
        flags.add(FeatureFlag.DOCUMENT)
    elif parsed_path.endswith(".pdf"):
        flags.add(FeatureFlag.PDF)

    if not options.block_ads:
        flags.add(FeatureFlag.DISABLE_ADBLOCK)

    return flags


# ---------------------------------------------------------------------------
# Engines
# ---------------------------------------------------------------------------

ENGINES = ["fetch", "playwright", "fire-engine"]


async def scrape_with_fetch(url: str, options: ScrapeOptions, engine: str) -> dict:
    async with httpx.AsyncClient(
        verify=not options.skip_tls_verification,
        timeout=(options.timeout or 30000) / 1000,
        follow_redirects=True,
    ) as client:
        response = await client.get(url, headers=options.headers or {})
        return {
            "html": response.text,
            "status_code": response.status_code,
            "error": None,
            "url": str(response.url),
            "engine": engine,
            "content_type": response.headers.get("content-type"),
        }


async def scrape_with_browser_placeholder(url: str, options: ScrapeOptions, engine: str) -> dict:
    # Extension point for future Playwright / browser-rendered extraction.
    return await scrape_with_fetch(url, options, engine)


async def scrape_with_engine(url: str, engine: str, options: ScrapeOptions) -> dict:
    """
    Current implementation:
    - fetch works
    - playwright and fire-engine fall back to fetch for now
    """
    try:
        if engine == "fetch":
            return await scrape_with_fetch(url, options, engine)
        if engine in {"playwright", "fire-engine"}:
            return await scrape_with_browser_placeholder(url, options, engine)
        return await scrape_with_fetch(url, options, engine)
    except Exception as e:
        return {
            "html": "",
            "status_code": 0,
            "error": str(e),
            "url": url,
            "engine": engine,
            "content_type": None,
        }


# ---------------------------------------------------------------------------
# HTML / link extraction
# ---------------------------------------------------------------------------

def html_to_markdown(html: str) -> Optional[str]:
    if md is None:
        return None
    try:
        return md(html)
    except Exception:
        return None


def strip_html_tags(value: str) -> str:
    value = re.sub(r"<script\b[^>]*>.*?</script\s*>", " ", value, flags=re.IGNORECASE | re.DOTALL)
    value = re.sub(r"<style\b[^>]*>.*?</style\s*>", " ", value, flags=re.IGNORECASE | re.DOTALL)
    value = re.sub(r"<[^>]+>", " ", value)
    value = unescape(value)
    return re.sub(r"\s+", " ", value).strip()


def safe_filename(value: str, default: str = "item") -> str:
    cleaned = re.sub(r"^https?://", "", value).strip()
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "_", cleaned).strip("._")
    return cleaned[:MAX_FILENAME_LENGTH] or default


def _extract_attr_value(attrs: str, attr: str) -> Optional[str]:
    pattern = re.compile(rf'{attr}\s*=\s*["\']([^"\']+)["\']', re.IGNORECASE)
    match = pattern.search(attrs)
    if match:
        return match.group(1).strip()
    return None


def extract_title(html: str) -> Optional[str]:
    match = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    title = strip_html_tags(match.group(1))
    return title or None


def _is_same_domain(origin_host: str, target_host: str) -> bool:
    origin = (origin_host or "").lower().removeprefix("www.")
    target = (target_host or "").lower().removeprefix("www.")
    return bool(origin and target and (target == origin or target.endswith("." + origin)))


def extract_link_groups(html: str, base_url: str, page_url: str) -> tuple[list[str], list[str], list[str]]:
    pattern = re.compile(r'<a\s([^>]*?)href=["\']([^"\']+)["\']([^>]*)>', re.IGNORECASE)
    links: list[str] = []
    internal: set[str] = set()
    external: set[str] = set()
    page_host = urlparse(page_url).hostname or ""

    for match in pattern.finditer(html):
        href = match.group(2).strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:", "data:")):
            continue
        absolute = urljoin(base_url, href)
        parsed = urlparse(absolute)
        if parsed.scheme not in ("http", "https"):
            continue
        links.append(absolute)
        if _is_same_domain(page_host, parsed.hostname or ""):
            internal.add(absolute)
        else:
            external.add(absolute)

    deduped = list(dict.fromkeys(links))
    return deduped, sorted(internal), sorted(external)


def extract_media_items(
    html: str,
    base_url: str,
    tag: str,
    attr: str = "src",
    item_type: str = "media",
    include_alt_title: bool = False,
) -> list[dict]:
    pattern = re.compile(rf"<{tag}\b([^>]*)>", re.IGNORECASE)
    seen: set[str] = set()
    items: list[dict] = []

    for match in pattern.finditer(html):
        attrs = match.group(1)
        raw = _extract_attr_value(attrs, attr)
        if not raw:
            continue
        absolute = urljoin(base_url, raw)
        parsed = urlparse(absolute)
        if parsed.scheme not in ("http", "https"):
            continue
        if absolute in seen:
            continue
        seen.add(absolute)
        item = {"url": absolute, "type": item_type}
        if include_alt_title:
            alt = _extract_attr_value(attrs, "alt")
            title = _extract_attr_value(attrs, "title")
            if alt:
                item["alt"] = alt
            if title:
                item["title"] = title
        items.append(item)

    return items


def extract_document_links(all_links: list[str]) -> list[dict]:
    items = []
    for link in all_links:
        path = urlparse(link).path.lower()
        if any(path.endswith(ext) for ext in DOCUMENT_LINK_EXTS):
            items.append({"url": link, "type": "document"})
    return items


def extract_audio_links(all_links: list[str]) -> list[dict]:
    items = []
    for link in all_links:
        path = urlparse(link).path.lower()
        if any(path.endswith(ext) for ext in AUDIO_EXTS):
            items.append({"url": link, "type": "audio"})
    return items


def extract_video_links(all_links: list[str]) -> list[dict]:
    items = []
    for link in all_links:
        hostname = (urlparse(link).hostname or "").lower()
        path = urlparse(link).path.lower()
        is_video_host = hostname in {
            "youtube.com",
            "www.youtube.com",
            "m.youtube.com",
            "youtu.be",
            "vimeo.com",
            "www.vimeo.com",
            "player.vimeo.com",
        }
        if any(path.endswith(ext) for ext in VIDEO_EXTS) or is_video_host:
            items.append({"url": link, "type": "video"})
    return items


def build_structured_page(url: str, html: str, markdown: Optional[str], metadata: dict) -> dict:
    final_url = metadata.get("url", url)
    links, internal_links, external_links = extract_link_groups(html, final_url, final_url)
    images = extract_media_items(html, final_url, tag="img", item_type="image", include_alt_title=True)
    audio = extract_media_items(html, final_url, tag="audio", item_type="audio")
    audio.extend(extract_media_items(html, final_url, tag="source", item_type="audio"))
    video = extract_media_items(html, final_url, tag="video", item_type="video")
    video.extend(extract_media_items(html, final_url, tag="source", item_type="video"))
    video.extend(extract_media_items(html, final_url, tag="iframe", item_type="video"))
    documents = extract_document_links(links)
    audio.extend(extract_audio_links(links))
    video.extend(extract_video_links(links))

    def _dedupe(items: list[dict]) -> list[dict]:
        deduped = []
        seen_urls: set[str] = set()
        for item in items:
            link = item.get("url")
            if not link or link in seen_urls:
                continue
            seen_urls.add(link)
            deduped.append(item)
        return deduped

    page_metadata = dict(metadata)
    title = extract_title(html)
    if title:
        page_metadata["title"] = title

    return {
        "url": final_url,
        "text": strip_html_tags(html),
        "markdown": markdown or "",
        "html": html,
        "internal_links": internal_links,
        "external_links": external_links,
        "images": _dedupe(images),
        "audio": _dedupe(audio),
        "video": _dedupe(video),
        "documents": _dedupe(documents),
        "metadata": page_metadata,
    }


def extract_links(html: str, base_url: str) -> list[str]:
    links, _, _ = extract_link_groups(html, base_url, base_url)
    return links


def save_json(path: str, payload: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def decode_duckduckgo_result_url(url: str) -> str:
    parsed = urlparse(url)
    if "duckduckgo.com" not in (parsed.netloc or "").lower():
        return url
    query = parse_qs(parsed.query)
    redirected = query.get("uddg")
    if redirected and redirected[0]:
        return redirected[0]
    return url


def parse_duckduckgo_results(html: str, max_results: int) -> list[dict]:
    result_pattern = re.compile(
        r'<a[^>]*class=["\'][^"\']*result__a[^"\']*["\'][^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
        re.IGNORECASE | re.DOTALL,
    )
    snippet_pattern = re.compile(
        r'<a[^>]*class=["\'][^"\']*result__snippet[^"\']*["\'][^>]*>(.*?)</a>|<div[^>]*class=["\'][^"\']*result__snippet[^"\']*["\'][^>]*>(.*?)</div>',
        re.IGNORECASE | re.DOTALL,
    )

    snippets = [strip_html_tags((m.group(1) or m.group(2) or "")) for m in snippet_pattern.finditer(html)]
    items = []
    seen: set[str] = set()
    for match in result_pattern.finditer(html):
        if len(items) >= max_results:
            break
        href = decode_duckduckgo_result_url(unescape(match.group(1).strip()))
        title = strip_html_tags(match.group(2))
        if not href or href in seen:
            continue
        seen.add(href)
        snippet = snippets[len(items)] if len(snippets) > len(items) else ""
        items.append(
            {
                "rank": len(items) + 1,
                "title": title,
                "url": href,
                "snippet": snippet,
            }
        )
    return items


# ---------------------------------------------------------------------------
# Core scrape
# ---------------------------------------------------------------------------

async def scrape_url(
    scrape_id: str,
    url: str,
    options: ScrapeOptions,
    team_id: str,
    crawl_id: Optional[str] = None,
) -> ScrapeResult:
    feature_flags = build_feature_flags(url, options)
    print(f"[{scrape_id}] Scraping {url} with flags: {[f.value for f in feature_flags]}")

    engines_to_try = list(ENGINES)

    if FeatureFlag.PDF in feature_flags or FeatureFlag.DOCUMENT in feature_flags:
        engines_to_try = ["fetch", "playwright", "fire-engine"]

    if options.proxy in ("stealth", "enhanced"):
        engines_to_try = ["fire-engine"] + [e for e in engines_to_try if e != "fire-engine"]

    last_error: Optional[Exception] = None

    for engine in engines_to_try:
        print(f"[{scrape_id}] Trying engine: {engine}")
        try:
            engine_result = await asyncio.wait_for(
                scrape_with_engine(url, engine, options),
                timeout=(options.timeout or 30000) / 1000,
            )

            html = engine_result.get("html", "")
            status = engine_result.get("status_code", 0)
            is_good_status = 200 <= status < 300 or status == 304
            has_content = len(html.strip()) > 0

            if has_content or (not is_good_status and status > 0):
                document = Document(
                    raw_html=html,
                    markdown=html_to_markdown(html) if "markdown" in options.formats else None,
                    metadata={
                        "source_url": url,
                        "url": engine_result.get("url", url),
                        "status_code": status,
                        "engine": engine,
                        "team_id": team_id,
                        "crawl_id": crawl_id,
                        "content_type": engine_result.get("content_type"),
                        "engine_error": engine_result.get("error"),
                    },
                )
                structured_page = build_structured_page(
                    url=url,
                    html=document.raw_html or "",
                    markdown=document.markdown,
                    metadata=document.metadata,
                )
                document.text = structured_page["text"]
                document.internal_links = structured_page["internal_links"]
                document.external_links = structured_page["external_links"]
                document.images = structured_page["images"]
                document.audio = structured_page["audio"]
                document.video = structured_page["video"]
                document.documents = structured_page["documents"]
                document.structured_data = structured_page
                print(f"[{scrape_id}] Engine {engine} succeeded (status={status})")
                return ScrapeResult(success=True, document=document)

            raise EngineUnsuccessfulError(engine)

        except asyncio.TimeoutError:
            last_error = ScrapeTimeoutError(f"Engine {engine} timed out")
            print(f"[{scrape_id}] Engine {engine} timed out")
        except EngineUnsuccessfulError as e:
            last_error = e
            print(f"[{scrape_id}] Engine {engine} returned empty content")
        except Exception as e:
            last_error = e
            print(f"[{scrape_id}] Engine {engine} error: {e}")

    print(f"[{scrape_id}] All engines failed for {url}")
    return ScrapeResult(success=False, error=last_error or NoEnginesLeftError(url))


# ---------------------------------------------------------------------------
# Crawler
# ---------------------------------------------------------------------------

class Crawler:
    def __init__(
        self,
        redis_url: Optional[str] = None,
        max_concurrency: int = 5,
        store: Optional[CrawlStore] = None,
        output_dir: str = "output",
    ):
        self.max_concurrency = max_concurrency
        self.output_dir = output_dir
        self.page_output_dir = os.path.join(output_dir, "pages")
        self.search_output_dir = os.path.join(output_dir, "search")

        if store is not None:
            self.store = store
            print("[crawler] Using custom store")
            return

        redis_url = redis_url or os.getenv("REDIS_URL")
        if redis_url:
            self.store = RedisCrawlStore(redis_url)
            print(f"[crawler] Using Redis store: {redis_url}")
        else:
            self.store = InMemoryCrawlStore()
            print("[crawler] No REDIS_URL set — using in-memory store (local mode)")

    def _page_artifact_path(self, crawl_id: str, url: str) -> str:
        base = safe_filename(url, default="page")
        digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:10]
        return os.path.join(self.page_output_dir, crawl_id, f"{base}-{digest}.json")

    def save_page_artifact(self, crawl_id: str, document: Document):
        artifact = document.structured_data
        if not artifact:
            artifact = build_structured_page(
                url=document.metadata.get("url", ""),
                html=document.raw_html or "",
                markdown=document.markdown,
                metadata=document.metadata,
            )
            document.structured_data = artifact
        path = self._page_artifact_path(crawl_id, artifact.get("url", document.metadata.get("url", "")))
        save_json(path, artifact)

    async def search_web(self, query: str, max_results: int = 10) -> dict:
        search_url = "https://duckduckgo.com/html/"
        headers = {"User-Agent": f"{DEFAULT_USER_AGENT} (+duckduckgo-search)"}
        results: list[dict] = []
        error: Optional[str] = None
        try:
            async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
                response = await client.get(search_url, params={"q": query}, headers=headers)
                response.raise_for_status()
            results = parse_duckduckgo_results(response.text, max_results=max_results)
        except Exception as exc:
            error = str(exc)

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
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        search_file = os.path.join(self.search_output_dir, f"{safe_filename(query, 'search')}-{stamp}.json")
        save_json(search_file, payload)
        return payload

    async def search_and_crawl(
        self,
        query: str,
        team_id: str = "local-dev",
        max_results: int = 5,
        crawler_options: Optional[CrawlerOptions] = None,
        scrape_options: Optional[ScrapeOptions] = None,
    ) -> dict:
        search_payload = await self.search_web(query=query, max_results=max_results)
        crawl_opts = crawler_options or CrawlerOptions(max_depth=0, limit=1, allow_subdomains=True)
        scrape_opts = scrape_options or ScrapeOptions(formats=["markdown", "html"])

        all_docs: list[Document] = []
        for item in search_payload.get("results", []):
            target_url = item.get("url")
            if not target_url:
                continue
            crawl_id = await self.start_crawl(
                origin_url=target_url,
                crawler_options=crawl_opts,
                scrape_options=scrape_opts,
                team_id=team_id,
            )
            docs = await self.run_crawl(crawl_id)
            all_docs.extend(docs)

        return {"search": search_payload, "documents": all_docs}

    async def start_crawl(
        self,
        origin_url: str,
        crawler_options: CrawlerOptions,
        scrape_options: ScrapeOptions,
        team_id: str,
    ) -> str:
        crawl_id = str(uuid.uuid4())
        crawl = StoredCrawl(
            origin_url=origin_url,
            crawler_options=crawler_options,
            scrape_options=scrape_options,
            team_id=team_id,
        )
        await self.store.save_crawl(crawl_id, crawl)
        await self.store.mark_crawl_active(crawl_id)
        print(f"[crawl:{crawl_id}] Started crawl for {origin_url}")
        return crawl_id

    async def run_crawl(
        self,
        crawl_id: str,
        on_document=None,
    ) -> list[Document]:
        crawl_data = await self.store.get_crawl(crawl_id)
        if not crawl_data:
            raise ValueError(f"Crawl {crawl_id} not found")

        options = CrawlerOptions(**crawl_data["crawler_options"])
        scrape_opts = ScrapeOptions(**crawl_data["scrape_options"])
        origin_url = crawl_data["origin_url"]
        team_id = crawl_data["team_id"]
        url_filter = UrlFilter(options, origin_url)

        queue: asyncio.Queue[tuple[str, int]] = asyncio.Queue()
        queue.put_nowait((origin_url, 0))

        await self.store.lock_url(
            crawl_id,
            origin_url,
            limit=options.limit,
            ignore_query_params=options.ignore_query_parameters,
            deduplicate_similar=options.deduplicate_similar_urls,
        )

        results: list[Document] = []
        semaphore = asyncio.Semaphore(self.max_concurrency)
        active_tasks: set[asyncio.Task] = set()

        async def process_url(url: str, depth: int):
            async with semaphore:
                job_id = str(uuid.uuid4())
                await self.store.add_crawl_job(crawl_id, job_id)

                result = await scrape_url(
                    scrape_id=job_id,
                    url=url,
                    options=scrape_opts,
                    team_id=team_id,
                    crawl_id=crawl_id,
                )

                await self.store.add_crawl_job_done(crawl_id, job_id, result.success)

                if result.success and result.document:
                    results.append(result.document)
                    try:
                        self.save_page_artifact(crawl_id, result.document)
                    except Exception as artifact_error:
                        result.document.warning = f"artifact_save_failed: {artifact_error}"
                    if on_document:
                        await on_document(url, result.document)

                    if depth < options.max_depth:
                        new_urls = extract_links(result.document.raw_html or "", url)
                        for new_url in new_urls:
                            if not url_filter.is_allowed(new_url, depth + 1):
                                continue

                            locked = await self.store.lock_url(
                                crawl_id,
                                new_url,
                                limit=options.limit,
                                ignore_query_params=options.ignore_query_parameters,
                                deduplicate_similar=options.deduplicate_similar_urls,
                            )
                            if locked:
                                queue.put_nowait((new_url, depth + 1))

        while not queue.empty() or active_tasks:
            while not queue.empty():
                url, depth = queue.get_nowait()
                task = asyncio.create_task(process_url(url, depth))
                active_tasks.add(task)
                task.add_done_callback(active_tasks.discard)

            if active_tasks:
                await asyncio.wait(active_tasks, return_when=asyncio.FIRST_COMPLETED)

        await self.store.finish_crawl(crawl_id)
        await self.store.finish_kickoff(crawl_id)
        print(f"[crawl:{crawl_id}] Finished. {len(results)} pages scraped.")
        return results


# ---------------------------------------------------------------------------
# Example usage
# ---------------------------------------------------------------------------

async def main():
    crawler = Crawler(max_concurrency=3)

    crawl_id = await crawler.start_crawl(
        origin_url="https://example.com",
        crawler_options=CrawlerOptions(
            max_crawled_links=10,
            max_depth=2,
            limit=10,
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
        print(
            f"  + {url} | text={len(doc.text or '')} chars | "
            f"images={len(doc.images)} audio={len(doc.audio)} video={len(doc.video)} docs={len(doc.documents)}"
        )

    docs = await crawler.run_crawl(crawl_id, on_document=on_doc)
    print(f"\nTotal documents: {len(docs)}")


if __name__ == "__main__":
    asyncio.run(main())
