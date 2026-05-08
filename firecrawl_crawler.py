"""
Python port of Firecrawl's core crawl-redis + scrapeURL logic.
Requires: httpx
Optional: redis (only needed when REDIS_URL is set)
"""

import asyncio
import bisect
import json
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional
from urllib.parse import urlparse, urlunparse

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
    timeout: Optional[int] = None
    proxy: Optional[str] = None  # "basic" | "stealth" | "enhanced" | "auto"
    location: Optional[str] = None
    mobile: bool = False
    skip_tls_verification: bool = True
    block_ads: bool = True
    max_age: int = 0
    headers: dict[str, str] = field(default_factory=dict)

@dataclass
class CrawlerOptions:
    max_crawled_links: int = 1000
    max_depth: int = 10
    limit: int = 10000
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
    raw_html: Optional[str] = None
    screenshot: Optional[str] = None
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
# Redis helpers  (crawl-redis.ts equivalent)
# ---------------------------------------------------------------------------

TTL = 24 * 60 * 60  # 24 hours in seconds

class CrawlRedis:
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        try:
            import redis.asyncio as aioredis
        except ImportError:
            raise ImportError(
                "The 'redis' package is required to use Redis mode.\n"
                "Install it with:  pip install redis aioredis\n"
                "Or unset REDIS_URL to use the built-in in-memory store."
            ) from None
        self.redis = aioredis.from_url(redis_url, decode_responses=True)

    # -- Crawl lifecycle ----------------------------------------------------

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

    # -- Job tracking -------------------------------------------------------

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

    # -- URL deduplication --------------------------------------------------

    @staticmethod
    def normalize_url(url: str, ignore_query_params: bool = False) -> str:
        parsed = urlparse(url)
        query = "" if ignore_query_params else parsed.query
        fragment = parsed.fragment
        if not fragment or len(fragment) <= 1 or not (
            fragment.startswith("/") or fragment.startswith("!/")):
            fragment = ""
        return urlunparse((
            parsed.scheme, parsed.netloc, parsed.path,
            parsed.params, query, fragment
        ))

    @staticmethod
    def generate_url_permutations(url: str) -> list[str]:
        """
        Generates www/no-www x http/https x slash/index.html/index.php/bare
        permutations for robust deduplication.
        """
        parsed = urlparse(url)
        hostname = parsed.hostname or ""

        if hostname.startswith("www."):
            hosts = [hostname, hostname[4:]]
        else:
            hosts = ["www." + hostname, hostname]

        schemes = ["http", "https"] if parsed.scheme in ("http", "https") else [parsed.scheme]

        path = parsed.path
        paths: list[str] = []
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

    async def lock_url(
        self,
        crawl_id: str,
        url: str,
        limit: Optional[int],
        ignore_query_params: bool = False,
        deduplicate_similar: bool = False,
    ) -> bool:
        """
        Returns True if the URL was successfully locked (not yet visited).
        Returns False if already visited or limit reached.
        """
        normalized = self.normalize_url(url, ignore_query_params)

        if limit is not None:
            count = await self.redis.scard(f"crawl:{crawl_id}:visited_unique")
            if count >= limit:
                return False

        if deduplicate_similar:
            visit_key = self.generate_url_permutations(normalized)[0]
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
# In-memory store (local-dev fallback — no Redis required)
# ---------------------------------------------------------------------------

class CrawlMemory:
    """In-memory crawl state store for local development (no Redis required)."""

    def __init__(self):
        self._crawls: dict[str, dict] = {}
        self._active_crawls: set[str] = set()
        self._jobs: dict[str, set[str]] = {}
        self._jobs_done: dict[str, set[str]] = {}
        self._jobs_done_ordered: dict[str, list[tuple[float, str]]] = {}
        self._visited: dict[str, set[str]] = {}
        self._visited_unique: dict[str, set[str]] = {}
        self._finish: set[str] = set()
        self._kickoff_finish: set[str] = set()
        self._errors: dict[str, str] = {}
        self._robots_blocked: dict[str, set[str]] = {}

    async def save_crawl(self, crawl_id: str, crawl: StoredCrawl):
        self._crawls[crawl_id] = {
            "origin_url": crawl.origin_url,
            "crawler_options": crawl.crawler_options.__dict__,
            "scrape_options": crawl.scrape_options.__dict__,
            "team_id": crawl.team_id,
            "robots": crawl.robots,
            "cancelled": crawl.cancelled,
            "created_at": crawl.created_at,
            "zero_data_retention": crawl.zero_data_retention,
        }

    async def get_crawl(self, crawl_id: str) -> Optional[dict]:
        return self._crawls.get(crawl_id)

    async def finish_crawl(self, crawl_id: str):
        self._finish.add(crawl_id)
        self._active_crawls.discard(crawl_id)
        self._visited.pop(crawl_id, None)
        self._visited_unique.pop(crawl_id, None)

    async def set_crawl_error(self, crawl_id: str, error: str):
        self._errors[crawl_id] = error

    async def mark_crawl_active(self, crawl_id: str):
        self._active_crawls.add(crawl_id)

    async def add_crawl_job(self, crawl_id: str, job_id: str):
        self._jobs.setdefault(crawl_id, set()).add(job_id)

    async def add_crawl_jobs(self, crawl_id: str, job_ids: list[str]):
        if not job_ids:
            return
        self._jobs.setdefault(crawl_id, set()).update(job_ids)

    async def add_crawl_job_done(self, crawl_id: str, job_id: str, success: bool):
        self._jobs_done.setdefault(crawl_id, set()).add(job_id)
        if success:
            bisect.insort(self._jobs_done_ordered.setdefault(crawl_id, []), (time.time(), job_id))

    async def get_done_jobs_ordered(self, crawl_id: str, start=0, end=-1) -> list[str]:
        ordered = self._jobs_done_ordered.get(crawl_id, [])
        sliced = ordered[start:] if end == -1 else ordered[start:end + 1]
        return [job_id for _, job_id in sliced]

    async def is_crawl_finished(self, crawl_id: str) -> bool:
        done = len(self._jobs_done.get(crawl_id, set()))
        total = len(self._jobs.get(crawl_id, set()))
        kickoff = crawl_id in self._kickoff_finish
        return done == total and kickoff

    async def finish_kickoff(self, crawl_id: str):
        self._kickoff_finish.add(crawl_id)

    @staticmethod
    def normalize_url(url: str, ignore_query_params: bool = False) -> str:
        return CrawlRedis.normalize_url(url, ignore_query_params)

    @staticmethod
    def generate_url_permutations(url: str) -> list[str]:
        return CrawlRedis.generate_url_permutations(url)

    async def lock_url(
        self,
        crawl_id: str,
        url: str,
        limit: Optional[int],
        ignore_query_params: bool = False,
        deduplicate_similar: bool = False,
    ) -> bool:
        normalized = self.normalize_url(url, ignore_query_params)

        if limit is not None:
            if len(self._visited_unique.get(crawl_id, set())) >= limit:
                return False

        visit_key = (
            self.generate_url_permutations(normalized)[0]
            if deduplicate_similar
            else normalized
        )

        visited = self._visited.setdefault(crawl_id, set())
        if visit_key in visited:
            return False

        visited.add(visit_key)
        self._visited_unique.setdefault(crawl_id, set()).add(normalized)
        return True

    async def record_robots_blocked(self, crawl_id: str, url: str):
        self._robots_blocked.setdefault(crawl_id, set()).add(url)


# ---------------------------------------------------------------------------
# URL filtering (WebCrawler.filterURL equivalent)
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
            if not self.options.allow_backward_crawling:
                if target_host != origin_host and target_host != "www." + origin_host:
                    return False

        if self.options.includes:
            if not any(re.search(pat, parsed.path) for pat in self.options.includes):
                return False

        if self.options.excludes:
            if any(re.search(pat, parsed.path) for pat in self.options.excludes):
                return False

        return True

# ---------------------------------------------------------------------------
# Feature flag builder (buildFeatureFlags equivalent)
# ---------------------------------------------------------------------------

DOCUMENT_EXTS = {".docx", ".doc", ".odt", ".rtf", ".xlsx", ".xls"}

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
# Scrape engine stub (scrapeURLWithEngine equivalent)
# ---------------------------------------------------------------------------

async def scrape_with_engine(url: str, engine: str, options: ScrapeOptions) -> dict:
    """
    Stub: replace with actual engine calls (httpx fetch, Playwright, Fire Engine, etc.)
    Returns dict with keys: html, status_code, error, url
    """
    import httpx
    try:
        async with httpx.AsyncClient(verify=not options.skip_tls_verification, timeout=30) as client:
            resp = await client.get(url, headers=options.headers or {})
            return {
                "html": resp.text,
                "status_code": resp.status_code,
                "error": None,
                "url": str(resp.url),
            }
    except Exception as e:
        return {"html": "", "status_code": 0, "error": str(e), "url": url}

# ---------------------------------------------------------------------------
# Core scrapeURL function (scrapeURL/index.ts equivalent)
# ---------------------------------------------------------------------------

ENGINES = ["fetch", "playwright", "fire-engine"]
WATERFALL_DELAY_S = 3.0

async def scrape_url(
    scrape_id: str,
    url: str,
    options: ScrapeOptions,
    team_id: str,
    crawl_id: Optional[str] = None,
) -> ScrapeResult:
    """
    Main scraping function. Tries engines in waterfall order.
    Returns ScrapeResult with success/document/error.
    """
    feature_flags = build_feature_flags(url, options)
    print(f"[{{scrape_id}}] Scraping {{url}} with flags: {{[f.value for f in feature_flags]}}")

    engines_to_try = list(ENGINES)
    if FeatureFlag.PDF in feature_flags or FeatureFlag.DOCUMENT in feature_flags:
        engines_to_try = ["pdf", "fire-engine"]
    if options.proxy in ("stealth", "enhanced"):
        engines_to_try = ["fire-engine"] + [e for e in engines_to_try if e != "fire-engine"]

    last_error: Optional[Exception] = None

    for engine in engines_to_try:
        print(f"[{{scrape_id}}] Trying engine: {{engine}}")
        try:
            engine_result = await asyncio.wait_for(
                scrape_with_engine(url, engine, options),
                timeout=(options.timeout or 300_000) / 1000,
            )

            html = engine_result.get("html", "")
            status = engine_result.get("status_code", 0)
            is_good_status = 200 <= status < 300 or status == 304
            has_content = len(html.strip()) > 0

            if has_content or not is_good_status:
                document = Document(
                    raw_html=html,
                    markdown=None,  # TODO: call html-to-markdown here
                    metadata={
                        "source_url": url,
                        "url": engine_result.get("url", url),
                        "status_code": status,
                        "engine": engine,
                    }
                )
                print(f"[{{scrape_id}}] Engine {{engine}} succeeded (status={{status}})")
                return ScrapeResult(success=True, document=document)
            else:
                raise EngineUnsuccessfulError(engine)

        except asyncio.TimeoutError:
            last_error = ScrapeTimeoutError(f"Engine {{engine}} timed out")
            print(f"[{{scrape_id}}] Engine {{engine}} timed out")
        except EngineUnsuccessfulError as e:
            last_error = e
        except Exception as e:
            last_error = e
            print(f"[{{scrape_id}}] Engine {{engine}} error: {{e}}")

    print(f"[{{scrape_id}}] All engines failed for {{url}}")
    return ScrapeResult(success=False, error=last_error or NoEnginesLeftError(url))

# ---------------------------------------------------------------------------
# HTML link extractor (filterLinks equivalent)
# ---------------------------------------------------------------------------

def extract_links(html: str, base_url: str) -> list[str]:
    """Extract all <a href> links from HTML, resolved to absolute URLs."""
    from urllib.parse import urljoin
    pattern = re.compile(r'<a\s[^>]*href=["\']([^"\']+)["\']', re.IGNORECASE)
    links = []
    for match in pattern.finditer(html):
        href = match.group(1).strip()
        if href and not href.startswith(("#", "javascript:", "mailto:")):
            absolute = urljoin(base_url, href)
            links.append(absolute)
    return list(set(links))

# ---------------------------------------------------------------------------
# Crawl orchestrator (queue-worker equivalent)
# ---------------------------------------------------------------------------

class Crawler:
    def __init__(self,
        redis_url: Optional[str] = None,
        max_concurrency: int = 5,
    ):
        resolved_url = redis_url or os.environ.get("REDIS_URL", "").strip()
        if resolved_url:
            self.store = CrawlRedis(resolved_url)
            print(f"[crawler] Using Redis store at {resolved_url}")
        else:
            self.store = CrawlMemory()
            print("[crawler] No REDIS_URL set — using in-memory store (local mode)")
        self.max_concurrency = max_concurrency

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
        print(f"[crawl:{{crawl_id}}] Started crawl for {{origin_url}}")
        return crawl_id

    async def run_crawl(
        self,
        crawl_id: str,
        on_document=None,
    ) -> list[Document]:
        crawl_data = await self.store.get_crawl(crawl_id)
        if not crawl_data:
            raise ValueError(f"Crawl {{crawl_id}} not found")

        options = CrawlerOptions(**crawl_data["crawler_options"])
        scrape_opts = ScrapeOptions(**crawl_data["scrape_options"])
        origin_url = crawl_data["origin_url"]
        team_id = crawl_data["team_id"]
        url_filter = UrlFilter(options, origin_url)

        queue: asyncio.Queue[tuple[str, int]] = asyncio.Queue()
        queue.put_nowait((origin_url, 0))

        await self.store.lock_url(
            crawl_id, origin_url,
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

                success = result.success
                await self.store.add_crawl_job_done(crawl_id, job_id, success)

                if success and result.document:
                    results.append(result.document)
                    if on_document:
                        await on_document(url, result.document)

                    new_urls = extract_links(result.document.raw_html or "", url)
                    for new_url in new_urls:
                        if not url_filter.is_allowed(new_url, depth + 1):
                            continue
                        locked = await self.store.lock_url(
                            crawl_id, new_url,
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
        print(f"[crawl:{{crawl_id}}] Finished. {{len(results)}} pages scraped.")
        return results

# ---------------------------------------------------------------------------
# Example usage
# ---------------------------------------------------------------------------

async def main():
    # No redis_url needed for local development — uses in-memory store by default.
    # Set REDIS_URL env var to switch to Redis (e.g. for production).
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
            timeout=30_000,
        ),
        team_id="my-team",
    )

    async def on_doc(url, doc):
        print(f"  + {{url}} ({{len(doc.raw_html or '')}} bytes)")

    docs = await crawler.run_crawl(crawl_id, on_document=on_doc)
    print(f"\nTotal documents: {{len(docs)}}")

if __name__ == "__main__":
    asyncio.run(main())
