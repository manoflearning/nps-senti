from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Dict, List, cast

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

from .config import CrawlerConfig, load_config
from .discovery.gdelt import GdeltConfig, GdeltDiscoverer
from .discovery.youtube import YouTubeDiscoverer
from .discovery.forums import ForumsDiscoverer
from .extract.extractor import Extractor
from .fetch.fetcher import Fetcher
from .models import Candidate
from .storage.index import DocumentIndex
from .storage.writer import MultiSourceJsonlWriter
from .utils import normalize_url

load_dotenv()

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PipelineStats:
    discovered: Dict[str, int]
    fetched: int
    stored: int
    duplicates_skipped: int
    failed_fetch: int
    quality_rejected: int
    index_duplicates: int
    extraction_failed: int


class UnifiedPipeline:
    def __init__(
        self,
        config: CrawlerConfig,
        include_sources: set[str] | None = None,
        forum_sites_filter: set[str] | None = None,
        max_fetch: int | None = None,
    ) -> None:
        self.config = config
        # Optional limiter to run only selected sources
        # Accepted values: {"gdelt", "youtube", "forums"}
        self.include_sources = include_sources
        # Optional: within forums, include only specific site keys
        self.forum_sites_filter = forum_sites_filter
        # Optional cap on number of fetch attempts in this run
        self.max_fetch = max_fetch if (max_fetch is None or max_fetch > 0) else None
        self.session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "HEAD"),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.fetcher = Fetcher(
            self.session,
            timeout=config.limits.request_timeout_sec,
        )
        self.session.headers.update({"User-Agent": self.fetcher.config.user_agent})
        self.extractor = Extractor(
            config.keywords,
            config.lang,
            config.quality,
        )
        # Write to per-source JSONL files
        self.storage = MultiSourceJsonlWriter(config.output.root)
        self.index = DocumentIndex(self.storage.output_dir)

    def _trim_candidates(self, candidates: List[Candidate]) -> List[Candidate]:
        max_total = self.config.limits.max_candidates_per_source
        if len(candidates) > max_total:
            return candidates[:max_total]
        return candidates

    def discover(self) -> Dict[str, List[Candidate]]:
        discoveries: Dict[str, List[Candidate]] = {}

        # Helper to check if a logical source should run
        def _should_run(key: str) -> bool:
            return not self.include_sources or key in self.include_sources

        # GDELT discoverer (can be disabled via config)
        gdelt_candidates: List[Candidate] = []
        if _should_run("gdelt") and getattr(self.config.gdelt, "enabled", True):
            gdelt = GdeltDiscoverer(
                session=self.session,
                keywords=self.config.keywords,
                languages=self.config.lang,
                start_date=self.config.time_window.start_date,
                end_date=self.config.time_window.end_date,
                request_timeout=self.config.limits.request_timeout_sec,
                config=GdeltConfig(
                    max_records_per_keyword=self.config.gdelt.max_records_per_keyword,
                    chunk_days=self.config.gdelt.chunk_days,
                    overlap_days=self.config.gdelt.overlap_days,
                    pause_between_requests=self.config.gdelt.pause_between_requests,
                    max_attempts=self.config.gdelt.max_attempts,
                    rate_limit_backoff_sec=self.config.gdelt.rate_limit_backoff_sec,
                    max_concurrency=self.config.gdelt.max_concurrency,
                    max_days_back=self.config.gdelt.max_days_back,
                ),
            )
            gdelt_candidates = gdelt.discover()
        yt = None
        if _should_run("youtube"):
            yt = YouTubeDiscoverer(
                api_key=os.environ.get("YOUTUBE_API_KEY"),
                keywords=self.config.keywords,
                start_date=self.config.time_window.start_date,
                end_date=self.config.time_window.end_date,
            )

        # Forums discoverer
        forum_sites = self.config.forums.sites if hasattr(self.config, "forums") else {}
        # Apply forum sites filter if provided
        if self.forum_sites_filter:
            forum_sites = {
                k: v for k, v in forum_sites.items() if k in self.forum_sites_filter
            }
        forums = None
        if _should_run("forums"):
            forums = ForumsDiscoverer(
                session=self.session,
                request_timeout=self.config.limits.request_timeout_sec,
                user_agent=self.fetcher.config.user_agent,
                sites_config=forum_sites,
            )

        if _should_run("gdelt"):
            discoveries["gdelt"] = self._trim_candidates(gdelt_candidates)
        if yt is not None:
            discoveries["youtube"] = self._trim_candidates(yt.discover())
        if forums is not None:
            forum_results = forums.discover()
            for site, cands in forum_results.items():
                discoveries[site] = self._trim_candidates(cands)
        return discoveries

    def run(self) -> PipelineStats:
        logger.info("Starting unified pipeline: run_id=%s", self.config.runtime.run_id)
        discovered = self.discover()

        unique_candidates: Dict[str, Candidate] = {}
        for source, candidates in discovered.items():
            for candidate in candidates:
                if not candidate.url:
                    continue
                lowered = candidate.url.lower()
                if lowered.endswith("/robots.txt") or lowered.endswith("robots.txt"):
                    continue
                norm = normalize_url(candidate.url)
                if norm.endswith("/") or norm.count("/") <= 2:
                    # Skip bare domain/homepage captures
                    continue
                if norm not in unique_candidates:
                    unique_candidates[norm] = candidate

        # prioritize forums and gdelt first, youtube last (meta only)
        ordered_sources = [
            "dcinside",
            "bobaedream",
            "fmkorea",
            "mlbpark",
            "theqoo",
            "ppomppu",
            "gdelt",
            "youtube",
        ]
        all_candidates: List[Candidate] = []
        for source in ordered_sources:
            for candidate in unique_candidates.values():
                if candidate.source == source:
                    all_candidates.append(candidate)
        # append any remaining candidates whose source wasn't in the ordered list
        remaining = [c for c in unique_candidates.values() if c not in all_candidates]
        all_candidates.extend(remaining)
        logger.info("Total unique candidates: %d", len(all_candidates))

        fetched = 0
        stored = 0
        duplicates = 0
        failed_fetch = 0
        quality_rejected = 0
        index_duplicates = 0
        # dedupe removed
        extraction_failed = 0

        attempted = 0
        for candidate in tqdm(
            all_candidates,
            desc="Fetching",
            unit="doc",
        ):
            if self.max_fetch is not None and attempted >= self.max_fetch:
                break
            attempted += 1
            fetch_result = self.fetcher.fetch(candidate)
            if not fetch_result or not fetch_result.html:
                failed_fetch += 1
                continue
            fetched += 1
            document, quality_info = self.extractor.build_document(
                candidate,
                fetch_result,
                run_id=self.config.runtime.run_id,
            )
            if not document:
                if quality_info and quality_info.get("status") == "quality-reject":
                    quality_rejected += 1
                else:
                    extraction_failed += 1
                continue
            if document.extra is None:
                document.extra = {}
            fetch_meta = cast(dict, document.extra.setdefault("fetch", {}))
            fetch_meta.update(
                {
                    "encoding": fetch_result.encoding,
                    "status_code": fetch_result.status_code,
                    "fetched_from": fetch_result.fetched_from,
                }
            )
            # Skip if we've already stored this exact URL before (regardless of content changes)
            if self.index.contains(document.id) or self.index.contains_url(
                document.url
            ):
                duplicates += 1
                index_duplicates += 1
                continue
            # Keep similar posts; only index-based exact duplicates are filtered
            self.storage.append(document)
            self.index.add(document.id)
            self.index.add_url(document.url)
            stored += 1

        stats = PipelineStats(
            discovered={k: len(v) for k, v in discovered.items()},
            fetched=fetched,
            stored=stored,
            duplicates_skipped=duplicates,
            failed_fetch=failed_fetch,
            quality_rejected=quality_rejected,
            index_duplicates=index_duplicates,
            extraction_failed=extraction_failed,
        )
        logger.info("Pipeline completed stats=%s", stats)
        self.index.flush()
        return stats


def run_pipeline() -> PipelineStats:
    config = load_config()
    pipeline = UnifiedPipeline(config)
    return pipeline.run()
