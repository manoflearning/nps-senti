from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Dict, List

import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

from .config import CrawlerConfig, load_config
from .dedupe.simhash_dedupe import SimhashDeduper
from .discovery.gdelt import GdeltConfig, GdeltDiscoverer
from .discovery.youtube import YouTubeDiscoverer
from .discovery.forums import ForumsDiscoverer
from .extract.extractor import Extractor
from .fetch.fetcher import Fetcher
from .models import Candidate, Document
from .storage.index import DocumentIndex
from .storage.writer import JsonlWriter
from .utils import normalize_url

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
    dedupe_duplicates: int
    extraction_failed: int


class UnifiedPipeline:
    def __init__(self, config: CrawlerConfig) -> None:
        self.config = config
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
        self.deduper = SimhashDeduper()
        self.storage = JsonlWriter(
            config.output.root,
            config.runtime.run_id,
            file_name=config.output.file_name,
        )
        self.index = DocumentIndex(self.storage.output_dir)

    def _trim_candidates(self, candidates: List[Candidate]) -> List[Candidate]:
        max_total = self.config.limits.max_candidates_per_source
        if len(candidates) > max_total:
            return candidates[:max_total]
        return candidates

    def discover(self) -> Dict[str, List[Candidate]]:
        discoveries: Dict[str, List[Candidate]] = {}
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
            ),
        )
        yt = YouTubeDiscoverer(
            api_key=os.environ.get("YOUTUBE_API_KEY"),
            keywords=self.config.keywords,
            start_date=self.config.time_window.start_date,
            end_date=self.config.time_window.end_date,
        )

        # Forums discoverer
        forum_sites = self.config.forums.sites if hasattr(self.config, "forums") else {}
        forums = ForumsDiscoverer(
            session=self.session,
            request_timeout=self.config.limits.request_timeout_sec,
            user_agent=self.fetcher.config.user_agent,
            sites_config=forum_sites,
        )

        discoveries["gdelt"] = self._trim_candidates(gdelt.discover())
        discoveries["youtube"] = self._trim_candidates(yt.discover())
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
        ordered_sources = ["dcinside", "bobaedream", "fmkorea", "mlbpark", "theqoo", "ppomppu", "gdelt", "youtube"]
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
        dedupe_duplicates = 0
        extraction_failed = 0

        for candidate in tqdm(
            all_candidates[: self.config.limits.max_fetch_per_run],
            desc="Fetching",
            unit="doc",
        ):
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
            document.extra.setdefault("fetch", {})
            document.extra["fetch"].update(
                {
                    "encoding": fetch_result.encoding,
                    "status_code": fetch_result.status_code,
                    "fetched_from": fetch_result.fetched_from,
                }
            )
            if self.index.contains(document.id):
                duplicates += 1
                index_duplicates += 1
                continue
            if self.deduper.add(document):
                duplicates += 1
                dedupe_duplicates += 1
                continue
            self.storage.append(document)
            self.index.add(document.id)
            stored += 1

        stats = PipelineStats(
            discovered={k: len(v) for k, v in discovered.items()},
            fetched=fetched,
            stored=stored,
            duplicates_skipped=duplicates,
            failed_fetch=failed_fetch,
            quality_rejected=quality_rejected,
            index_duplicates=index_duplicates,
            dedupe_duplicates=dedupe_duplicates,
            extraction_failed=extraction_failed,
        )
        logger.info("Pipeline completed stats=%s", stats)
        self.index.flush()
        return stats


def run_pipeline() -> PipelineStats:
    config = load_config()
    pipeline = UnifiedPipeline(config)
    return pipeline.run()
