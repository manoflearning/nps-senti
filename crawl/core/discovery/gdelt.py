from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from time import sleep
from typing import Iterable, List, Optional, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import requests

from ..models import Candidate

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class GdeltConfig:
    max_records_per_keyword: int = 75
    chunk_days: int = 30
    overlap_days: int = 0
    pause_between_requests: float = 1.0
    max_attempts: int = 3
    rate_limit_backoff_sec: float = 5.0
    max_concurrency: int = 4
    max_days_back: Optional[int] = None


class GdeltDiscoverer:
    API_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

    def __init__(
        self,
        session: requests.Session,
        keywords: Iterable[str],
        languages: Iterable[str],
        start_date: datetime,
        end_date: Optional[datetime],
        request_timeout: int,
        config: GdeltConfig | None = None,
    ) -> None:
        self.session = session
        self.keywords = [kw for kw in keywords if kw.strip()]
        self.languages = [lang.lower() for lang in languages]
        self.start_date = start_date
        self.end_date = end_date
        self.request_timeout = request_timeout
        self.config = config or GdeltConfig()
        if self.config.chunk_days <= 0:
            self.config.chunk_days = 30
        if self.config.max_records_per_keyword <= 0:
            self.config.max_records_per_keyword = 75
        if self.config.pause_between_requests < 0:
            self.config.pause_between_requests = 0.0
        if self.config.max_attempts <= 0:
            self.config.max_attempts = 1
        if self.config.rate_limit_backoff_sec < 0:
            self.config.rate_limit_backoff_sec = 0.0

    def _iter_windows(self) -> Iterable[tuple[datetime, datetime]]:
        end = self.end_date or datetime.now(timezone.utc)
        start = self.start_date
        if self.config.max_days_back and self.config.max_days_back > 0:
            clamp_start = end - timedelta(days=self.config.max_days_back)
            if clamp_start > start:
                start = clamp_start
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)
        chunk = timedelta(days=self.config.chunk_days)
        overlap = timedelta(days=max(self.config.overlap_days, 0))
        current_start = start
        while current_start < end:
            window_end = min(current_start + chunk, end)
            yield current_start, window_end
            next_start = window_end - overlap
            if next_start <= current_start:
                next_start = window_end
            current_start = next_start

    def _build_params(
        self, keyword: str, window_start: datetime, window_end: datetime
    ) -> dict[str, str]:
        if " " in keyword.strip():
            query_term = f'"{keyword}"'
        else:
            query_term = keyword
        params = {
            "query": query_term,
            "mode": "ArtList",
            "format": "json",
            "maxrecords": str(self.config.max_records_per_keyword),
            "sort": "DateDesc",
        }
        if self.languages:
            gdelt_langs = []
            for lang in self.languages:
                if lang == "ko":
                    gdelt_langs.append("sourcelang:KOREAN")
                elif lang == "en":
                    gdelt_langs.append("sourcelang:ENGLISH")
                else:
                    gdelt_langs.append(f"lang:{lang.upper()}")
            if gdelt_langs:
                if len(gdelt_langs) == 1:
                    language_clause = gdelt_langs[0]
                else:
                    language_clause = f"({' OR '.join(gdelt_langs)})"
                params["query"] = f"{query_term} {language_clause}"
        params["startdatetime"] = window_start.strftime("%Y%m%d%H%M%S")
        window_end_inclusive = window_end - timedelta(seconds=1)
        if window_end_inclusive < window_start:
            window_end_inclusive = window_start
        params["enddatetime"] = window_end_inclusive.strftime("%Y%m%d%H%M%S")
        return params

    def discover(self) -> List[Candidate]:
        windows = list(self._iter_windows())
        tasks: List[Tuple[str, datetime, datetime]] = []
        for keyword in self.keywords:
            if len(keyword.strip()) < 3:
                continue
            for window_start, window_end in windows:
                tasks.append((keyword, window_start, window_end))

        seen_urls: Set[str] = set()
        seen_lock = threading.Lock()
        results: List[Candidate] = []

        def worker(kw: str, ws: datetime, we: datetime) -> List[Candidate]:
            params = self._build_params(kw, ws, we)
            attempt = 0
            response = None
            while attempt < self.config.max_attempts:
                try:
                    response = self.session.get(
                        self.API_URL,
                        params=params,
                        timeout=self.request_timeout,
                    )
                    if response.status_code == 429:
                        retry_after = response.headers.get("Retry-After")
                        wait = None
                        if retry_after:
                            try:
                                wait = float(retry_after)
                            except ValueError:
                                wait = None
                        if wait is None:
                            wait = self.config.rate_limit_backoff_sec * (2**attempt)
                        logger.info(
                            "GDELT 429 rate-limited. Sleeping %.1fs (attempt %d)",
                            wait,
                            attempt + 1,
                        )
                        sleep(wait)
                        attempt += 1
                        continue
                    response.raise_for_status()
                    break
                except requests.RequestException as exc:
                    attempt += 1
                    logger.warning(
                        "GDELT request failed: kw=%s window=%s-%s attempt=%d error=%s",
                        kw,
                        ws.date(),
                        we.date(),
                        attempt,
                        exc,
                    )
                    if attempt < self.config.max_attempts:
                        sleep(self.config.rate_limit_backoff_sec * attempt)
                    else:
                        response = None
                        break
            if response is None:
                return []
            try:
                payload = response.json()
            except ValueError as exc:
                logger.warning(
                    "GDELT JSON decode failed: kw=%s window=%s-%s error=%s",
                    kw,
                    ws.date(),
                    we.date(),
                    exc,
                )
                return []
            articles = payload.get("articles", [])
            batch: List[Candidate] = []
            for article in articles:
                url = article.get("url")
                if not url:
                    continue
                with seen_lock:
                    if url in seen_urls:
                        continue
                    seen_urls.add(url)
                seendate = article.get("seendate")
                timestamp = None
                if seendate:
                    try:
                        # Prefer full timestamp with time if available (e.g., 20251123T143000Z)
                        if "T" in seendate:
                            ts = datetime.strptime(seendate, "%Y%m%dT%H%M%SZ")
                        else:
                            ts = datetime.strptime(seendate, "%Y%m%d")
                        timestamp = ts.replace(tzinfo=timezone.utc)
                    except ValueError:
                        timestamp = None
                batch.append(
                    Candidate(
                        url=url,
                        source="gdelt",
                        discovered_via={
                            "type": "gdelt",
                            "keyword": kw,
                            "seendate": seendate,
                            "window": {
                                "start": ws.isoformat(),
                                "end": we.isoformat(),
                            },
                        },
                        snapshot_url=None,
                        timestamp=timestamp,
                        title=article.get("title"),
                        extra={"gdelt": article},
                    )
                )
            if self.config.pause_between_requests:
                sleep(self.config.pause_between_requests)
            return batch

        max_workers = max(1, int(self.config.max_concurrency))
        if len(tasks) <= 1 or max_workers == 1:
            for kw, ws, we in tasks:
                results.extend(worker(kw, ws, we))
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futures = [ex.submit(worker, kw, ws, we) for kw, ws, we in tasks]
                for fut in as_completed(futures):
                    try:
                        results.extend(fut.result())
                    except Exception as exc:  # noqa: BLE001
                        logger.debug("GDELT worker error: %s", exc)

        logger.info("GDELT discovered %d candidates", len(results))
        return results
