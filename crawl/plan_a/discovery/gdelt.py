from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional, Set

import requests

from ..models import Candidate

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class GdeltConfig:
    max_records_per_keyword: int = 75
    chunk_days: int = 30
    overlap_days: int = 0


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

    def _iter_windows(self) -> Iterable[tuple[datetime, datetime]]:
        start = self.start_date
        end = self.end_date or datetime.now(timezone.utc)
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

    def _build_params(self, keyword: str, window_start: datetime, window_end: datetime) -> dict[str, str]:
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
        seen_urls: Set[str] = set()
        candidates: List[Candidate] = []
        for keyword in self.keywords:
            if len(keyword.strip()) < 3:
                continue
            for window_start, window_end in self._iter_windows():
                params = self._build_params(keyword, window_start, window_end)
                try:
                    response = self.session.get(
                        self.API_URL,
                        params=params,
                        timeout=self.request_timeout,
                    )
                    response.raise_for_status()
                except requests.RequestException as exc:
                    logger.warning(
                        "GDELT request failed: keyword=%s window=%s-%s error=%s",
                        keyword,
                        window_start.date(),
                        window_end.date(),
                        exc,
                    )
                    continue
                try:
                    payload = response.json()
                except ValueError as exc:
                    logger.warning(
                        "GDELT JSON decode failed: keyword=%s window=%s-%s error=%s",
                        keyword,
                        window_start.date(),
                        window_end.date(),
                        exc,
                    )
                    continue
                articles = payload.get("articles", [])
                for article in articles:
                    url = article.get("url")
                    if not url or url in seen_urls:
                        continue
                    seen_urls.add(url)
                    seendate = article.get("seendate")
                    timestamp = None
                    if seendate:
                        try:
                            timestamp = datetime.strptime(seendate, "%Y%m%d")
                        except ValueError:
                            timestamp = None
                    candidates.append(
                        Candidate(
                            url=url,
                            source="gdelt",
                            discovered_via={
                                "type": "gdelt",
                                "keyword": keyword,
                                "seendate": seendate,
                                "window": {
                                    "start": window_start.isoformat(),
                                    "end": window_end.isoformat(),
                                },
                            },
                            snapshot_url=None,
                            timestamp=timestamp,
                            title=article.get("title"),
                            extra={"gdelt": article},
                        )
                    )
        logger.info("GDELT discovered %d candidates", len(candidates))
        return candidates
