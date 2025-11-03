from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from time import sleep
from typing import Iterable, List, Optional
from urllib.parse import quote

import requests

from ..models import Candidate

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class CommonCrawlConfig:
    max_indexes: int = 3
    per_domain_limit: int = 50
    pause_between_requests: float = 1.0


class CommonCrawlDiscoverer:
    COLLINFO_URL = "https://index.commoncrawl.org/collinfo.json"

    def __init__(
        self,
        session: requests.Session,
        domains: Iterable[str],
        keywords: Iterable[str],
        request_timeout: int,
        config: CommonCrawlConfig | None = None,
    ) -> None:
        self.session = session
        domain_list = [domain.lower().strip() for domain in domains if domain.strip()]
        self.domains = list(dict.fromkeys(domain_list))
        if not self.domains:
            self.domains = ["nps.or.kr", "blog.naver.com", "news.naver.com"]
        self.keywords = list(keywords)
        self.request_timeout = request_timeout
        self.config = config or CommonCrawlConfig()
        self._indexes_cache: Optional[List[dict]] = None

    def _load_indexes(self) -> List[dict]:
        if self._indexes_cache is not None:
            return self._indexes_cache
        try:
            response = self.session.get(self.COLLINFO_URL, timeout=self.request_timeout)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as exc:
            logger.warning("Unable to load Common Crawl index metadata: %s", exc)
            self._indexes_cache = []
            return self._indexes_cache
        self._indexes_cache = data[: self.config.max_indexes]
        return self._indexes_cache

    def _build_request_url(self, index_api: str, pattern: str) -> str:
        encoded = quote(pattern, safe="/*:")
        return f"{index_api}?url={encoded}&output=json&limit={self.config.per_domain_limit}"

    def _iter_records(self, url: str) -> Iterable[dict]:
        try:
            response = self.session.get(url, timeout=self.request_timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.debug("CommonCrawl request error: %s", exc)
            return
        for line in response.text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                logger.debug("Failed to decode Common Crawl line: %s", line[:200])

    def discover(self) -> List[Candidate]:
        candidates: List[Candidate] = []
        indexes = self._load_indexes()
        for index in indexes:
            index_api = index.get("cdx-api")
            if not index_api:
                continue
            for domain in self.domains:
                pattern = f"{domain}/*"
                request_url = self._build_request_url(index_api, pattern)
                logger.info("CommonCrawl query index=%s domain=%s", index["id"], domain)
                try:
                    for record in self._iter_records(request_url):
                        timestamp_raw = record.get("timestamp")
                        timestamp: Optional[datetime] = None
                        if timestamp_raw:
                            try:
                                timestamp = datetime.strptime(timestamp_raw, "%Y%m%d%H%M%S")
                            except ValueError:
                                timestamp = None
                        filename = record.get("filename")
                        offset = record.get("offset")
                        length = record.get("length")
                        if not filename or not offset or not length:
                            continue
                        try:
                            offset_int = int(offset)
                            length_int = int(length)
                        except (TypeError, ValueError):
                            continue
                        candidates.append(
                            Candidate(
                                url=record.get("redirect") or record.get("url"),
                                source="commoncrawl",
                                discovered_via={
                                    "type": "commoncrawl",
                                    "index": index.get("id"),
                                    "domain": domain,
                                },
                                snapshot_url=None,
                                timestamp=timestamp,
                                title=None,
                                extra={
                                    "commoncrawl": {
                                        "filename": filename,
                                        "offset": offset_int,
                                        "length": length_int,
                                    }
                                },
                            )
                        )
                except requests.HTTPError as exc:
                    logger.debug(
                        "CommonCrawl request failed: index=%s domain=%s error=%s",
                        index.get("id"),
                        domain,
                        exc,
                    )
                    continue
                sleep(self.config.pause_between_requests)
        logger.info("CommonCrawl discovered %d candidates", len(candidates))
        return candidates
