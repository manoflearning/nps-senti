from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests
import os

from ..models import Candidate, FetchResult

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class FetcherConfig:
    # You can override via env CRAWLER_USER_AGENT to include contact info
    user_agent: str = os.environ.get(
        "CRAWLER_USER_AGENT",
        "nps-senti-crawler/0.1 (contact: set CRAWLER_USER_AGENT)",
    )
    pause_seconds: float = 0.5
    allow_live_fetch: bool = True


class RobotsCache:
    def __init__(
        self, session: requests.Session, timeout: int, user_agent: str
    ) -> None:
        self.session = session
        self.timeout = timeout
        self.user_agent = user_agent
        self.cache: dict[str, RobotFileParser] = {}
        self._allow_all_hosts: set[str] = set()

    def allowed(self, url: str) -> bool:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return False
        base = f"{parsed.scheme}://{parsed.netloc}"
        if base in self._allow_all_hosts:
            return True
        parser = self.cache.get(base)
        if parser is None:
            parser = RobotFileParser()
            robots_url = f"{base}/robots.txt"
            try:
                response = self.session.get(
                    robots_url,
                    headers={"User-Agent": self.user_agent},
                    timeout=self.timeout,
                )
                if response.status_code >= 400:
                    # Treat as allow-all if robots not available
                    parser.parse([])
                    self._allow_all_hosts.add(base)
                else:
                    parser.parse(response.text.splitlines())
            except requests.RequestException:
                parser.parse([])
                self._allow_all_hosts.add(base)
            self.cache[base] = parser
        if base in self._allow_all_hosts:
            return True
        return parser.can_fetch(self.user_agent, url)


class Fetcher:
    def __init__(
        self,
        session: requests.Session,
        timeout: int,
        config: FetcherConfig | None = None,
    ) -> None:
        self.session = session
        self.timeout = timeout
        self.config = config or FetcherConfig()
        self.robots = RobotsCache(session, timeout, self.config.user_agent)

    def _decode_bytes(
        self, body: bytes, content_type: Optional[str]
    ) -> tuple[str, Optional[str]]:
        encoding = None
        if content_type:
            lower = content_type.lower()
            if "charset=" in lower:
                encoding = lower.split("charset=")[-1].split(";")[0].strip()
        candidates = [encoding, "utf-8", "euc-kr", "cp949", "latin-1"]
        for enc in candidates:
            if not enc:
                continue
            try:
                return body.decode(enc, errors="replace"), enc
            except LookupError:
                continue
        return body.decode("utf-8", errors="replace"), "utf-8"

    def _fetch_live(self, candidate: Candidate) -> Optional[FetchResult]:
        if not self.config.allow_live_fetch:
            return None
        if not self.robots.allowed(candidate.url):
            logger.debug("Live fetch disallowed by robots: %s", candidate.url)
            return None
        headers = {"User-Agent": self.config.user_agent}
        try:
            response = self.session.get(
                candidate.url, headers=headers, timeout=self.timeout
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.debug("Live fetch failed: %s", exc)
            return None
        html, encoding = self._decode_bytes(
            response.content, response.headers.get("Content-Type")
        )
        return FetchResult(
            url=candidate.url,
            fetched_from="live",
            status_code=response.status_code,
            html=html,
            snapshot_url=candidate.url,
            encoding=encoding,
            fetched_at=datetime.utcnow(),
        )

    def fetch(self, candidate: Candidate) -> Optional[FetchResult]:
        # Only live fetch (snapshots/CC removed)
        result = self._fetch_live(candidate)
        if result:
            time.sleep(self.config.pause_seconds)
            return result
        return None
