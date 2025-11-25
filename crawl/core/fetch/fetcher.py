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
        # Use a conservative browser-like default UA to avoid degraded responses
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/128.0.0.0 Safari/537.36",
    )
    pause_seconds: float = 0.5
    allow_live_fetch: bool = True
    obey_robots: bool = False


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
        self.robots: RobotsCache | None = None
        if self.config.obey_robots:
            self.robots = RobotsCache(session, timeout, self.config.user_agent)

    def _decode_bytes(
        self,
        body: bytes,
        content_type: Optional[str],
        apparent: Optional[str] = None,
    ) -> tuple[str, Optional[str]]:
        # 1) charset from HTTP header
        header_enc: Optional[str] = None
        if content_type:
            lower = content_type.lower()
            if "charset=" in lower:
                header_enc = lower.split("charset=")[-1].split(";")[0].strip()

        # 2) charset from HTML <meta> (scan small prefix of bytes; safe ASCII search)
        meta_enc: Optional[str] = None
        head = body[:4096]
        try:
            import re as _re  # local alias

            m = _re.search(
                rb"charset\s*=\s*([A-Za-z0-9_\-]+)", head, flags=_re.IGNORECASE
            )
            if m:
                meta_enc = m.group(1).decode("ascii", errors="ignore").lower()
        except Exception:  # noqa: BLE001
            meta_enc = None

        # 3) Heuristic order: header -> apparent -> meta -> utf-8 -> cp949 -> euc-kr -> latin-1
        candidates = [
            header_enc,
            apparent.lower() if apparent else None,
            meta_enc,
            "utf-8",
            "cp949",
            "euc-kr",
            "latin-1",
        ]

        # Try strict decode first to avoid silent mojibake, then fallback with replace
        for enc in candidates:
            if not enc:
                continue
            try:
                return body.decode(enc, errors="strict"), enc
            except Exception:
                continue
        # Last-resort: utf-8 with replacement; should rarely be used
        return body.decode("utf-8", errors="replace"), "utf-8"

    def _fetch_live(self, candidate: Candidate) -> Optional[FetchResult]:
        if not self.config.allow_live_fetch:
            return None
        obey_robots = self.config.obey_robots
        # Allow per-candidate override to bypass robots (opt-in via config)
        override = False
        if obey_robots:
            try:
                override = bool((candidate.extra or {}).get("robots_override", False))
            except Exception:  # noqa: BLE001
                override = False
        if (
            obey_robots
            and not override
            and self.robots is not None
            and not self.robots.allowed(candidate.url)
        ):
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
            response.content,
            response.headers.get("Content-Type"),
            getattr(response, "apparent_encoding", None),
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
