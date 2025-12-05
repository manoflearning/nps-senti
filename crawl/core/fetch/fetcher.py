from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser
from threading import Lock

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
    per_host_pause_sec: dict[str, float] = field(default_factory=dict)


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
            except Exception:
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
        self._host_locks: dict[str, Lock] = {}
        self._last_fetch_at: dict[str, float] = {}

    def _normalize_host(self, url: str) -> Optional[str]:
        try:
            parsed = urlparse(url)
        except Exception:  # noqa: BLE001
            return None
        host = parsed.netloc.lower()
        if not host:
            return None
        if ":" in host:
            host = host.split(":", 1)[0]
        if host.startswith("www."):
            host = host[4:]
        return host or None

    def _host_pause(self, host: Optional[str]) -> Optional[float]:
        if not host:
            return None
        pause_map = self.config.per_host_pause_sec or {}
        if host in pause_map:
            return pause_map[host]
        for key, pause in pause_map.items():
            if host.endswith(f".{key}"):
                return pause
        return None

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
        except Exception as exc:  # noqa: BLE001
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
        host = self._normalize_host(candidate.url)
        host_pause = self._host_pause(host) or 0.0
        pause = max(self.config.pause_seconds, host_pause)

        if host_pause > 0 and host:
            lock = self._host_locks.setdefault(host, Lock())
            with lock:
                last = self._last_fetch_at.get(host, 0.0)
                if last > 0:
                    elapsed = time.monotonic() - last
                    wait = pause - elapsed
                    if wait > 0:
                        time.sleep(wait)
                result = self._fetch_live(candidate)
                self._last_fetch_at[host] = time.monotonic()
                return result

        result = self._fetch_live(candidate)
        if result and pause > 0:
            time.sleep(pause)
        return result
