from __future__ import annotations

import gzip
import io
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests
from warcio.archiveiterator import ArchiveIterator

from ..models import Candidate, FetchResult

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class FetcherConfig:
    user_agent: str = "nps-senti-crawler/0.1 (+https://github.com/example)"
    pause_seconds: float = 0.5
    allow_live_fetch: bool = True


class RobotsCache:
    def __init__(self, session: requests.Session, timeout: int, user_agent: str) -> None:
        self.session = session
        self.timeout = timeout
        self.user_agent = user_agent
        self.cache: dict[str, RobotFileParser] = {}

    def allowed(self, url: str) -> bool:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return False
        base = f"{parsed.scheme}://{parsed.netloc}"
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
                    parser.parse([])
                    parser.allow_all = True
                else:
                    parser.parse(response.text.splitlines())
            except requests.RequestException:
                parser.parse([])
                parser.allow_all = True
            self.cache[base] = parser
        return parser.can_fetch(self.user_agent, url)


class InternetArchiveClient:
    API_URL = "https://archive.org/wayback/available"

    def __init__(self, session: requests.Session, timeout: int) -> None:
        self.session = session
        self.timeout = timeout

    def resolve_snapshot(self, url: str, timestamp: Optional[datetime]) -> Optional[tuple[str, str]]:
        params = {"url": url}
        if timestamp:
            params["timestamp"] = timestamp.strftime("%Y%m%d%H%M%S")
        try:
            response = self.session.get(self.API_URL, params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
        except (requests.RequestException, ValueError) as exc:
            logger.debug("Internet Archive lookup failed: url=%s error=%s", url, exc)
            return None
        snapshot = data.get("archived_snapshots", {}).get("closest")
        if not snapshot or not snapshot.get("available"):
            return None
        return snapshot.get("url"), snapshot.get("timestamp")


class Fetcher:
    CC_BASE_URL = "https://data.commoncrawl.org/"

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
        self.ia_client = InternetArchiveClient(session, timeout)

    def _decode_bytes(self, body: bytes, content_type: Optional[str]) -> tuple[str, Optional[str]]:
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

    def _fetch_commoncrawl(self, candidate: Candidate) -> Optional[FetchResult]:
        info = candidate.extra.get("commoncrawl") if candidate.extra else None
        if not info:
            return None
        filename = info.get("filename")
        offset = info.get("offset")
        length = info.get("length")
        if filename is None or offset is None or length is None:
            return None
        range_end = offset + length - 1
        url = f"{self.CC_BASE_URL}{filename}"
        headers = {
            "Range": f"bytes={offset}-{range_end}",
            "User-Agent": self.config.user_agent,
        }
        try:
            response = self.session.get(url, headers=headers, timeout=self.timeout)
            if response.status_code not in (200, 206):
                logger.debug("CC fetch status=%s url=%s", response.status_code, url)
                return None
        except requests.RequestException as exc:
            logger.debug("CC fetch request failed: %s", exc)
            return None

        stream = io.BytesIO(response.content)
        try:
            for record in ArchiveIterator(stream):
                if record.rec_type != "response":
                    continue
                payload = record.content_stream().read()
                if record.http_headers and record.http_headers.get_header("Content-Encoding") == "gzip":
                    payload = gzip.decompress(payload)
                content_type = None
                if record.http_headers:
                    content_type = record.http_headers.get_header("Content-Type")
                html, encoding = self._decode_bytes(payload, content_type)
                snapshot_url = (
                    f"{url}?offset={offset}&length={length}"
                )
                return FetchResult(
                    url=candidate.url,
                    fetched_from="commoncrawl",
                    status_code=200,
                    html=html,
                    snapshot_url=snapshot_url,
                    encoding=encoding,
                    fetched_at=datetime.utcnow(),
                )
        except Exception as exc:  # noqa: BLE001
            logger.debug("Failed to parse CC record: %s", exc)
            return None
        return None

    def _fetch_snapshot(self, snapshot_url: str, candidate_url: str) -> Optional[FetchResult]:
        headers = {"User-Agent": self.config.user_agent}
        try:
            response = self.session.get(snapshot_url, headers=headers, timeout=self.timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.debug("Snapshot fetch failed: %s", exc)
            return None
        html, encoding = self._decode_bytes(response.content, response.headers.get("Content-Type"))
        return FetchResult(
            url=candidate_url,
            fetched_from="snapshot",
            status_code=response.status_code,
            html=html,
            snapshot_url=snapshot_url,
            encoding=encoding,
            fetched_at=datetime.utcnow(),
        )

    def _fetch_live(self, candidate: Candidate) -> Optional[FetchResult]:
        if not self.config.allow_live_fetch:
            return None
        if not self.robots.allowed(candidate.url):
            logger.debug("Live fetch disallowed by robots: %s", candidate.url)
            return None
        headers = {"User-Agent": self.config.user_agent}
        try:
            response = self.session.get(candidate.url, headers=headers, timeout=self.timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.debug("Live fetch failed: %s", exc)
            return None
        html, encoding = self._decode_bytes(response.content, response.headers.get("Content-Type"))
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
        # Common Crawl record
        result = self._fetch_commoncrawl(candidate)
        if result:
            time.sleep(self.config.pause_seconds)
            return result

        # Provided snapshot or fallback to Internet Archive
        snapshots_to_try: list[str] = []
        if candidate.snapshot_url:
            snapshots_to_try.append(candidate.snapshot_url)
        snapshot = self.ia_client.resolve_snapshot(candidate.url, candidate.timestamp)
        if snapshot:
            snapshots_to_try.append(snapshot[0])
        for snap_url in snapshots_to_try:
            result = self._fetch_snapshot(snap_url, candidate.url)
            if result:
                time.sleep(self.config.pause_seconds)
                return result

        # Live fetch
        result = self._fetch_live(candidate)
        if result:
            time.sleep(self.config.pause_seconds)
            return result
        return None
