from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from time import sleep
from typing import Any, Dict, List, Mapping, Optional, Tuple, Callable
from urllib.parse import urljoin, urlparse, urlencode, parse_qsl

import requests
from bs4 import BeautifulSoup

from ..models import Candidate
from ..utils import normalize_url
from ..fetch.fetcher import RobotsCache

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ForumSiteConfig:
    enabled: bool = True
    boards: List[str] = field(default_factory=list)  # list of listing URLs
    max_pages: int = 1
    per_board_limit: int = 50
    pause_between_requests: float = 0.5
    obey_robots: bool = True

    def __post_init__(self) -> None:
        self.max_pages = max(1, int(self.max_pages))
        self.per_board_limit = max(1, int(self.per_board_limit))
        self.pause_between_requests = max(0.0, float(self.pause_between_requests))


def _update_query_param(url: str, key: str, value: str) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query[key] = value
    new_query = urlencode(query)
    return parsed._replace(query=new_query).geturl()


class ForumsDiscoverer:
    """Discover forum threads from configured boards across multiple sites.

    This keeps discovery lightweight: we only hit listing pages, obey robots,
    and let the main Fetcher handle per-thread fetch with robots checks too.
    """

    def __init__(
        self,
        session: Any,
        request_timeout: int,
        user_agent: str,
        sites_config: Mapping[str, Any],
    ) -> None:
        self.session = session
        self.timeout = request_timeout
        self.sites_config = sites_config
        self.robots = RobotsCache(session, request_timeout, user_agent)

    def _get_href(self, tag) -> Optional[str]:  # type: ignore[no-untyped-def]
        """Best-effort extract href as str from a tag attribute that can be varied types."""
        try:
            value = tag.get("href")
        except Exception:  # noqa: BLE001
            return None
        if value is None:
            return None
        if isinstance(value, str):
            return value
        # bs4 can produce list-like attribute values; pick first stringy part
        try:
            if isinstance(value, (list, tuple)) and value:
                first = value[0]
                return first if isinstance(first, str) else str(first)
        except Exception:  # noqa: BLE001
            return None
        return str(value)

    def _as_opt_str(self, value: object) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        try:
            if isinstance(value, (list, tuple)) and value:
                first = value[0]
                return first if isinstance(first, str) else str(first)
        except Exception:  # noqa: BLE001
            return None
        try:
            return str(value)
        except Exception:  # noqa: BLE001
            return None

    # -------- Parsers per site ---------
    def _parse_dcinside(
        self, base_url: str, html: str
    ) -> List[Tuple[str, Optional[str], Dict[str, Optional[str]]]]:
        soup = BeautifulSoup(html, "html.parser")
        items: List[Tuple[str, Optional[str], Dict[str, Optional[str]]]] = []
        for a in soup.select("td.gall_tit a[href]"):
            href = self._get_href(a) or ""
            if "/board/view/" in href:
                url = urljoin(base_url, href)
                title = a.get_text(strip=True) or None
                # try to find parent row for meta
                author = None
                published_at = None
                tr = a.find_parent("tr")
                if tr:
                    writer = tr.select_one("td.gall_writer") or tr.select_one(
                        "td.gall_writer ub-writer"
                    )
                    if writer:
                        author = writer.get_text(strip=True) or None
                    tdn = tr.select_one("td.gall_date")
                    if tdn:
                        published_at = self._as_opt_str(
                            tdn.get("title")
                        ) or tdn.get_text(strip=True)
                items.append(
                    (url, title, {"author": author, "published_at": published_at})
                )
        # Fallback heuristic for some skins
        if not items:
            for a in soup.select('a[href*="/board/view/"]'):
                href = self._get_href(a) or ""
                url = urljoin(base_url, href)
                title = a.get_text(strip=True) or None
                items.append((url, title, {"author": None, "published_at": None}))
        return items

    def _parse_bobaedream(
        self, base_url: str, html: str
    ) -> List[Tuple[str, Optional[str], Dict[str, Optional[str]]]]:
        soup = BeautifulSoup(html, "html.parser")
        items: List[Tuple[str, Optional[str], Dict[str, Optional[str]]]] = []
        # Support both legacy /board/bbs_view? and current /view? patterns
        links = soup.select('a[href*="/board/bbs_view?"], a[href*="/view?code="]')
        for a in links:
            href = self._get_href(a) or ""
            url = urljoin(base_url, href)
            title = a.get_text(strip=True) or None
            tr = a.find_parent("tr")
            author = None
            published_at = None
            if tr:
                au = tr.select_one("td.author, td.writer, td.name")
                dt = tr.select_one("td.date, td.regdate, td.time")
                if au:
                    author = au.get_text(strip=True) or None
                if dt:
                    published_at = self._as_opt_str(dt.get("title")) or dt.get_text(
                        strip=True
                    )
            items.append((url, title, {"author": author, "published_at": published_at}))
        return items

    def _parse_mlbpark(
        self, base_url: str, html: str
    ) -> List[Tuple[str, Optional[str], Dict[str, Optional[str]]]]:
        soup = BeautifulSoup(html, "html.parser")
        items: List[Tuple[str, Optional[str], Dict[str, Optional[str]]]] = []
        # MLBPark links can be like /mp/b.php?b=bullpen&m=view&idx=... or sometimes without m=view
        for a in soup.select('a[href*="/mp/b.php"]'):
            href = self._get_href(a) or ""
            if "m=view" not in href and "idx=" not in href:
                continue
            url = urljoin(base_url, href)
            title = a.get_text(strip=True) or None
            tr = a.find_parent("tr")
            author = None
            published_at = None
            if tr:
                au = tr.select_one("td.nikcon, td.author, td.name")
                dt = tr.select_one("td.date, td.time")
                if au:
                    author = au.get_text(strip=True) or None
                if dt:
                    published_at = self._as_opt_str(dt.get("title")) or dt.get_text(
                        strip=True
                    )
            items.append((url, title, {"author": author, "published_at": published_at}))
        return items

    def _parse_theqoo(
        self, base_url: str, html: str
    ) -> List[Tuple[str, Optional[str], Dict[str, Optional[str]]]]:
        soup = BeautifulSoup(html, "html.parser")
        items: List[Tuple[str, Optional[str], Dict[str, Optional[str]]]] = []
        for a in soup.select('a[href*="/square/"]'):
            href = self._get_href(a) or ""
            if re.search(r"/square/\d+", href):
                url = urljoin(base_url, href)
                title = a.get_text(strip=True) or None
                tr = a.find_parent("tr")
                author = None
                published_at = None
                if tr:
                    au = tr.select_one("td.nik, td.author, td.name")
                    dt = tr.select_one("td.time, td.date")
                    if au:
                        author = au.get_text(strip=True) or None
                    if dt:
                        published_at = self._as_opt_str(dt.get("title")) or dt.get_text(
                            strip=True
                        )
                items.append(
                    (url, title, {"author": author, "published_at": published_at})
                )
        return items

    def _parse_ppomppu(
        self, base_url: str, html: str
    ) -> List[Tuple[str, Optional[str], Dict[str, Optional[str]]]]:
        soup = BeautifulSoup(html, "html.parser")
        items: List[Tuple[str, Optional[str], Dict[str, Optional[str]]]] = []
        for a in soup.select('a[href*="/zboard/view.php?id="]'):
            href = self._get_href(a) or ""
            url = urljoin(base_url, href)
            title = a.get_text(strip=True) or None
            tr = a.find_parent("tr")
            author = None
            published_at = None
            if tr:
                au = tr.select_one("td.name, td.author, td.writer")
                dt = tr.select_one("td.date, td.regdate, td.time")
                if au:
                    author = au.get_text(strip=True) or None
                if dt:
                    published_at = self._as_opt_str(dt.get("title")) or dt.get_text(
                        strip=True
                    )
            items.append((url, title, {"author": author, "published_at": published_at}))
        return items

    def _build_page_url(self, site: str, base_url: str, page: int) -> str:
        # Best-effort pagination parameter per site
        if page <= 1:
            return base_url
        key = {
            "dcinside": "page",
            "bobaedream": "page",
            "mlbpark": "p",
            "theqoo": "page",
            "ppomppu": "page",
        }.get(site, "page")
        return _update_query_param(base_url, key, str(page))

    def _get_parser(
        self, site: str
    ) -> Optional[
        Callable[[str, str], List[Tuple[str, Optional[str], Dict[str, Optional[str]]]]]
    ]:
        return {
            "dcinside": self._parse_dcinside,
            "bobaedream": self._parse_bobaedream,
            "mlbpark": self._parse_mlbpark,
            "theqoo": self._parse_theqoo,
            "ppomppu": self._parse_ppomppu,
        }.get(site)

    def discover(self) -> Dict[str, List[Candidate]]:
        per_site: Dict[str, List[Candidate]] = {}
        for site, cfg in self.sites_config.items():
            if not cfg or not getattr(cfg, "enabled", False):
                continue
            parser = self._get_parser(site)
            if not parser:
                logger.debug("No parser for forum site=%s", site)
                continue
            all_candidates: List[Candidate] = []
            obey_robots = bool(getattr(cfg, "obey_robots", True))
            for board_url in cfg.boards:
                if not board_url:
                    continue
                seen_norm: set[str] = set()
                for page in range(1, cfg.max_pages + 1):
                    page_url = self._build_page_url(site, board_url, page)
                    # robots check on listing page (can be overridden per-site)
                    if obey_robots and not self.robots.allowed(page_url):
                        logger.debug("Discovery robots disallow: %s", page_url)
                        continue
                    try:
                        resp = self.session.get(page_url, timeout=self.timeout)
                        if resp.status_code >= 400:
                            logger.debug(
                                "Listing fetch failed %s status=%s",
                                page_url,
                                resp.status_code,
                            )
                            break
                        posts: List[
                            Tuple[str, Optional[str], Dict[str, Optional[str]]]
                        ] = parser(board_url, resp.text)
                    except requests.RequestException as exc:
                        logger.debug(
                            "Listing request error: url=%s error=%s", page_url, exc
                        )
                        break
                    # Normalize and de-dup within board
                    for entry in posts:
                        if isinstance(entry, tuple) and len(entry) == 3:
                            url, title, meta = entry
                        else:
                            continue
                        if not url:
                            continue
                        norm = normalize_url(url)
                        if norm in seen_norm:
                            continue
                        seen_norm.add(norm)
                        # parse timestamp
                        ts: Optional[datetime] = None
                        published_at = (
                            meta.get("published_at") if isinstance(meta, dict) else None
                        )
                        if published_at and isinstance(published_at, str):
                            ts = self._parse_datetime_guess(published_at)
                        all_candidates.append(
                            Candidate(
                                url=url,
                                source=site,
                                discovered_via={
                                    "type": "forum",
                                    "site": site,
                                    "board": board_url,
                                    "page": page,
                                },
                                title=title,
                                snapshot_url=None,
                                timestamp=ts,
                                extra={
                                    "forum": {"site": site, "board": board_url},
                                    # Hint to fetcher to bypass robots if discovery already did
                                    # (only set when site explicitly disabled robots obedience)
                                    **(
                                        {"robots_override": True}
                                        if not obey_robots
                                        else {}
                                    ),
                                },
                            )
                        )
                        if len(all_candidates) >= cfg.per_board_limit:
                            break
                    if len(all_candidates) >= cfg.per_board_limit:
                        break
                    sleep(cfg.pause_between_requests)
            per_site[site] = all_candidates
            logger.info(
                "ForumsDiscoverer site=%s discovered=%d", site, len(all_candidates)
            )
        return per_site

    # ----- helpers -----
    def _parse_datetime_guess(self, s: str) -> Optional[datetime]:
        s = (s or "").strip()
        if not s:
            return None
        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y.%m.%d %H:%M:%S",
            "%Y.%m.%d %H:%M",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%Y-%m-%d",
            "%Y.%m.%d",
            "%Y/%m/%d",
        ):
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                continue
        # strip any non-digit separators and try fallback YYYYMMDDHHMMSS
        digits = re.sub(r"\D", "", s)
        for fmt in ("%Y%m%d%H%M%S", "%Y%m%d%H%M", "%Y%m%d"):
            try:
                return datetime.strptime(digits, fmt)
            except ValueError:
                continue
        return None
