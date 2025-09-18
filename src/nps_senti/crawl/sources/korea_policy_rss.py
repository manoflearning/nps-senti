from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Iterator
from urllib.parse import parse_qs, urlparse
import urllib.request

from ..models import RawItem
from .base import BaseSource

LOGGER = logging.getLogger(__name__)

_FEED_URL = "https://www.korea.kr/rss/policy.xml"
_USER_AGENT = "nps-senti-crawler/0.1"


@dataclass(frozen=True, slots=True)
class _FeedEntry:
    title: str
    link: str
    published_at: datetime


class KoreaPolicyRSSSource(BaseSource):
    """Pull items from korea.kr policy RSS and fetch detail pages.

    This is a pragmatic, publicly reachable source to validate the crawl stage
    end-to-end. It is not NPS-specific, but the pipeline remains source-agnostic
    and stores a `source` field for downstream filtering.
    """

    def __init__(self, *, max_items: int | None = None) -> None:
        self._max_items = max_items

    @property
    def source_id(self) -> str:
        return "korea_policy_rss"

    def iter_items(self, seen_ids: set[str]) -> Iterator[RawItem]:
        entries = self._load_feed()
        count = 0
        for e in entries:
            item_id = self._extract_item_id(e.link)
            if item_id in seen_ids:
                continue
            try:
                detail_html = self._fetch(e.link)
            except Exception as exc:  # pragma: no cover - network errors
                LOGGER.warning("failed to fetch rss detail %s: %s", e.link, exc)
                continue
            content, attachments = _extract_text_and_attachments(detail_html)
            yield RawItem(
                source=self.source_id,
                item_id=item_id,
                url=e.link,
                title=e.title.strip(),
                content=content,
                published_at=e.published_at,
                attachments=attachments,
                raw_html=detail_html,
            )
            count += 1
            if self._max_items is not None and count >= self._max_items:
                break

    # --- Helpers ---------------------------------------------------------

    def _load_feed(self) -> list[_FeedEntry]:
        xml_text = self._fetch(_FEED_URL)
        root = ET.fromstring(xml_text)
        channel = root.find("channel")
        if channel is None:
            return []
        out: list[_FeedEntry] = []
        for item in channel.findall("item"):
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            pub = (item.findtext("pubDate") or "").strip()
            if not title or not link:
                continue
            try:
                published_at = _parse_rfc822(pub)
            except Exception:
                published_at = datetime.now(timezone.utc)
            out.append(_FeedEntry(title=title, link=link, published_at=published_at))
        return out

    def _fetch(self, url: str) -> str:
        req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
        with urllib.request.urlopen(req) as resp:  # type: ignore[call-arg]
            data = resp.read()
        return data.decode("utf-8", errors="ignore")

    def _extract_item_id(self, url: str) -> str:
        # Prefer explicit newsId query param; fallback to whole URL.
        parsed = urlparse(url)
        q = parse_qs(parsed.query)
        nid = q.get("newsId")
        if nid and nid[0]:
            return nid[0]
        return url


def _parse_rfc822(text: str) -> datetime:
    # Example: Thu, 18 Sep 2025 11:37:03 GMT
    try:
        dt = datetime.strptime(text, "%a, %d %b %Y %H:%M:%S %Z")
    except ValueError:
        # Fallback: remove GMT or zone and parse naive.
        cleaned = re.sub(r" [A-Z]+$", "", text)
        dt = datetime.strptime(cleaned, "%a, %d %b %Y %H:%M:%S")
    return dt.replace(tzinfo=timezone.utc)


class _BodyTextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._text: list[str] = []
        self._attachments: list[str] = []

    def handle_starttag(self, tag: str, attrs):
        attr = dict(attrs)
        if tag == "br":
            self._text.append("\n")
        if tag == "a":
            href = attr.get("href")
            if href and href.lower().endswith((".pdf", ".hwp", ".docx")):
                self._attachments.append(href)

    def handle_endtag(self, tag: str):
        if tag in {"p", "li"}:
            self._text.append("\n")

    def handle_data(self, data: str):
        s = data.strip()
        if s:
            self._text.append(s + " ")

    @property
    def text(self) -> str:
        # Normalize whitespace and collapse multiple blank lines
        raw = "".join(self._text)
        lines = [ln.strip() for ln in raw.splitlines()]
        lines = [ln for ln in lines if ln]
        return "\n".join(lines)

    @property
    def attachments(self) -> list[str]:
        return list(dict.fromkeys(self._attachments))


def _extract_text_and_attachments(html: str) -> tuple[str, list[str]]:
    parser = _BodyTextParser()
    parser.feed(html)
    text = parser.text
    atts = parser.attachments
    return text, atts
