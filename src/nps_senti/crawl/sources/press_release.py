from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Iterator
from urllib.parse import parse_qs, urljoin, urlparse
import urllib.request

from ..models import RawItem
from .base import BaseSource

LOGGER = logging.getLogger(__name__)

_BASE_URL = "https://www.nps.or.kr"
_LIST_PATH = "/jsppage/news/pressrelease/list.jsp"
_DETAIL_PATH = "/jsppage/news/pressrelease/view.jsp"
_USER_AGENT = "nps-senti-crawler/0.1"


@dataclass(frozen=True, slots=True)
class ListingEntry:
    item_id: str
    title: str
    url: str
    published_at: datetime


@dataclass(frozen=True, slots=True)
class DetailPage:
    content: str
    attachments: list[str]
    raw_html: str


class PressReleaseSource(BaseSource):
    @property
    def source_id(self) -> str:
        return "nps_press_release"

    def iter_items(self, seen_ids: set[str]) -> Iterator[RawItem]:
        seen = set(seen_ids)
        page = 1
        while True:
            try:
                html = self.fetch_listing(page)
            except Exception as exc:  # pragma: no cover - network errors
                LOGGER.warning("failed to fetch listing page %s: %s", page, exc)
                break

            entries = self.parse_listing(html)
            if not entries:
                break

            page_contains_only_seen = True
            for entry in entries:
                if entry.item_id in seen:
                    continue

                page_contains_only_seen = False

                try:
                    detail_html = self.fetch_detail(entry.url)
                except Exception as exc:  # pragma: no cover - network errors
                    LOGGER.warning(
                        "failed to fetch detail %s (%s): %s",
                        entry.item_id,
                        entry.url,
                        exc,
                    )
                    continue

                detail = self.parse_detail(detail_html)
                seen.add(entry.item_id)
                yield RawItem(
                    source=self.source_id,
                    item_id=entry.item_id,
                    url=entry.url,
                    title=entry.title,
                    content=detail.content,
                    published_at=entry.published_at,
                    attachments=detail.attachments,
                    raw_html=detail.raw_html,
                )

            if page_contains_only_seen:
                break

            page += 1

    # --- Parsing helpers -------------------------------------------------

    def parse_listing(self, html: str) -> list[ListingEntry]:
        parser = _ListingParser()
        parser.feed(html)
        return parser.entries

    def parse_detail(self, html: str) -> DetailPage:
        parser = _DetailParser()
        parser.feed(html)
        content = parser.content.strip()
        return DetailPage(
            content="\n".join(line.strip() for line in content.splitlines() if line.strip()),
            attachments=parser.attachments,
            raw_html=html,
        )

    # --- Network helpers -------------------------------------------------

    def fetch_listing(self, page: int) -> str:
        url = f"{_BASE_URL}{_LIST_PATH}?page={page}"
        return self._fetch_text(url)

    def fetch_detail(self, url: str) -> str:
        return self._fetch_text(url)

    def _fetch_text(self, url: str) -> str:
        request = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
        with urllib.request.urlopen(request) as response:  # type: ignore[call-arg]
            data = response.read()
        return data.decode("utf-8", errors="ignore")


class _ListingParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.entries: list[ListingEntry] = []
        self._in_title_td = False
        self._in_date_td = False
        self._current_href: str | None = None
        self._title_parts: list[str] = []
        self._date_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs):
        attr = dict(attrs)
        classes = attr.get("class", "")
        if tag == "td" and "title" in classes.split():
            self._in_title_td = True
            self._title_parts = []
        elif tag == "td" and "date" in classes.split():
            self._in_date_td = True
            self._date_parts = []

        if self._in_title_td and tag == "a":
            href = attr.get("href")
            if href:
                self._current_href = urljoin(_BASE_URL, href)

    def handle_endtag(self, tag: str):
        if tag == "td":
            if self._in_title_td:
                self._in_title_td = False
            elif self._in_date_td:
                self._in_date_td = False
        elif tag == "tr":
            self._finalize_row()

    def handle_data(self, data: str):
        if self._in_title_td:
            self._title_parts.append(data)
        elif self._in_date_td:
            self._date_parts.append(data)

    def _finalize_row(self) -> None:
        if not self._current_href:
            self._reset_row()
            return

        title = "".join(self._title_parts).strip()
        date_str = "".join(self._date_parts).strip()

        if not title or not date_str:
            self._reset_row()
            return

        published_at = _parse_date(date_str)
        item_id = _extract_item_id(self._current_href)
        if not item_id:
            LOGGER.debug("skip row without item_id: %s", self._current_href)
            self._reset_row()
            return

        self.entries.append(
            ListingEntry(
                item_id=item_id,
                title=title,
                url=self._current_href,
                published_at=published_at,
            )
        )
        self._reset_row()

    def _reset_row(self) -> None:
        self._in_title_td = False
        self._in_date_td = False
        self._current_href = None
        self._title_parts = []
        self._date_parts = []


class _DetailParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.attachments: list[str] = []
        self._content_depth = 0
        self._content_parts: list[str] = []

    @property
    def content(self) -> str:
        return "".join(self._content_parts)

    def handle_starttag(self, tag: str, attrs):
        attr = dict(attrs)
        classes = attr.get("class", "").split()

        if tag == "div" and "content" in classes:
            self._content_depth += 1
        elif self._content_depth > 0 and tag == "div":
            self._content_depth += 1

        if self._content_depth > 0 and tag in {"p", "br"}:
            if tag == "br":
                self._content_parts.append("\n")

        if tag == "a":
            href = attr.get("href")
            if not href:
                return
            href_abs = urljoin(_BASE_URL, href)
            if "attachment" in classes or href.lower().endswith((".pdf", ".hwp", ".docx")):
                if href_abs not in self.attachments:
                    self.attachments.append(href_abs)

    def handle_endtag(self, tag: str):
        if self._content_depth > 0 and tag == "div":
            self._content_depth -= 1
            if self._content_depth < 0:
                self._content_depth = 0
        elif self._content_depth > 0 and tag == "p":
            self._content_parts.append("\n")

    def handle_data(self, data: str):
        if self._content_depth > 0:
            self._content_parts.append(data)


def _parse_date(text: str) -> datetime:
    cleaned = text.strip()
    if not cleaned:
        raise ValueError("empty date")
    cleaned = cleaned.replace(".", "-")
    try:
        dt = datetime.strptime(cleaned, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"unsupported date format: {text!r}") from exc
    return dt.replace(tzinfo=timezone.utc)


def _extract_item_id(url: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    for key in ("seq", "articleSeq", "no"):
        value = query.get(key)
        if value:
            return value[0]
    tail = parsed.path.rstrip("/").split("/")[-1]
    if tail:
        return tail
    return ""
