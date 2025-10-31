import datetime as dt
import html
import logging
import re
import time
import urllib.parse
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Any, Optional

import requests

from scrape.base_scraper import BaseScraper


_TITLE_BLOCK_PATTERN = re.compile(r"<table class='tbl_type01'.*?</table>", re.S)
_ROW_PATTERN = re.compile(r"<tr>.*?</tr>", re.S)
_TITLE_PATTERN = re.compile(
    r"<div class='tit'><a href='([^']+)'[^>]*class='txt'>(.*?)</a>", re.S
)
_DATE_PATTERN = re.compile(r"<span class='date'>([^<]+)</span>")
_REPLY_PATTERN = re.compile(r"<span class='replycnt'>\[(\d+)\]</span>")
_POST_DATETIME_PATTERN = re.compile(
    r"<div class='text3'>.*?<span class='val'>([^<]+)</span>", re.S
)
_COMMENT_BLOCK_PATTERN = re.compile(
    r"<div class=['\"][^\"']*\bother_con\b[^\"']*['\"][^>]*id=['\"]reply_(\d+)['\"][^>]*>"
    r".*?<span class=['\"]name['\"]>(.*?)</span>"
    r".*?<span class=['\"]date['\"]>(.*?)</span>"
    r".*?<span class=['\"]re_txt['\"]>(.*?)</span>",
    re.S,
)


class _TextExtractor(HTMLParser):
    _BLOCK_TAGS = {"p", "div", "li", "ul", "ol", "tr", "td"}

    def __init__(self) -> None:
        super().__init__()
        self._parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        if tag == "br":
            self._parts.append("\n")
        elif tag in self._BLOCK_TAGS:
            self._parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in self._BLOCK_TAGS:
            self._parts.append("\n")

    def handle_startendtag(
        self, tag: str, attrs: list[tuple[str, Optional[str]]]
    ) -> None:
        if tag == "br":
            self._parts.append("\n")

    def handle_data(self, data: str) -> None:
        if data:
            self._parts.append(data)

    def get_text(self) -> str:
        raw = html.unescape("".join(self._parts))
        lines = [line.strip() for line in raw.splitlines()]
        cleaned = "\n".join(line for line in lines if line)
        return cleaned.strip()


def _clean_text(html_snippet: str) -> str:
    parser = _TextExtractor()
    parser.feed(html_snippet)
    parser.close()
    return parser.get_text()


class _ClassTextExtractor(HTMLParser):
    def __init__(self, target_class: str) -> None:
        super().__init__()
        self._target_class = target_class
        self._capturing = False
        self._depth = 0
        self._parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        attr_map = {key: value for key, value in attrs}
        classes = attr_map.get("class", "")
        if (
            not self._capturing
            and isinstance(classes, str)
            and self._target_class in classes.split()
        ):
            self._capturing = True
            self._depth = 1
            return
        if self._capturing:
            if tag == "br":
                self._parts.append("\n")
            elif tag in _TextExtractor._BLOCK_TAGS:
                self._parts.append("\n")
            self._depth += 1

    def handle_endtag(self, tag: str) -> None:
        if not self._capturing:
            return
        if tag in _TextExtractor._BLOCK_TAGS:
            self._parts.append("\n")
        self._depth -= 1
        if self._depth == 0:
            self._capturing = False

    def handle_startendtag(
        self, tag: str, attrs: list[tuple[str, Optional[str]]]
    ) -> None:
        if self._capturing and tag == "br":
            self._parts.append("\n")

    def handle_data(self, data: str) -> None:
        if self._capturing and data:
            self._parts.append(data)

    def get_text(self) -> str:
        raw = html.unescape("".join(self._parts))
        lines = [line.strip() for line in raw.splitlines()]
        return "\n".join(line for line in lines if line).strip()


@dataclass
class _PostMeta:
    document_id: str
    url: str
    title: str
    raw_date: str
    comment_count: int


@dataclass
class _PostDetail:
    content: str
    posted_at: Optional[dt.datetime]
    comments: list[str]


class MlbparkScraper(BaseScraper):
    """Collect MLBPARK posts and comments via HTTP requests."""

    BASE_URL = "https://mlbpark.donga.com/mp/b.php"

    def __init__(
        self,
        board: str = "bullpen",
        max_pages: int = 5,
        request_timeout: float = 10.0,
        request_delay: float = 0.2,
    ) -> None:
        super().__init__()
        self.board = board
        self.max_pages = max_pages
        self.request_timeout = request_timeout
        self.request_delay = request_delay
        self.logger = logging.getLogger(self.__class__.__name__)
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/125.0.0.0 Safari/537.36"
                )
            }
        )

    def _build_request_params(
        self, keyword: str, start_date: dt.date, end_date: dt.date
    ) -> dict[str, Any]:
        return {
            "m": "search",
            "b": self.board,
            "select": "sct",
            "search_select": "sct",
            "query": keyword,
        }

    def _make_request(self, params: dict[str, Any]) -> requests.Response:
        return self._session.get(
            self.BASE_URL, params=params, timeout=self.request_timeout
        )

    def _parse_response(self, response: requests.Response) -> list[_PostMeta]:
        return self._parse_search_html(response.text)

    def scrape(
        self, keyword: str, start_date: dt.date, end_date: dt.date
    ) -> list[dict[str, Any]]:
        collected: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        params = self._build_request_params(keyword, start_date, end_date)
        for page in range(1, self.max_pages + 1):
            params["p"] = page
            self.logger.debug("Fetching page %s for keyword '%s'", page, keyword)
            response = self._make_request(params=params)
            response.raise_for_status()
            metas = self._parse_response(response)
            if not metas:
                break

            stop_pagination = False
            for meta in metas:
                if meta.document_id in seen_ids:
                    continue
                seen_ids.add(meta.document_id)

                listing_dt = self._parse_listing_datetime(meta.raw_date, end_date)
                if listing_dt and listing_dt.date() > end_date:
                    continue
                if listing_dt and listing_dt.date() < start_date:
                    stop_pagination = True
                    continue

                detail = self._fetch_article(meta.url)
                post_dt = detail.posted_at or listing_dt
                if post_dt:
                    post_date = post_dt.date()
                    if post_date > end_date:
                        continue
                    if post_date < start_date:
                        stop_pagination = True
                        continue

                record = {
                    "title": meta.title,
                    "link": meta.url,
                    "search_keyword": keyword,
                    "date": post_dt.isoformat() if post_dt else "",
                    "content": detail.content,
                    "comments": detail.comments,
                    "comment_count": len(detail.comments),
                }
                collected.append(record)
                time.sleep(self.request_delay)

            if stop_pagination:
                break

        return collected

    def _parse_search_html(self, html_text: str) -> list[_PostMeta]:
        table_match = _TITLE_BLOCK_PATTERN.search(html_text)
        if not table_match:
            self.logger.debug("Search table not found in response")
            return []

        metas: list[_PostMeta] = []
        rows = _ROW_PATTERN.findall(table_match.group(0))
        for row_html in rows:
            title_match = _TITLE_PATTERN.search(row_html)
            if not title_match:
                continue

            url = html.unescape(title_match.group(1).strip())
            title = _clean_text(title_match.group(2))
            date_match = _DATE_PATTERN.search(row_html)
            if not date_match:
                continue

            raw_date = html.unescape(date_match.group(1).strip())
            reply_match = _REPLY_PATTERN.search(row_html)
            comment_count = int(reply_match.group(1)) if reply_match else 0
            document_id = self._extract_document_id(url)
            if not document_id:
                continue

            metas.append(
                _PostMeta(
                    document_id=document_id,
                    url=url,
                    title=title,
                    raw_date=raw_date,
                    comment_count=comment_count,
                )
            )
        return metas

    def _parse_listing_datetime(
        self, value: str, reference: dt.date
    ) -> Optional[dt.datetime]:
        value = value.strip()
        if not value:
            return None

        for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y.%m.%d %H:%M"):
            try:
                return dt.datetime.strptime(value, pattern)
            except ValueError:
                continue

        for pattern in ("%Y-%m-%d", "%Y.%m.%d"):
            try:
                date_obj = dt.datetime.strptime(value, pattern)
                return dt.datetime.combine(date_obj.date(), dt.time(0, 0))
            except ValueError:
                continue

        for pattern in ("%H:%M:%S", "%H:%M"):
            try:
                time_obj = dt.datetime.strptime(value, pattern).time()
                return dt.datetime.combine(reference, time_obj)
            except ValueError:
                continue

        return None

    def _fetch_article(self, url: str) -> _PostDetail:
        self.logger.debug("Fetching article %s", url)
        response = self._session.get(url, timeout=self.request_timeout)
        response.raise_for_status()
        html_text = response.text
        content = self._extract_content(html_text)
        posted_at = self._extract_post_datetime(html_text)
        comments = self._extract_comments(html_text)
        return _PostDetail(content=content, posted_at=posted_at, comments=comments)

    def _extract_content(self, html_text: str) -> str:
        start_marker = "<div class='ar_txt' id='contentDetail'>"
        start_idx = html_text.find(start_marker)
        if start_idx == -1:
            return ""
        start_idx += len(start_marker)
        end_marker = "<div class='ar_txt_tool'>"
        end_idx = html_text.find(end_marker, start_idx)
        if end_idx == -1:
            end_marker = "<div class='view_suggest reply_zone"
            end_idx = html_text.find(end_marker, start_idx)
        snippet = html_text[start_idx:end_idx] if end_idx != -1 else html_text[start_idx:]
        return _clean_text(snippet)

    def _extract_post_datetime(self, html_text: str) -> Optional[dt.datetime]:
        match = _POST_DATETIME_PATTERN.search(html_text)
        if not match:
            return None
        value = html.unescape(match.group(1)).strip()
        for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y.%m.%d %H:%M:%S", "%Y.%m.%d %H:%M"):
            try:
                return dt.datetime.strptime(value, pattern)
            except ValueError:
                continue
        for pattern in ("%Y-%m-%d", "%Y.%m.%d"):
            try:
                return dt.datetime.strptime(value, pattern)
            except ValueError:
                continue
        return None

    def _extract_comments(self, html_text: str) -> list[str]:
        comments: list[str] = []
        for match in _COMMENT_BLOCK_PATTERN.finditer(html_text):
            block_html = match.group(0)
            extractor = _ClassTextExtractor("re_txt")
            extractor.feed(block_html)
            extractor.close()
            text = extractor.get_text()
            if text:
                comments.append(text)
        return comments

    def _extract_document_id(self, url: str) -> Optional[str]:
        parsed = urllib.parse.urlparse(url)
        qs = urllib.parse.parse_qs(parsed.query)
        doc_ids = qs.get("id")
        if doc_ids:
            return doc_ids[0]
        return None
