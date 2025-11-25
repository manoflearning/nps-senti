from __future__ import annotations

import json
import logging
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple, cast

from langdetect import DetectorFactory, LangDetectException, detect
import trafilatura
import os
import re
import requests
from urllib.parse import parse_qs, urlparse, unquote

from ..models import Candidate, Document, FetchResult
from ..config import QualityConfig
from ..utils import normalize_url, sha1_hex

logger = logging.getLogger(__name__)

DetectorFactory.seed = 0


@dataclass(slots=True)
class ExtractionResult:
    text: str
    title: Optional[str]
    authors: List[str]
    published_at: Optional[str]


class Extractor:
    def __init__(
        self,
        keywords: Iterable[str],
        allowed_languages: Iterable[str],
        quality_config: QualityConfig,
    ) -> None:
        self.keywords = [kw for kw in keywords if kw.strip()]
        self.keywords_lower = [kw.lower() for kw in self.keywords]
        self.allowed_languages = [lang.lower() for lang in allowed_languages]
        self.quality_config = quality_config
        self.youtube_api_key = os.environ.get("YOUTUBE_API_KEY")

        # YouTube comments fetching knobs (env-based to avoid config churn)
        # - YOUTUBE_COMMENTS_PAGES: how many pages to fetch (default: 5)
        # - YOUTUBE_COMMENTS_INCLUDE_REPLIES: include replies (true/false; default: true)
        # - YOUTUBE_COMMENTS_ORDER: relevance|time (default: relevance)
        # - YOUTUBE_COMMENTS_TEXT_FORMAT: html|plainText (default: html)
        def _env_bool(name: str, default: bool = False) -> bool:
            v = os.environ.get(name)
            if v is None:
                return default
            v = v.strip().lower()
            return v in {"1", "true", "yes", "y", "on"}

        def _env_int(name: str, default: int) -> int:
            try:
                return int(os.environ.get(name, str(default)))
            except ValueError:
                return default

        self.youtube_comments_pages = max(0, _env_int("YOUTUBE_COMMENTS_PAGES", 5))
        self.youtube_comments_include_replies = _env_bool(
            "YOUTUBE_COMMENTS_INCLUDE_REPLIES", True
        )
        order = (os.environ.get("YOUTUBE_COMMENTS_ORDER") or "relevance").lower()
        self.youtube_comments_order = (
            order if order in {"relevance", "time"} else "relevance"
        )
        fmt = (os.environ.get("YOUTUBE_COMMENTS_TEXT_FORMAT") or "html").strip()
        self.youtube_comments_text_format = (
            fmt if fmt in {"html", "plainText"} else "html"
        )

        # Forums comments fetching knobs
        # - FORUMS_COMMENTS_ENABLED: enable/disable forums comment collection (default: true)
        # - FORUMS_COMMENTS_MAX: limit number of comments appended to text/meta (default: 200)
        self.forums_comments_enabled = _env_bool("FORUMS_COMMENTS_ENABLED", True)
        self.forums_comments_max = max(0, _env_int("FORUMS_COMMENTS_MAX", 200))

        # Fast mode: skip expensive comment fetches to improve throughput
        fast_mode = _env_bool("FAST_CRAWL", False)
        if fast_mode:
            self.youtube_comments_pages = 0
            self.forums_comments_enabled = False

        # Optional cookies for sites that gate comment APIs
        self.theqoo_cookies = os.environ.get("THEQOO_COOKIES")
        self.ppomppu_cookies = os.environ.get("PPOMPPU_COOKIES")
        # Optional login credentials for auto session refresh
        self.theqoo_id = os.environ.get("THEQOO_ID")
        self.theqoo_pw = os.environ.get("THEQOO_PW")
        self.ppomppu_id = os.environ.get("PPOMPPU_ID")
        self.ppomppu_pw = os.environ.get("PPOMPPU_PW")

    def _fallback_title_from_html(self, html: str) -> Optional[str]:
        if not html:
            return None
        try:
            from bs4 import BeautifulSoup  # lazy import

            soup = BeautifulSoup(html, "html.parser")
            # Prefer OpenGraph title
            og = soup.select_one('meta[property="og:title"]')
            if og and og.get("content"):
                return str(og.get("content")).strip() or None
            # Common meta alternatives
            meta_title = soup.select_one('meta[name="title"]')
            if meta_title and meta_title.get("content"):
                return str(meta_title.get("content")).strip() or None
            # Fallback to <title>
            if soup.title and soup.title.string:
                return soup.title.string.strip() or None
        except Exception:  # noqa: BLE001
            return None
        return None

    def _run_trafilatura(self, html: str, url: str) -> Optional[ExtractionResult]:
        try:
            extraction_json = trafilatura.extract(html, url=url, output_format="json")
        except Exception as exc:  # noqa: BLE001
            logger.debug("Trafilatura extraction failed: %s", exc)
            extraction_json = None
        if extraction_json:
            try:
                data = json.loads(extraction_json)
            except json.JSONDecodeError:
                data = {}
            text = data.get("text") or ""
            title = data.get("title")
            authors = []
            if author := data.get("author"):
                if isinstance(author, str):
                    authors = [author]
                elif isinstance(author, list):
                    authors = [str(a) for a in author]
            published_at = data.get("date")
            return ExtractionResult(
                text=text.strip(),
                title=title.strip() if isinstance(title, str) else None,
                authors=authors,
                published_at=published_at,
            )
        # fallback plain text
        try:
            # Default behavior returns plain text when output_format is omitted
            text_plain = trafilatura.extract(html, url=url)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Trafilatura plain extraction failed: %s", exc)
            return None
        if not text_plain:
            return None
        return ExtractionResult(
            text=text_plain.strip(),
            title=None,
            authors=[],
            published_at=None,
        )

    def _detect_lang(self, text: str) -> str:
        if not text:
            return "und"
        try:
            return detect(text)
        except LangDetectException:
            return "und"

    def _build_quality(self, text: str, lang: str) -> Dict[str, object]:
        score = 0.0
        reasons: List[str] = []
        length = len(text)

        if lang.lower() in self.allowed_languages:
            score += 0.3
        else:
            reasons.append(f"lang={lang}")

        text_lower = text.lower()
        keyword_hits = sum(1 for kw in self.keywords_lower if kw and kw in text_lower)
        coverage = (
            keyword_hits / len(self.keywords_lower) if self.keywords_lower else 0.0
        )
        if keyword_hits >= self.quality_config.min_keyword_hits:
            score += 0.2
        else:
            reasons.append("keyword_hits")

        return {
            "score": round(score, 3),
            "reasons": reasons,
            "keyword_coverage": round(coverage, 3),
            "length": length,
            "keyword_hits": keyword_hits,
        }

    def _parse_datetime_loose(self, s: str) -> Optional[datetime]:
        if not s:
            return None
        cleaned = re.sub(r"\([^)]*\)", " ", s)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        iso_try = cleaned.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(iso_try)
        except Exception:
            pass
        patterns = [
            (
                r"(?P<y4>\d{4})[./-](?P<m>\d{1,2})[./-](?P<d>\d{1,2})"
                r"(?:\s+(?P<h>\d{1,2}):(?P<min>\d{2})(?::(?P<s>\d{2}))?)?"
            ),
            (
                r"(?P<y2>\d{2})[./-](?P<m>\d{1,2})[./-](?P<d>\d{1,2})"
                r"(?:\s+(?P<h>\d{1,2}):(?P<min>\d{2})(?::(?P<s>\d{2}))?)?"
            ),
            (
                r"(?P<y4t>\d{4})(?P<mt>\d{2})(?P<dt>\d{2})T(?P<ht>\d{2})(?P<mint>\d{2})(?P<st>\d{2})Z?"
            ),
        ]
        for pattern in patterns:
            match = re.search(pattern, cleaned)
            if not match:
                continue
            groups = match.groupdict()
            y_str = groups.get("y4") or groups.get("y2") or groups.get("y4t")
            if not y_str:
                continue
            year = int(y_str)
            if groups.get("y2"):
                year = year + 2000 if year < 70 else year + 1900
            try:
                month = int(groups.get("m") or groups.get("mt") or 0)
                day = int(groups.get("d") or groups.get("dt") or 0)
                hour = int(groups.get("h") or groups.get("ht") or 0)
                minute = int(groups.get("min") or groups.get("mint") or 0)
                second = int(groups.get("s") or groups.get("st") or 0)
                return datetime(year, month, day, hour, minute, second)
            except ValueError:
                continue
        return None

    def _iter_datetimes_from_text(self, text: str) -> List[Tuple[datetime, bool]]:
        if not text:
            return []
        cleaned = re.sub(r"\s+", " ", text)
        results: List[Tuple[datetime, bool]] = []
        seen: set[Tuple[str, bool]] = set()
        patterns = [
            (
                r"(?P<y4>\d{4})[./-](?P<m>\d{1,2})[./-](?P<d>\d{1,2})"
                r"(?:\s+(?P<h>\d{1,2}):(?P<min>\d{2})(?::(?P<s>\d{2}))?)?"
            ),
            (
                r"(?P<y2>\d{2})[./-](?P<m>\d{1,2})[./-](?P<d>\d{1,2})"
                r"(?:\s+(?P<h>\d{1,2}):(?P<min>\d{2})(?::(?P<s>\d{2}))?)?"
            ),
        ]
        for pattern in patterns:
            for match in re.finditer(pattern, cleaned):
                groups = match.groupdict()
                y_str = groups.get("y4") or groups.get("y2")
                if not y_str:
                    continue
                year = int(y_str)
                if groups.get("y2"):
                    year = year + 2000 if year < 70 else year + 1900
                try:
                    month = int(groups.get("m") or 0)
                    day = int(groups.get("d") or 0)
                    hour = int(groups.get("h") or 0)
                    minute = int(groups.get("min") or 0)
                    second = int(groups.get("s") or 0)
                    dt = datetime(year, month, day, hour, minute, second)
                except ValueError:
                    continue
                has_time = bool(groups.get("h"))
                key = (dt.isoformat(), has_time)
                if key in seen:
                    continue
                seen.add(key)
                results.append((dt, has_time))
        return results

    def _normalize_published_at(self, value: Optional[str]) -> Optional[str]:
        if not value or not isinstance(value, str):
            return None
        dt = self._parse_datetime_loose(value)
        return dt.isoformat() if dt else None

    def _infer_forum_published_at(
        self,
        candidate: Candidate,
        extraction: ExtractionResult,
        fetch_result: FetchResult,
    ) -> Optional[str]:
        if not (
            isinstance(candidate.discovered_via, dict)
            and candidate.discovered_via.get("type") == "forum"
        ):
            return None

        dt_candidates: List[Tuple[datetime, bool]] = []

        site = (candidate.source or "").lower()

        if site == "dcinside" and fetch_result.html:
            try:
                from bs4 import BeautifulSoup  # type: ignore

                soup = BeautifulSoup(fetch_result.html, "html.parser")
                selector_hits: List[Tuple[datetime, bool]] = []
                for el in soup.select(
                    "span.gall_date, td.gall_date, div.gall_date, span.date, span.write_time"
                ):
                    raw_attr = el.get("title")
                    raw_text = el.get_text(" ", strip=True)
                    raw = raw_attr if isinstance(raw_attr, str) else raw_text
                    if not raw:
                        continue
                    dt = self._parse_datetime_loose(raw)
                    if not dt:
                        continue
                    selector_hits.append((dt, ":" in raw))
                if selector_hits:
                    # Prefer the first explicit metadata timestamp for dcinside
                    return selector_hits[0][0].isoformat()
                dt_candidates.extend(selector_hits)
            except Exception:  # noqa: BLE001
                pass

        for payload in (extraction.text, fetch_result.html):
            if not payload:
                continue
            dt_candidates.extend(self._iter_datetimes_from_text(payload))

        forum_meta = (
            candidate.extra.get("forum") if isinstance(candidate.extra, dict) else None
        )
        if isinstance(forum_meta, dict):
            comments = forum_meta.get("comments")
            if isinstance(comments, list):
                for comment in comments:
                    if not isinstance(comment, dict):
                        continue
                    ts = comment.get("publishedAt") or comment.get("published_at")
                    if not ts:
                        continue
                    dt = self._parse_datetime_loose(str(ts))
                    if dt:
                        has_time = ":" in str(ts)
                        dt_candidates.append((dt, has_time))

        if not dt_candidates:
            return None

        dt_with_time = [dt for dt, has_time in dt_candidates if has_time]
        if dt_with_time:
            chosen = max(dt_with_time)
        else:
            chosen = max(dt for dt, _ in dt_candidates)
        return chosen.isoformat()

    def build_document(
        self,
        candidate: Candidate,
        fetch_result: FetchResult,
        run_id: str,
    ) -> tuple[Optional[Document], Optional[Dict[str, object]]]:
        extraction = self._run_trafilatura(fetch_result.html or "", candidate.url)
        if not extraction or not extraction.text:
            # Try YouTube augmentation even if HTML extraction failed
            if candidate.source == "youtube":
                extraction = ExtractionResult(
                    text="", title=candidate.title, authors=[], published_at=None
                )
            # For forum threads, allow building from comments-only content
            elif isinstance(candidate.discovered_via, dict) and (
                candidate.discovered_via.get("type") == "forum"
            ):
                extraction = ExtractionResult(
                    text="",
                    title=self._fallback_title_from_html(fetch_result.html or "")
                    or candidate.title,
                    authors=[],
                    published_at=None,
                )
            else:
                return None, {"status": "extract-failed"}

        # YouTube comment/description augmentation
        if candidate.source == "youtube":
            try:
                extraction = self._augment_youtube(candidate, extraction)
            except Exception as exc:  # noqa: BLE001
                logger.debug("YouTube augmentation failed: %s", exc)

        # Forums comment augmentation
        try:
            if self.forums_comments_enabled and isinstance(
                candidate.discovered_via, dict
            ):
                if candidate.discovered_via.get("type") == "forum":
                    extraction = self._augment_forum(
                        candidate, extraction, fetch_result
                    )
        except Exception as exc:  # noqa: BLE001
            logger.debug("Forum augmentation failed: %s", exc)

        lang = self._detect_lang(extraction.text)
        quality = self._build_quality(extraction.text, lang)
        if cast(int, quality["keyword_hits"]) < self.quality_config.min_keyword_hits:
            return None, {
                "status": "quality-reject",
                "quality": quality,
                "reason": "keyword_hits",
            }

        url_norm = normalize_url(candidate.url)
        # Exact-duplicate guard: ID derives only from normalized URL
        doc_id = sha1_hex(url_norm)

        published_at = self._normalize_published_at(extraction.published_at)
        if not published_at:
            published_at = self._infer_forum_published_at(
                candidate, extraction, fetch_result
            )
        if not published_at and candidate.timestamp:
            published_at = candidate.timestamp.isoformat()

        # Prefer extractor's title; if missing, try deriving from HTML head
        derived_title = extraction.title or self._fallback_title_from_html(
            fetch_result.html or ""
        )

        document = Document(
            id=doc_id,
            source=candidate.source,
            url=candidate.url,
            snapshot_url=fetch_result.snapshot_url,
            title=derived_title or candidate.title,
            text=extraction.text,
            lang=lang,
            published_at=published_at,
            authors=extraction.authors,
            discovered_via=candidate.discovered_via,
            quality=quality,
            dup={},
            crawl={
                "run_id": run_id,
                "fetched_at": fetch_result.fetched_at.isoformat() + "Z",
                "fetched_from": fetch_result.fetched_from,
            },
            extra=candidate.extra or {},
        )

        return document, quality

    # ----------------- YouTube helpers -----------------
    def _youtube_strip_html(self, s: str) -> str:
        if not s:
            return ""
        return re.sub(r"<[^>]+>", " ", s).strip()

    def _augment_youtube(
        self, candidate: Candidate, extraction: ExtractionResult
    ) -> ExtractionResult:
        # Build text from snippet description + top comments
        video_details = (candidate.extra or {}).get("youtube", {})
        snippet = (
            video_details.get("snippet", {}) if isinstance(video_details, dict) else {}
        )
        statistics = (
            video_details.get("statistics", {})
            if isinstance(video_details, dict)
            else {}
        )
        title = extraction.title or snippet.get("title") or candidate.title
        description = snippet.get("description") or ""

        comments_texts: List[str] = []
        comments_meta: List[dict] = []
        video_id = None
        if isinstance(video_details, dict):
            video_id = video_details.get("id")
        if not video_id and candidate.url:
            # Try to parse from URL
            from urllib.parse import urlparse, parse_qs

            parsed = urlparse(candidate.url)
            qs = parse_qs(parsed.query)
            if "v" in qs and qs["v"]:
                video_id = qs["v"][0]

        # Fetch comments (best-effort, optional pagination/replies)
        if self.youtube_api_key and video_id and self.youtube_comments_pages > 0:
            try:
                url = "https://www.googleapis.com/youtube/v3/commentThreads"
                params = {
                    "key": self.youtube_api_key,
                    "part": (
                        "snippet,replies"
                        if self.youtube_comments_include_replies
                        else "snippet"
                    ),
                    "videoId": video_id,
                    "maxResults": "100",
                    "order": self.youtube_comments_order,
                    "textFormat": self.youtube_comments_text_format,
                }
                page_token: Optional[str] = None
                pages = 0
                while True:
                    if page_token:
                        params["pageToken"] = page_token
                    resp = requests.get(url, params=params, timeout=20)
                    if resp.status_code >= 400:
                        break
                    try:
                        data = resp.json()
                    except ValueError:
                        logger.debug(
                            "YouTube comments response not JSON for video=%s", video_id
                        )
                        break
                    for item in data.get("items", []):
                        top = (
                            item.get("snippet", {})
                            .get("topLevelComment", {})
                            .get("snippet", {})
                        )
                        if self.youtube_comments_text_format == "html":
                            text = top.get("textDisplay") or ""
                            text_clean = self._youtube_strip_html(text)
                        else:
                            text_clean = (
                                top.get("textOriginal") or top.get("textDisplay") or ""
                            ).strip()
                        if text_clean:
                            comments_texts.append(text_clean)
                            comments_meta.append(
                                {
                                    "author": top.get("authorDisplayName"),
                                    "likeCount": top.get("likeCount"),
                                    "publishedAt": top.get("publishedAt"),
                                }
                            )
                        if self.youtube_comments_include_replies:
                            replies = (item.get("replies") or {}).get(
                                "comments", []
                            ) or []
                            for r in replies:
                                rs = r.get("snippet", {})
                                if self.youtube_comments_text_format == "html":
                                    r_text = rs.get("textDisplay") or ""
                                    r_clean = self._youtube_strip_html(r_text)
                                else:
                                    r_clean = (
                                        rs.get("textOriginal")
                                        or rs.get("textDisplay")
                                        or ""
                                    ).strip()
                                if r_clean:
                                    comments_texts.append(r_clean)
                                    comments_meta.append(
                                        {
                                            "author": rs.get("authorDisplayName"),
                                            "likeCount": rs.get("likeCount"),
                                            "publishedAt": rs.get("publishedAt"),
                                        }
                                    )
                    pages += 1
                    if pages >= self.youtube_comments_pages:
                        break
                    page_token = data.get("nextPageToken")
                    if not page_token:
                        break
            except requests.RequestException:
                pass

        combined_parts = []
        if title:
            combined_parts.append(title)
        if description:
            combined_parts.append(description)
        if extraction.text:
            combined_parts.append(extraction.text)
        if comments_texts:
            combined_parts.append("\n".join(comments_texts))
        text_combined = "\n\n".join([p for p in combined_parts if p and p.strip()])

        # Patch candidate.extra with comments meta if available
        if isinstance(candidate.extra, dict):
            yt = candidate.extra.setdefault("youtube", {})
            if isinstance(yt, dict):
                yt.setdefault("statistics", statistics)
                if comments_meta:
                    yt["comments"] = comments_meta

        return ExtractionResult(
            text=text_combined or extraction.text,
            title=title,
            authors=extraction.authors,
            published_at=extraction.published_at,
        )

    # ----------------- Forums helpers -----------------
    def _clean_ws(self, s: str) -> str:
        return re.sub(r"\s+", " ", (s or "").strip())

    def _extract_text(self, root, candidates: list[str]) -> str:  # type: ignore[no-untyped-def]
        for sel in candidates:
            el = root.select_one(sel)
            if el:
                return self._clean_ws(el.get_text(" ", strip=True))
        return self._clean_ws(root.get_text(" ", strip=True))

    def _extract_attr_or_text(
        self, root, sel: str, attr: str = "datetime"
    ) -> Optional[str]:  # type: ignore[no-untyped-def]
        el = root.select_one(sel)
        if not el:
            return None
        val = el.get(attr)
        if isinstance(val, str) and val.strip():
            return val.strip()
        txt = el.get_text(" ", strip=True)
        return txt.strip() if txt else None

    def _extract_comments_generic(self, soup) -> List[dict]:  # type: ignore[no-untyped-def]
        items: List[dict] = []
        containers = [
            "ul.cmt_list li",
            "div.cmt_list li",
            "div.comment_list li",
            "div.comments li",
            "#comment li",
            "#Comment li",
            "#cmt li",
            "div#comment .comment",
            "div#cmt .comment",
            "li.comment",
            "div.comment",
            "div.reply",
            "li.reply",
            "div.reple",
            "li.reple",
            "table#cmttbl tr",
        ]
        seen_texts: set[str] = set()
        for sel in containers:
            for node in soup.select(sel):
                text = self._extract_text(
                    node,
                    [
                        ".cmt_txt",
                        ".comment_txt",
                        ".comment-text",
                        ".comment-content",
                        ".txt",
                        ".text",
                        "p",
                    ],
                )
                if not text or len(text) < 2:
                    continue
                # Skip boilerplate
                if text in {"신고", "삭제", "추천", "비공개"}:
                    continue
                if text in seen_texts:
                    continue
                seen_texts.add(text)
                author = self._extract_text(
                    node,
                    [
                        ".nickname",
                        ".nick",
                        ".name",
                        ".writer",
                        ".author",
                        ".user",
                        ".member",
                        ".ub-writer",
                    ],
                )
                if author:
                    # Some forums include extra labels inside author
                    author = self._clean_ws(re.sub(r"\b(익명|관리자)\b", "", author))
                ts = (
                    self._extract_attr_or_text(node, "time[datetime]")
                    or self._extract_attr_or_text(node, "time", "datetime")
                    or self._extract_attr_or_text(node, ".date", "title")
                    or self._extract_attr_or_text(node, ".date", "data-time")
                    or self._extract_attr_or_text(node, ".date", "data-datetime")
                    or self._extract_attr_or_text(node, ".date")
                    or self._extract_attr_or_text(node, ".time")
                )
                items.append(
                    {"author": author or None, "text": text, "publishedAt": ts}
                )
                if 0 < self.forums_comments_max <= len(items):
                    return items
        return items

    def _fetch_comments_dcinside(
        self,
        candidate: Candidate,
        soup,
    ) -> List[dict]:  # type: ignore[no-untyped-def]
        e_token = soup.select_one("#e_s_n_o")
        if not e_token or not e_token.get("value"):
            return []

        parsed = urlparse(candidate.url or "")
        query = parse_qs(parsed.query)
        gall_id = (query.get("id") or [None])[0]
        article_no = (query.get("no") or [None])[0]

        if not gall_id:
            forum_meta = (
                candidate.extra.get("forum")
                if isinstance(candidate.extra, dict)
                else {}
            )
            board_url = None
            if isinstance(forum_meta, dict):
                board_url = forum_meta.get("board")
            if board_url:
                board_query = parse_qs(urlparse(str(board_url)).query)
                gall_id = (board_query.get("id") or [None])[0]

        if not gall_id or not article_no:
            return []

        board_type = ""
        board_type_el = soup.select_one("#board_type")
        if board_type_el and board_type_el.get("value"):
            board_type = board_type_el.get("value")

        gall_type = ""
        gall_type_el = soup.select_one("#_GALLTYPE_")
        if gall_type_el and gall_type_el.get("value"):
            gall_type = gall_type_el.get("value")

        secret_key = ""
        secret_key_el = soup.select_one("#secret_article_key")
        if secret_key_el and secret_key_el.get("value"):
            secret_key = secret_key_el.get("value")

        data = {
            "id": gall_id,
            "no": article_no,
            "cmt_id": gall_id,
            "cmt_no": article_no,
            "focus_cno": "",
            "focus_pno": "",
            "e_s_n_o": e_token.get("value"),
            "comment_page": "1",
            "sort": "D",
            "prevCnt": "",
            "board_type": board_type,
            "_GALLTYPE_": gall_type,
            "secret_article_key": secret_key,
        }

        user_agent = os.environ.get(
            "CRAWLER_USER_AGENT",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/128.0.0.0 Safari/537.36",
        )

        session = requests.Session()
        try:
            session.get(candidate.url, headers={"User-Agent": user_agent}, timeout=20)
            resp = session.post(
                "https://gall.dcinside.com/board/comment/",
                headers={
                    "User-Agent": user_agent,
                    "Referer": candidate.url,
                    "X-Requested-With": "XMLHttpRequest",
                },
                data=data,
                timeout=20,
            )
            resp.raise_for_status()
            payload = resp.json()
        except (requests.RequestException, ValueError):
            return []

        rows = payload.get("comments")
        if not isinstance(rows, list):
            return []

        try:
            from bs4 import BeautifulSoup  # type: ignore
        except Exception:  # noqa: BLE001
            BeautifulSoup = None  # type: ignore

        results: List[dict] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            memo_html = row.get("memo") or ""
            if BeautifulSoup:
                memo_text = BeautifulSoup(memo_html, "html.parser").get_text(
                    " ", strip=True
                )
            else:
                memo_text = re.sub(r"<[^>]+>", " ", memo_html)
            memo_text = self._clean_ws(memo_text)
            if not memo_text:
                continue
            author = row.get("name") or None
            ip = row.get("ip") or None
            if author and ip:
                author_display = f"{author} ({ip})"
            elif ip and not author:
                author_display = ip
            else:
                author_display = author
            results.append(
                {
                    "author": author_display,
                    "text": memo_text,
                    "publishedAt": row.get("reg_date"),
                    "id": row.get("no"),
                    "replyTo": row.get("c_no") or None,
                    "depth": row.get("depth"),
                }
            )
            if 0 < self.forums_comments_max <= len(results):
                break
        return results

    def _fetch_comments_bobaedream(
        self,
        candidate: Candidate,
        soup,
    ) -> List[dict]:  # type: ignore[no-untyped-def]
        parsed = urlparse(candidate.url or "")
        query = parse_qs(parsed.query)
        board_code = query.get("code") or query.get("board")
        board_code = board_code[0] if board_code else None
        article_no = query.get("No") or query.get("no")
        article_no = article_no[0] if article_no else None
        if not board_code or not article_no:
            return []

        page_html = soup.decode() if hasattr(soup, "decode") else ""

        tb_match = re.search(r"tb=([A-Za-z0-9_]+)", page_html)
        wid_match = re.search(r"wid=([^&\"\\]+)", page_html)
        if not tb_match or not wid_match:
            return []
        tb_value = tb_match.group(1)
        wid_value = unquote(wid_match.group(1))

        user_agent = os.environ.get(
            "CRAWLER_USER_AGENT",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/128.0.0.0 Safari/537.36",
        )

        session = requests.Session()
        try:
            session.get(candidate.url, headers={"User-Agent": user_agent}, timeout=20)
            params = {
                "tb": tb_value,
                "code": board_code,
                "No": article_no,
                "page": "1",
                "strLimit": "100",
                "strOrder": "",
                "strMywrite": "",
                "focus": "top",
                "wid": wid_value,
            }
            resp = session.get(
                "https://www.bobaedream.co.kr/board_renew/bulletin/comment_list.php",
                params=params,
                headers={"User-Agent": user_agent, "Referer": candidate.url},
                timeout=20,
            )
            resp.raise_for_status()
            from bs4 import BeautifulSoup  # type: ignore

            comment_soup = BeautifulSoup(resp.text, "html.parser")
        except (requests.RequestException, ValueError):
            return []
        except Exception:  # noqa: BLE001
            return []

        results: List[dict] = []
        seen_ids: set[str] = set()
        for text_node in comment_soup.select("dd[id^=small_cmt_]"):
            cid_value = text_node.get("id")
            if not cid_value:
                continue
            cid = str(cid_value)
            numeric_id = cid.split("_")[-1]
            if numeric_id in seen_ids:
                continue
            seen_ids.add(numeric_id)
            dl = text_node.find_parent("dl")
            if not dl:
                continue
            dt = dl.find("dt")
            author = None
            published = None
            if dt:
                name = dt.select_one("span.author")
                author = (
                    self._clean_ws(name.get_text(" ", strip=True)) if name else None
                )
                date_span = dt.select_one("span.date")
                if date_span and date_span.get_text(strip=True):
                    published = date_span.get_text(strip=True)
            text = self._clean_ws(text_node.get_text(" ", strip=True))
            if not text:
                continue
            results.append(
                {
                    "author": author,
                    "text": text,
                    "publishedAt": published,
                    "id": numeric_id,
                    "depth": 0,
                }
            )
            if 0 < self.forums_comments_max <= len(results):
                break

        return results

    def _fetch_comments_mlbpark(
        self,
        candidate: Candidate,
        soup,
    ) -> List[dict]:  # type: ignore[no-untyped-def]
        parsed = urlparse(candidate.url or "")
        query = parse_qs(parsed.query)
        board = query.get("b") or query.get("board")
        board = board[0] if board else None
        article_id = query.get("id") or query.get("no")
        article_id = article_id[0] if article_id else None
        if not board or not article_id:
            return []

        user_agent = os.environ.get(
            "CRAWLER_USER_AGENT",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/128.0.0.0 Safari/537.36",
        )

        session = requests.Session()
        try:
            session.get(candidate.url, headers={"User-Agent": user_agent}, timeout=20)
            resp = session.get(
                "https://mlbpark.donga.com/mp/b.php",
                params={"b": board, "id": article_id, "m": "reply"},
                headers={"User-Agent": user_agent, "Referer": candidate.url},
                timeout=20,
            )
            resp.raise_for_status()
            from bs4 import BeautifulSoup  # type: ignore

            comment_soup = BeautifulSoup(resp.text, "html.parser")
        except (requests.RequestException, ValueError):
            return []
        except Exception:  # noqa: BLE001
            return []

        results: List[dict] = []
        for block in comment_soup.select("div.other_con"):
            cid_value = block.get("id")
            if not cid_value:
                continue
            cid = str(cid_value)
            text_span = block.select_one("span.re_txt")
            if not text_span:
                continue
            text = self._clean_ws(text_span.get_text(" ", strip=True))
            if not text:
                continue
            name_span = block.select_one(".txt .name")
            author = (
                self._clean_ws(name_span.get_text(" ", strip=True))
                if name_span
                else None
            )
            date_span = block.select_one(".txt .date")
            published = (
                date_span.get_text(strip=True)
                if date_span and date_span.get_text(strip=True)
                else None
            )
            ip_span = block.select_one(".txt .ip")
            ip_val = (
                self._clean_ws(ip_span.get_text(" ", strip=True)) if ip_span else None
            )
            if author and ip_val:
                author_display = f"{author} {ip_val}".strip()
            else:
                author_display = author or ip_val
            results.append(
                {
                    "author": author_display,
                    "text": text,
                    "publishedAt": published,
                    "id": cid.replace("reply_", ""),
                    "depth": 0,
                }
            )
            if 0 < self.forums_comments_max <= len(results):
                break

        return results

    def _fetch_comments_theqoo(
        self,
        candidate: Candidate,
        soup,
    ) -> List[dict]:  # type: ignore[no-untyped-def]
        # Parse mid and document id from URL path: /{mid}/{document_srl}
        try:
            parsed = urlparse(candidate.url or "")
            parts = [p for p in parsed.path.split("/") if p]
            mid = parts[-2] if len(parts) >= 2 else None
            doc_id = parts[-1] if parts else None
        except Exception:  # noqa: BLE001
            mid = None
            doc_id = None
        if not mid or not doc_id:
            return []

        # Hit the public HTML partial for comments
        user_agent = os.environ.get(
            "CRAWLER_USER_AGENT",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/128.0.0.0 Safari/537.36",
        )
        session = requests.Session()
        try:
            base_headers = {"User-Agent": user_agent}
            if self.theqoo_cookies:
                base_headers["Cookie"] = self.theqoo_cookies
            session.get(candidate.url, headers=base_headers, timeout=20)
            resp = session.get(
                "https://theqoo.net/index.php",
                params={
                    "module": "board",
                    "act": "dispBoardContentCommentList",
                    "mid": mid,
                    "document_srl": doc_id,
                },
                headers={
                    **base_headers,
                    "Referer": candidate.url,
                    "X-Requested-With": "XMLHttpRequest",
                },
                timeout=20,
            )
            if resp.status_code >= 400:
                # Try login once if credentials are available
                if self._maybe_login_theqoo(session):
                    resp = session.get(
                        "https://theqoo.net/index.php",
                        params={
                            "module": "board",
                            "act": "dispBoardContentCommentList",
                            "mid": mid,
                            "document_srl": doc_id,
                        },
                        headers={
                            **base_headers,
                            "Referer": candidate.url,
                            "X-Requested-With": "XMLHttpRequest",
                        },
                        timeout=20,
                    )
                    if resp.status_code >= 400:
                        return []
                else:
                    return []
            from bs4 import BeautifulSoup  # type: ignore

            c_soup = BeautifulSoup(resp.text, "html.parser")
        except Exception:  # noqa: BLE001
            return []

        selectors = [
            "#cmtPosition li.fdb_itm",
            "ul.bd_lst_cmt li",
            "ul.reply li",
            "div.bd_cmt li",
            "article.xe_comment",
            "li.fdb_itm",
        ]
        nodes = []
        for sel in selectors:
            nodes = c_soup.select(sel)
            if nodes:
                break
        if not nodes:
            # If no nodes found, try logging in once and retry
            if self._maybe_login_theqoo(session):
                try:
                    resp = session.get(
                        "https://theqoo.net/index.php",
                        params={
                            "module": "board",
                            "act": "dispBoardContentCommentList",
                            "mid": mid,
                            "document_srl": doc_id,
                        },
                        headers={
                            "User-Agent": user_agent,
                            "Referer": candidate.url,
                            "X-Requested-With": "XMLHttpRequest",
                        },
                        timeout=20,
                    )
                    if resp.status_code < 400:
                        from bs4 import BeautifulSoup  # type: ignore

                        c_soup = BeautifulSoup(resp.text, "html.parser")
                        for sel in selectors:
                            nodes = c_soup.select(sel)
                            if nodes:
                                break
                except Exception:  # noqa: BLE001
                    nodes = []
        if not nodes:
            return []

        results: List[dict] = []
        for node in nodes:
            text = self._extract_text(
                node,
                [
                    ".xe_content",
                    ".xe_comment",
                    ".bd_cmt",
                    ".fdb_cont",
                    ".comment-content",
                    "p",
                ],
            )
            if not text:
                continue
            author = self._extract_text(
                node, [".author", ".nick", ".name", ".writer", "strong.name", "a.nick"]
            )
            ts = self._extract_attr_or_text(
                node, "time[datetime]"
            ) or self._extract_attr_or_text(node, ".date")
            results.append({"author": author or None, "text": text, "publishedAt": ts})
            if 0 < self.forums_comments_max <= len(results):
                break
        return results

    def _maybe_login_theqoo(self, session: requests.Session) -> bool:
        if not (self.theqoo_id and self.theqoo_pw):
            return False
        try:
            ua = os.environ.get(
                "CRAWLER_USER_AGENT",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/128.0.0.0 Safari/537.36",
            )
            # Fetch home to obtain CSRF token
            home = session.get(
                "https://theqoo.net/",
                headers={"User-Agent": ua},
                timeout=20,
            )
            token = None
            try:
                from bs4 import BeautifulSoup  # type: ignore

                hs = BeautifulSoup(home.text, "html.parser")
                meta = hs.find("meta", attrs={"name": "csrf-token"})
                if meta and meta.get("content"):
                    token = meta.get("content")
            except Exception:  # noqa: BLE001
                token = None

            headers = {
                "User-Agent": ua,
                "Referer": "https://theqoo.net/",
                "X-Requested-With": "XMLHttpRequest",
            }
            if token:
                headers["X-CSRF-Token"] = str(token)  # Rhymix token

            params = {"module": "member", "act": "procMemberLogin"}
            data = {
                "user_id": self.theqoo_id,
                "password": self.theqoo_pw,
                "keep_signed": "Y",
            }
            resp = session.post(
                "https://theqoo.net/index.php",
                params=params,
                data=data,
                headers=headers,
                timeout=20,
            )
            if resp.status_code >= 400:
                return False
            # Heuristic: presence of login status cookies implies success
            ck = session.cookies.get_dict()
            if "rx_login_status" in ck or "xe_logged" in ck:
                return True
        except requests.RequestException:
            return False
        return False

    def _fetch_comments_ppomppu(
        self,
        candidate: Candidate,
        soup,
    ) -> List[dict]:  # type: ignore[no-untyped-def]
        items: List[dict] = []
        containers = [
            "#comment tr",
            "#Comment tr",
            ".comList tr",
            "table#comment_table tr",
            "div.comment tr",
            "div#divComment tr",
        ]
        for sel in containers:
            for node in soup.select(sel):
                text = self._extract_text(
                    node, [".comContent", ".comment", ".txt", "p", "td"]
                )
                if not text:
                    continue
                author = self._extract_text(
                    node, [".writer", ".nick", ".name", "td.user", ".author"]
                )
                ts = (
                    self._extract_attr_or_text(node, "time[datetime]")
                    or self._extract_attr_or_text(node, ".date")
                    or self._extract_attr_or_text(node, ".regdate")
                )
                items.append(
                    {"author": author or None, "text": text, "publishedAt": ts}
                )
                if 0 < self.forums_comments_max <= len(items):
                    return items
            if items:
                return items

        # Attempt partial endpoints used by some skins
        try:
            parsed = urlparse(candidate.url or "")
            query = parse_qs(parsed.query)
            board = (query.get("id") or [None])[0]
            no = (query.get("no") or query.get("No") or [None])[0]
            if not board or not no:
                return []
            user_agent = os.environ.get(
                "CRAWLER_USER_AGENT",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/128.0.0.0 Safari/537.36",
            )
            session = requests.Session()
            base_headers = {"User-Agent": user_agent}
            if self.ppomppu_cookies:
                base_headers["Cookie"] = self.ppomppu_cookies
            session.get(candidate.url, headers=base_headers, timeout=20)
            from bs4 import BeautifulSoup  # type: ignore

            # 1) comment.php (primary)
            def _try_comment_php() -> List[dict]:
                out: List[dict] = []
                try:
                    resp = session.get(
                        "https://www.ppomppu.co.kr/zboard/comment.php",
                        params={
                            "id": board,
                            "no": no,
                            "c_page": "1",
                            "comment_mode": "sort_desc",
                        },
                        headers={
                            **base_headers,
                            "Referer": candidate.url,
                            "X-Requested-With": "XMLHttpRequest",
                        },
                        timeout=20,
                    )
                    if resp.status_code >= 400 or not resp.text:
                        return out
                    c_soup = BeautifulSoup(resp.text, "html.parser")
                    # Each comment line
                    for ln in c_soup.select("div.comment_line, div.comment_line2"):
                        # Identify comment id
                        cid = None
                        try:
                            parent_div = ln.find_parent(
                                "div",
                                id=lambda v: isinstance(v, str)
                                and v.startswith("comment_"),
                            )
                            if parent_div and parent_div.get("id"):
                                _idv = parent_div.get("id")
                                try:
                                    _idv_str = (
                                        _idv[0]
                                        if isinstance(_idv, (list, tuple))
                                        else str(_idv)
                                    )
                                except Exception:  # noqa: BLE001
                                    _idv_str = None
                                if _idv_str:
                                    cid = _idv_str.replace("comment_", "").strip()
                        except Exception:  # noqa: BLE001
                            cid = None
                        # Extract text
                        text = ""
                        if cid:
                            tgt = c_soup.select_one(f"#commentContent_{cid}")
                            if tgt:
                                text = self._clean_ws(tgt.get_text(" ", strip=True))
                        if not text:
                            text = self._extract_text(
                                ln, [".mid-text-area", ".comment", ".txt", "p", "div"]
                            )
                        if not text:
                            continue
                        # Author
                        author = self._extract_text(
                            ln,
                            [
                                ".comment_template_depth1_vote b a",
                                "b a",
                                ".name a",
                                ".writer",
                            ],
                        )
                        # Timestamp (best-effort: pick first HH:MM:SS in line)
                        ts = None
                        try:
                            import re as _re

                            m = _re.search(
                                r"\b\d{2}:\d{2}:\d{2}\b", ln.get_text(" ", strip=True)
                            )
                            if m:
                                ts = m.group(0)
                        except Exception:  # noqa: BLE001
                            ts = None
                        depth = 1 if "comment_line2" in (ln.get("class") or []) else 0
                        out.append(
                            {
                                "author": author or None,
                                "text": text,
                                "publishedAt": ts,
                                **({"id": cid} if cid else {}),
                                "depth": depth,
                            }
                        )
                        if 0 < self.forums_comments_max <= len(out):
                            return out
                except requests.RequestException:
                    return out
                return out

            items.extend(_try_comment_php())
            if items:
                return items

            for url, params in [
                (
                    "https://www.ppomppu.co.kr/zboard/_comment_list.php",
                    {"id": board, "no": no, "page": "1"},
                ),
                (
                    "https://www.ppomppu.co.kr/zboard/bbs_comment.php",
                    {"id": board, "no": no, "page": "1"},
                ),
            ]:
                try:
                    resp = session.get(
                        url,
                        params=params,
                        headers={
                            **base_headers,
                            "Referer": candidate.url,
                            "X-Requested-With": "XMLHttpRequest",
                        },
                        timeout=15,
                    )
                    if resp.status_code >= 400 or not resp.text:
                        continue
                    c_soup = BeautifulSoup(resp.text, "html.parser")
                    for node in c_soup.select(".comList tr, tr"):
                        text = self._extract_text(
                            node, [".comContent", ".comment", ".txt", "p", "td"]
                        )
                        if not text:
                            continue
                        author = self._extract_text(
                            node,
                            [".writer", ".nick", ".name", "td.user", ".author"],
                        )
                        ts = (
                            self._extract_attr_or_text(node, "time[datetime]")
                            or self._extract_attr_or_text(node, ".date")
                            or self._extract_attr_or_text(node, ".regdate")
                        )
                        items.append(
                            {
                                "author": author or None,
                                "text": text,
                                "publishedAt": ts,
                            }
                        )
                        if 0 < self.forums_comments_max <= len(items):
                            return items
                    if items:
                        return items
                except requests.RequestException:
                    continue
            # If still empty, try logging in once and retry endpoints
            if not items and self._maybe_login_ppomppu(session, candidate.url):
                # Try comment.php again after login
                items.extend(_try_comment_php())
                if items:
                    return items
                for url, params in [
                    (
                        "https://www.ppomppu.co.kr/zboard/_comment_list.php",
                        {"id": board, "no": no, "page": "1"},
                    ),
                    (
                        "https://www.ppomppu.co.kr/zboard/bbs_comment.php",
                        {"id": board, "no": no, "page": "1"},
                    ),
                ]:
                    try:
                        resp = session.get(
                            url,
                            params=params,
                            headers={
                                **base_headers,
                                "Referer": candidate.url,
                                "X-Requested-With": "XMLHttpRequest",
                            },
                            timeout=15,
                        )
                        if resp.status_code >= 400 or not resp.text:
                            continue
                        c_soup = BeautifulSoup(resp.text, "html.parser")
                        for node in c_soup.select(".comList tr, tr"):
                            text = self._extract_text(
                                node, [".comContent", ".comment", ".txt", "p", "td"]
                            )
                            if not text:
                                continue
                            author = self._extract_text(
                                node,
                                [".writer", ".nick", ".name", "td.user", ".author"],
                            )
                            ts = (
                                self._extract_attr_or_text(node, "time[datetime]")
                                or self._extract_attr_or_text(node, ".date")
                                or self._extract_attr_or_text(node, ".regdate")
                            )
                            items.append(
                                {
                                    "author": author or None,
                                    "text": text,
                                    "publishedAt": ts,
                                }
                            )
                            if 0 < self.forums_comments_max <= len(items):
                                return items
                        if items:
                            return items
                    except requests.RequestException:
                        continue
        except Exception:  # noqa: BLE001
            return items

        return items

    def _maybe_login_ppomppu(self, session: requests.Session, referer_url: str) -> bool:
        if not (self.ppomppu_id and self.ppomppu_pw):
            return False
        try:
            ua = os.environ.get(
                "CRAWLER_USER_AGENT",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/128.0.0.0 Safari/537.36",
            )
            resp = session.get(
                "https://www.ppomppu.co.kr/zboard/login.php",
                headers={"User-Agent": ua, "Referer": referer_url},
                timeout=20,
            )
            s_url = referer_url or "/"
            try:
                from bs4 import BeautifulSoup  # type: ignore

                lp = BeautifulSoup(resp.text, "html.parser")
                hidden = lp.find("input", attrs={"name": "s_url"})
                if hidden and hidden.get("value"):
                    s_url = hidden.get("value")
            except Exception:  # noqa: BLE001
                pass
            data = {
                "user_id": self.ppomppu_id,
                "password": self.ppomppu_pw,
                "s_url": s_url,
            }
            resp2 = session.post(
                "https://www.ppomppu.co.kr/zboard/login_check.php",
                data=data,
                headers={"User-Agent": ua, "Referer": referer_url},
                timeout=20,
                allow_redirects=True,
            )
            if resp2.status_code >= 400:
                return False
            # Heuristic: any cookie set/change implies success; real validation occurs on subsequent request
            return True if session.cookies.get_dict() else False
        except requests.RequestException:
            return False

    def _augment_forum(
        self,
        candidate: Candidate,
        extraction: ExtractionResult,
        fetch_result: FetchResult,
    ) -> ExtractionResult:
        html = fetch_result.html or ""
        if not html:
            return extraction
        try:
            from bs4 import BeautifulSoup  # lazy import
        except Exception:  # noqa: BLE001
            return extraction

        soup = BeautifulSoup(html, "html.parser")

        site = (candidate.source or "").lower()
        comments: List[dict] = []
        try:
            if site == "dcinside":
                comments = self._fetch_comments_dcinside(candidate, soup)
            elif site == "bobaedream":
                comments = self._fetch_comments_bobaedream(candidate, soup)
            elif site == "mlbpark":
                comments = self._fetch_comments_mlbpark(candidate, soup)
            elif site == "theqoo":
                comments = self._fetch_comments_theqoo(candidate, soup)
            elif site == "ppomppu":
                comments = self._fetch_comments_ppomppu(candidate, soup)
        except Exception:  # noqa: BLE001
            comments = []

        if not comments and site != "dcinside":
            comments = self._extract_comments_generic(soup)

        if not comments:
            return extraction

        # Cap to max allowed
        if self.forums_comments_max and len(comments) > self.forums_comments_max:
            comments = comments[: self.forums_comments_max]

        # Combine into text
        combined_parts: List[str] = []
        if extraction.text:
            combined_parts.append(extraction.text)
        comments_blob = "\n".join(
            [c.get("text", "") for c in comments if c.get("text")]
        )
        if comments_blob.strip():
            combined_parts.append(comments_blob)
        text_combined = "\n\n".join([p for p in combined_parts if p and p.strip()])

        # Patch candidate.extra["forum"]["comments"]
        if isinstance(candidate.extra, dict):
            forum_meta = candidate.extra.setdefault("forum", {})
            if isinstance(forum_meta, dict):
                forum_meta["comments"] = comments

        return ExtractionResult(
            text=text_combined or extraction.text,
            title=extraction.title,
            authors=extraction.authors,
            published_at=extraction.published_at,
        )
