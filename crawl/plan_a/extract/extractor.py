from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

from langdetect import DetectorFactory, LangDetectException, detect
import trafilatura
import os
import re
import requests

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
            text_plain = trafilatura.extract(html, url=url, output_format=None)
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

        if length >= self.quality_config.min_characters:
            score += 0.4
        else:
            reasons.append("len<threshold")

        if lang.lower() in self.allowed_languages:
            score += 0.3
        else:
            reasons.append(f"lang={lang}")

        text_lower = text.lower()
        keyword_hits = sum(1 for kw in self.keywords_lower if kw and kw in text_lower)
        coverage = keyword_hits / len(self.keywords_lower) if self.keywords_lower else 0.0
        if keyword_hits >= self.quality_config.min_keyword_hits:
            score += 0.2
        else:
            reasons.append("keyword_hits")
        if coverage < self.quality_config.min_keyword_coverage:
            reasons.append("coverage")

        if score < self.quality_config.min_score:
            reasons.append("low-score")

        return {
            "score": round(score, 3),
            "reasons": reasons,
            "keyword_coverage": round(coverage, 3),
            "length": length,
            "keyword_hits": keyword_hits,
        }

    def build_document(
        self,
        candidate: Candidate,
        fetch_result: FetchResult,
        run_id: str,
        plan: str,
    ) -> tuple[Optional[Document], Optional[Dict[str, object]]]:
        extraction = self._run_trafilatura(fetch_result.html or "", candidate.url)
        if not extraction or not extraction.text:
            # Try YouTube augmentation even if HTML extraction failed
            if candidate.source == "youtube":
                extraction = ExtractionResult(text="", title=candidate.title, authors=[], published_at=None)
            else:
                return None, {"status": "extract-failed"}

        # YouTube comment/description augmentation
        if candidate.source == "youtube":
            try:
                extraction = self._augment_youtube(candidate, extraction)
            except Exception as exc:  # noqa: BLE001
                logger.debug("YouTube augmentation failed: %s", exc)

        lang = self._detect_lang(extraction.text)
        quality = self._build_quality(extraction.text, lang)
        if quality["keyword_hits"] < self.quality_config.min_keyword_hits:
            return None, {"status": "quality-reject", "quality": quality, "reason": "keyword_hits"}
        if quality["keyword_coverage"] < self.quality_config.min_keyword_coverage:
            return None, {"status": "quality-reject", "quality": quality, "reason": "coverage"}
        if quality["length"] < self.quality_config.min_characters:
            return None, {"status": "quality-reject", "quality": quality, "reason": "length"}
        if quality["score"] < self.quality_config.min_score:
            return None, {"status": "quality-reject", "quality": quality, "reason": "score"}

        url_norm = normalize_url(candidate.url)
        hash_source = url_norm + "::" + sha1_hex(extraction.text[:1000])
        doc_id = sha1_hex(hash_source)

        published_at = extraction.published_at
        if published_at and isinstance(published_at, str) and len(published_at) < 10:
            published_at = None
        if not published_at and candidate.timestamp:
            published_at = candidate.timestamp.isoformat()

        document = Document(
            id=doc_id,
            source=candidate.source,
            url=candidate.url,
            snapshot_url=fetch_result.snapshot_url,
            title=extraction.title or candidate.title,
            text=extraction.text,
            lang=lang,
            published_at=published_at,
            authors=extraction.authors,
            discovered_via=candidate.discovered_via,
            quality=quality,
            dup={"simhash": None, "group": None},
            crawl={
                "plan": plan,
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

    def _augment_youtube(self, candidate: Candidate, extraction: ExtractionResult) -> ExtractionResult:
        # Build text from snippet description + top comments
        video_details = (candidate.extra or {}).get("youtube", {})
        snippet = video_details.get("snippet", {}) if isinstance(video_details, dict) else {}
        statistics = video_details.get("statistics", {}) if isinstance(video_details, dict) else {}
        title = extraction.title or snippet.get("title") or candidate.title
        description = snippet.get("description") or ""

        comments_texts: List[str] = []
        comments_meta: List[dict] = []
        video_id = video_details.get("id")
        if not video_id and candidate.url:
            # Try to parse from URL
            from urllib.parse import urlparse, parse_qs

            parsed = urlparse(candidate.url)
            qs = parse_qs(parsed.query)
            if "v" in qs and qs["v"]:
                video_id = qs["v"][0]

        # Fetch top-level comments (best-effort)
        if self.youtube_api_key and video_id:
            try:
                url = "https://www.googleapis.com/youtube/v3/commentThreads"
                params = {
                    "key": self.youtube_api_key,
                    "part": "snippet",
                    "videoId": video_id,
                    "maxResults": "50",
                    "order": "relevance",
                    "textFormat": "html",
                }
                resp = requests.get(url, params=params, timeout=20)
                if resp.status_code < 400:
                    data = resp.json()
                    for item in data.get("items", []):
                        top = item.get("snippet", {}).get("topLevelComment", {}).get("snippet", {})
                        text = top.get("textDisplay") or top.get("textOriginal") or ""
                        text_clean = self._youtube_strip_html(text)
                        if text_clean:
                            comments_texts.append(text_clean)
                            comments_meta.append(
                                {
                                    "author": top.get("authorDisplayName"),
                                    "likeCount": top.get("likeCount"),
                                    "publishedAt": top.get("publishedAt"),
                                }
                            )
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
