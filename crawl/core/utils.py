from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Iterable
from urllib.parse import ParseResult, parse_qsl, urlparse, urlunparse

ISO_8601_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)?$"
)


def normalize_url(url: str) -> str:
    parsed: ParseResult = urlparse(url.strip())
    scheme = parsed.scheme.lower() or "http"
    netloc = parsed.hostname.lower() if parsed.hostname else ""
    if parsed.port and parsed.port not in {80, 443}:
        netloc = f"{netloc}:{parsed.port}"
    path = parsed.path or "/"
    query_pairs = sorted(parse_qsl(parsed.query, keep_blank_values=True))
    query = "&".join(
        f"{k}={v}"
        for k, v in query_pairs
        if k.lower()
        not in {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content"}
    )
    normalized = urlunparse((scheme, netloc, path, "", query, ""))
    return normalized.rstrip("?")


def sha1_hex(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def keyword_coverage(text: str, keywords: Iterable[str]) -> float:
    if not text:
        return 0.0
    text_lower = text.lower()
    keywords_list = [kw.lower() for kw in keywords]
    if not keywords_list:
        return 0.0
    hits = sum(1 for kw in keywords_list if kw in text_lower)
    if hits <= 0:
        return 0.0
    return hits / len(keywords_list)


def json_dumps_clean(data: dict) -> str:
    return json.dumps(data, ensure_ascii=False)


def ensure_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        return dt.isoformat() + "Z"
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
