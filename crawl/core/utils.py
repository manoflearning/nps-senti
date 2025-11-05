from __future__ import annotations

import hashlib
from urllib.parse import ParseResult, parse_qsl, urlparse, urlunparse


def _filter_query_by_domain(
    host: str, path: str, pairs: list[tuple[str, str]]
) -> list[tuple[str, str]]:
    host_l = (host or "").lower()
    path_l = (path or "").lower()

    # DCInside thread view: keep only id and no
    if host_l.endswith("dcinside.com") and "/board/view/" in path_l:
        allow = {"id", "no"}
        return [(k, v) for k, v in pairs if k.lower() in allow]

    # Bobaedream thread view: legacy and current
    if host_l.endswith("bobaedream.co.kr") and (
        "/board/bbs_view" in path_l or path_l == "/view"
    ):
        allow = {"code", "no"}
        return [(k, v) for k, v in pairs if k.lower() in allow]

    # (FMKorea support removed)

    # MLBPark view: keep board and post id
    if host_l.endswith("mlbpark.donga.com") and "/mp/b.php" in path_l:
        # Threads typically have m=view and an id; sometimes idx is used
        allow = {"b", "id", "idx"}
        return [(k, v) for k, v in pairs if k.lower() in allow]

    # TheQoo usually path-based; no special query handling needed

    # Ppomppu view
    if host_l.endswith("ppomppu.co.kr") and "/zboard/view.php" in path_l:
        allow = {"id", "no"}
        return [(k, v) for k, v in pairs if k.lower() in allow]

    # Naver News (read): keep only oid, aid
    if host_l.endswith("news.naver.com"):
        allow = {"oid", "aid"}
        return [(k, v) for k, v in pairs if k.lower() in allow]

    return pairs


def normalize_url(url: str) -> str:
    parsed: ParseResult = urlparse(url.strip())
    scheme = parsed.scheme.lower() or "http"
    netloc = parsed.hostname.lower() if parsed.hostname else ""
    if parsed.port and parsed.port not in {80, 443}:
        netloc = f"{netloc}:{parsed.port}"
    path = parsed.path or "/"
    # Parse query to list and drop tracking params
    raw_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    blacklist = {
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_term",
        "utm_content",
        "fbclid",
        "gclid",
        "igshid",
        "mibextid",
        "ref",
        "ref_src",
        "spm",
    }
    filtered_pairs = [(k, v) for k, v in raw_pairs if k.lower() not in blacklist]
    # Apply domain-specific allowlists
    filtered_pairs = _filter_query_by_domain(netloc, path, filtered_pairs)
    # Sort for stability
    query_pairs = sorted(filtered_pairs)
    query = "&".join(f"{k}={v}" for k, v in query_pairs)
    normalized = urlunparse((scheme, netloc, path, "", query, ""))
    return normalized.rstrip("?")


def sha1_hex(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()
