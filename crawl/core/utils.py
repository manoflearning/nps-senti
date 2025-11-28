from __future__ import annotations

import hashlib
from urllib.parse import ParseResult, parse_qsl, urlparse, urlunparse

ID_QUERY_KEYS = {
    "id",
    "no",
    "idx",
    "idxno",
    "arcid",
    "aid",
    "oid",
    "ud",
    "newsid",
    "articleid",
    "article_id",
    "seq",
    "sn",
    "docid",
}
COMPANION_KEYS = {
    # board/category hints that matter with IDs
    "code",
    "b",
    "board",
    "cid",
    "category",
    "mid",
}


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

    # TheQoo threads: ID in path; drop page params to dedupe listings
    if host_l.endswith("theqoo.net"):
        return []

    # Ppomppu view
    if host_l.endswith("ppomppu.co.kr") and "/zboard/view.php" in path_l:
        allow = {"id", "no"}
        return [(k, v) for k, v in pairs if k.lower() in allow]

    # KMIB articles: keep only arcid/code to dedupe stg/sid1 variants
    if host_l.endswith("kmib.co.kr") and (
        "view.asp" in path_l or "view_amp.asp" in path_l
    ):
        allow = {"arcid", "code"}
        return [(k, v) for k, v in pairs if k.lower() in allow]

    # Moneys MT: article view, keep only article id
    if (
        host_l.endswith("moneys.mt.co.kr") or host_l.endswith("moneys.co.kr")
    ) and "mwview.php" in path_l:
        allow = {"no"}
        return [(k, v) for k, v in pairs if k.lower() in allow]

    # MoneyToday news: mt/hot view, keep only article id
    if host_l.endswith("mt.co.kr") and (
        "mtview.php" in path_l or "hotview.php" in path_l
    ):
        allow = {"no"}
        return [(k, v) for k, v in pairs if k.lower() in allow]

    # Herald Corp / Korea Herald: keep only ud article id
    if host_l.endswith("heraldcorp.com") or host_l.endswith("koreaherald.com"):
        if "view.php" in path_l:
            allow = {"ud"}
            return [(k, v) for k, v in pairs if k.lower() in allow]

    # Naver News (read): keep only oid, aid
    if host_l.endswith("news.naver.com"):
        allow = {"oid", "aid"}
        return [(k, v) for k, v in pairs if k.lower() in allow]

    # Generic heuristic: if an ID-like key exists, keep only ID + companions
    lower_keys = [k.lower() for k, _ in pairs]
    if any(k in ID_QUERY_KEYS for k in lower_keys):
        allow = ID_QUERY_KEYS | COMPANION_KEYS
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
