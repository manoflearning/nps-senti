# preprocess/preprocess_gdelt/stage2_transform.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional, Dict
import logging
import re
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

from .stage1_models_io import RawGdeltArticle, FlattenedGdeltArticle
import difflib


logger = logging.getLogger(__name__)


# ---------- 날짜 처리 ----------


def normalize_iso_utc(s: Optional[str]) -> Optional[str]:
    """
    published_at을 최대한 'YYYY-MM-DDTHH:MM:SSZ' 형태로 맞춘다.
    실패하면 None.
    """
    if not s:
        return None
    s = s.strip()
    if not s:
        return None

    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        # ISO 파싱이 안 되면 원문을 그대로 쓰지 않고 None 리턴
        return None


def choose_published_at(
    published_at: Optional[str],
    seendate: Optional[str],
) -> Optional[str]:
    """
    최종 published_at 선택 우선순위:
      1) published_at (제대로 된 ISO면 UTC로 정규화)
      2) seendate (마찬가지)
    """
    norm = normalize_iso_utc(published_at)
    if norm is not None:
        return norm

    norm2 = normalize_iso_utc(seendate)
    if norm2 is not None:
        return norm2

    # 둘 다 파싱 안 되면 원본 published_at이라도 돌려줌
    return (published_at or seendate or None)


# ---------- 텍스트 클리닝 ----------

TAIL_PATTERNS = [
    "all rights reserved",
    "무단 전재 및 재배포 금지",
    "©",
]


def clean_text(raw_text: str) -> str:
    """
    GDELT 뉴스 본문 텍스트 클리닝:
      - 사이트 footer/저작권 안내 일부 제거 (패턴 기반)
      - 공백/줄바꿈 정리
    """
    if not raw_text:
        return ""

    text = raw_text.replace("\r\n", "\n").replace("\r", "\n")

    lower_text = text.lower()
    cut_pos = None
    for pat in TAIL_PATTERNS:
        idx = lower_text.find(pat.lower())
        if idx != -1:
            if cut_pos is None or idx < cut_pos:
                cut_pos = idx
    if cut_pos is not None and cut_pos > 0:
        text = text[:cut_pos]

    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()


# ---------- 중복 처리 유틸 ----------

TITLE_NORM_SPACE_RE = re.compile(r"\s+")


def normalize_title_for_key(title: str) -> str:
    """
    중복 제거용 제목 정규화:
      - 뒤에 붙은 매체명/사이트명 잘라내기
      - 소문자 + 공백 축소
    예)
      'Government shutdown: what closes - NPR' ->
      'government shutdown: what closes'
    """
    t = (title or "").strip()

    for sep in (" - ", "｜", " | ", "|"):
        if sep in t:
            t = t.split(sep)[0]

    t = t.lower()
    t = TITLE_NORM_SPACE_RE.sub(" ", t)
    return t.strip()


def normalize_url_for_key(url: str) -> str:
    """
    URL 정규화:
      - scheme 제거 (http/https)
      - host 소문자
      - path 끝 슬래시 제거
      - utm_*, fbclid, gclid 등 추적용 쿼리 파라미터 제거
      - fragment 제거
    """
    if not url:
        return ""

    u = url.strip()
    try:
        parsed = urlparse(u)
    except Exception:
        return u.lower()

    netloc = (parsed.netloc or "").lower()
    path = (parsed.path or "").rstrip("/")

    keep_pairs = []
    if parsed.query:
        for k, v in parse_qsl(parsed.query, keep_blank_values=True):
            kl = k.lower()
            if kl.startswith("utm_"):
                continue
            if kl in {"fbclid", "gclid", "mc_cid", "mc_eid"}:
                continue
            keep_pairs.append((k, v))
    query = urlencode(keep_pairs, doseq=True)

    normalized = urlunparse(("", netloc, path, "", query, ""))
    return normalized


# ---------- 중복 제거 핵심 ----------


def deduplicate_records(
    records: List[FlattenedGdeltArticle],
) -> List[FlattenedGdeltArticle]:
    """
    GDELT 기사 중복 제거 (강화 버전).

    전략:
      1) 우선 (lang, normalized_title) 기준으로 그룹을 만든다.
      2) 그룹 안에서 text 유사도(SequenceMatcher 비율)가 0.995 이상이면
         사실상 같은 기사로 보고 1개만 남긴다.
      3) 같은 기사 그룹 안에서는
         - text 길이가 더 긴 것
         - 그 다음으로 published_at이 더 최신인 것
         을 우선 선택한다.

    이렇게 하면
      - 2296/2297처럼 제목/내용이 거의 같은 기사의 중복을 잡으면서
      - 제목만 같고 내용이 다른 건 그대로 여러 개 유지할 수 있다.
    """

    from collections import defaultdict

    def normalize_title_for_key(title: str) -> str:
        t = (title or "").strip()
        for sep in (" - ", "｜", " | ", "|"):
            if sep in t:
                t = t.split(sep)[0]
        t = t.lower()
        t = TITLE_NORM_SPACE_RE.sub(" ", t)
        return t.strip()

    def normalize_url_for_key(url: str) -> str:
        if not url:
            return ""
        u = url.strip()
        try:
            parsed = urlparse(u)
        except Exception:
            return u.lower()

        netloc = (parsed.netloc or "").lower()
        path = (parsed.path or "").rstrip("/")

        keep_pairs = []
        if parsed.query:
            for k, v in parse_qsl(parsed.query, keep_blank_values=True):
                kl = k.lower()
                if kl.startswith("utm_"):
                    continue
                if kl in {"fbclid", "gclid", "mc_cid", "mc_eid"}:
                    continue
                keep_pairs.append((k, v))
        query = urlencode(keep_pairs, doseq=True)

        normalized = urlunparse(("", netloc, path, "", query, ""))
        return normalized

    def parse_dt(s: Optional[str]) -> Optional[datetime]:
        if not s:
            return None
        s = s.strip()
        try:
            if s.endswith("Z"):
                s2 = s.replace("Z", "+00:00")
            else:
                s2 = s
            return datetime.fromisoformat(s2)
        except Exception:
            return None

    def choose_better(a: FlattenedGdeltArticle, b: FlattenedGdeltArticle) -> FlattenedGdeltArticle:
        # 1) text 길이가 긴 것 우선
        len_a = len(a.text or "")
        len_b = len(b.text or "")
        if len_b > len_a:
            winner, loser = b, a
        elif len_a > len_b:
            winner, loser = a, b
        else:
            # 2) 길이가 같으면 published_at 더 최신인 쪽
            da = parse_dt(a.published_at)
            db = parse_dt(b.published_at)
            if db and (not da or db > da):
                winner, loser = b, a
            else:
                winner, loser = a, b
        return winner

    # 1단계: (lang, normalized_title) 로 그룹핑
    groups: Dict[tuple, List[FlattenedGdeltArticle]] = defaultdict(list)
    for rec in records:
        lang_norm = (rec.lang or "").strip().lower()
        title_norm = normalize_title_for_key(rec.title or "")
        if lang_norm and title_norm:
            key = ("title", lang_norm, title_norm)
        elif rec.url:
            key = ("url", normalize_url_for_key(rec.url))
        else:
            key = ("id", rec.id)
        groups[key].append(rec)

    deduped: List[FlattenedGdeltArticle] = []
    total_merged = 0

    # 2단계: 각 그룹 안에서 text 유사도 기반 dedup
    for key, recs in groups.items():
        selected: List[FlattenedGdeltArticle] = []
        for rec in recs:
            merged = False
            for i, kept in enumerate(selected):
                sim = difflib.SequenceMatcher(None, kept.text or "", rec.text or "").ratio()
                # 🔥 거의 완전히 같은 기사면 같은 것으로 본다
                if sim >= 0.995:
                    better = choose_better(kept, rec)
                    selected[i] = better
                    total_merged += 1
                    merged = True
                    break
            if not merged:
                selected.append(rec)
        deduped.extend(selected)

    if total_merged > 0:
        logger.info(
            "[INFO] GDELT 중복 제거 (제목+텍스트 유사도 기반): "
            "원본 %d개 → 중복 병합 %d개 → 최종 %d개",
            len(records),
            total_merged,
            len(deduped),
        )
    else:
        logger.info("[INFO] GDELT 중복 제거 결과: 병합된 중복 없음 (원본 %d개)", len(records))

    return deduped


# ---------- Raw → Flattened ----------


def flatten_article(
    raw: RawGdeltArticle,
    min_length: int = 0,
    max_length: Optional[int] = None,
) -> Optional[FlattenedGdeltArticle]:
    """
    RawGdeltArticle 하나를 전처리하여 FlattenedGdeltArticle 로 변환.
    text 길이 기준(min_length, max_length)에 걸리면 None 반환.
    """
    title = (raw.title or "").strip()
    text_clean = clean_text(raw.text or "")
    length = len(text_clean)

    if min_length and length < min_length:
        return None
    if max_length is not None and length > max_length:
        return None

    published_at_iso = choose_published_at(raw.published_at, raw.seendate)

    return FlattenedGdeltArticle(
        id=raw.id,
        source=raw.source or "gdelt",
        lang=raw.lang or "en",
        title=title,
        text=text_clean,
        published_at=published_at_iso,
        url=raw.url,
    )
