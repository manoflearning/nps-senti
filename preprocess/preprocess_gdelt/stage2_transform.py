from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional, Dict
import logging
import re

from .stage1_models_io import RawGdeltArticle, FlattenedGdeltArticle


logger = logging.getLogger(__name__)


# ---------- 날짜 처리 ----------


def choose_published_at(
    published_at: Optional[str],
    seendate: Optional[str],  # 현재는 사용하지 않지만, 시그니처는 유지
) -> Optional[str]:
    """
    최종 published_at 문자열 선택.

    ✅ 지금은 원본 published_at만 사용하고, seendate는 사용하지 않는다.
    - published_at이 ISO8601 형식이면 UTC 기준 "YYYY-MM-DDTHH:MM:SSZ" 로 정규화
    - 파싱 실패하면 strip만 한 원본 문자열을 그대로 사용
    """

    if not published_at:
        return None

    s = published_at.strip()
    if not s:
        return None

    try:
        # 'Z'를 포함하거나, 타임존 없는 경우 모두 처리
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        # ISO 파싱이 안 되면, 그냥 정리된 원본 문자열을 반환
        return s


# ---------- 텍스트 클리닝 ----------

TAIL_PATTERNS = [
    # 뉴스 사이트 공통 footer, 저작권, 뉴스레터 안내 등 있다면 여기 추가
    "All rights reserved",
    "무단 전재 및 재배포 금지",
    "뉴스레터를 구독하세요",
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

    # 너무 많은 연속 줄바꿈 축소
    text = re.sub(r"\n{3,}", "\n\n", text)
    # 여러 공백 축소
    text = re.sub(r"[ \t]{2,}", " ", text)

    return text.strip()


# ---------- 중복 처리 ----------


def deduplicate_records(
    records: List[FlattenedGdeltArticle],
) -> List[FlattenedGdeltArticle]:
    """
    GDELT 기사 중복 제거.

    기준:
      - id가 있으면 id 기준
      - id가 비어 있으면 title 기준
    같은 key가 여러 개면:
      - text 길이가 더 긴 기사 선택
    """
    chosen: Dict[tuple, FlattenedGdeltArticle] = {}
    order: List[tuple] = []

    def make_key(rec: FlattenedGdeltArticle) -> tuple:
        if rec.id:
            return ("id", rec.id)
        return ("title", rec.title or "")

    for rec in records:
        key = make_key(rec)
        if key not in chosen:
            chosen[key] = rec
            order.append(key)
        else:
            prev = chosen[key]
            if len(rec.text) > len(prev.text):
                chosen[key] = rec

    if len(order) != len(chosen):
        logger.info(
            "[INFO] GDELT 중복 제거: 원본 %d개 → 중복 제거 후 %d개",
            len(records),
            len(chosen),
        )

    return [chosen[k] for k in order]


# ---------- 핵심: RawGdeltArticle → FlattenedGdeltArticle ----------


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

    # ✅ published_at만 사용, seendate는 fallback으로 쓰지 않는다.
    published_at_iso = choose_published_at(raw.published_at, raw.seendate)

    return FlattenedGdeltArticle(
        id=raw.id,
        source=raw.source or "gdelt",
        lang=raw.lang or "ko",
        title=title,
        text=text_clean,
        published_at=published_at_iso,
    )
