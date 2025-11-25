# preprocess/preprocess_youtube/stage2_transform.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict, Tuple
import logging
import re

from .stage1_models_io import RawYoutubeVideo, FlattenedYoutubeVideo


logger = logging.getLogger(__name__)


# ---------- 날짜/시간 처리 ----------

def choose_published_at(
    top_published: Optional[str],
    snippet_published: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    """
    최종 published_at 문자열과 그 출처를 선택한다.

    우선순위:
      1. 상위 published_at
      2. snippet.publishedAt
    """
    def _normalize_iso(s: str) -> Optional[str]:
        if not s:
            return None
        s = s.strip()
        if not s:
            return None
        try:
            # datetime.fromisoformat 은 'Z'를 못 읽어서 직접 처리
            if s.endswith("Z"):
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            else:
                dt = datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
            # 표준 출력은 UTC Z 형식으로
            dt_utc = dt.astimezone(timezone.utc)
            return dt_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        except Exception:
            return None

    # 1) 상위 published_at
    if top_published:
        norm = _normalize_iso(top_published)
        if norm is not None:
            return norm, "published_at"

    # 2) snippet.publishedAt
    if snippet_published:
        norm = _normalize_iso(snippet_published)
        if norm is not None:
            return norm, "extra.youtube.snippet.publishedAt"

    return None, None


def to_kst(iso_utc: Optional[str]) -> Optional[str]:
    """
    "YYYY-MM-DDTHH:MM:SSZ" (UTC) → KST(UTC+9) 로 변환한 ISO 문자열.
    (현재는 사용하지 않지만, 필요시 파생컬럼 만들 때 활용 가능)
    """
    if not iso_utc:
        return None
    try:
        dt = datetime.fromisoformat(iso_utc.replace("Z", "+00:00"))
        kst = dt + timedelta(hours=9)
        return kst.replace(microsecond=0).isoformat()
    except Exception:
        return None


# ---------- 텍스트 클리닝 (YouTube 특화) ----------

YOUTUBE_TAIL_PATTERNS = [
    # 유튜브 시스템/저작권 안내 (대표적인 키워드들)
    "yt-support-solutions-kr@google.com",
    "YouTube 상에 게시, 태그 또는 추천한 상품들은",
    "유튜브 상에 게시, 태그 또는 추천한 상품들은",
    "불법촬영물 신고",
    "저작권 침해 신고",
    "© 20",  # "© 2024 Google LLC" 같은 패턴
    "Google LLC",
]

HASHTAG_RE = re.compile(r"#(\w+)")


def extract_hashtags(text: str):
    """
    해시태그를 추출하면서, 텍스트 내에서는 '#'만 제거하고 단어는 살린다.
    예: "#국민연금 #노후생활비" → 텍스트: "국민연금 노후생활비", hashtags: ["국민연금", "노후생활비"]
    """
    hashtags: List[str] = []

    def _repl(m: re.Match) -> str:
        tag = m.group(1)
        hashtags.append(tag)
        return tag  # '#' 제거하고 단어만 남김

    new_text = HASHTAG_RE.sub(_repl, text)
    return new_text, hashtags


def clean_description(raw_desc: str):
    """
    유튜브 설명을 클리닝:
      - 시스템 꼬리 문구(불법촬영 신고, Google LLC 안내 등) 이후 삭제
      - 해시태그에서 '#' 제거 + 별도 리스트 수집
      - 공백/줄바꿈 정리
    """
    if not raw_desc:
        return "", []

    text = raw_desc.replace("\r\n", "\n").replace("\r", "\n")

    # 유튜브 tail 패턴 이후는 잘라내기
    lower_text = text.lower()
    cut_pos = None
    for pat in YOUTUBE_TAIL_PATTERNS:
        idx = lower_text.find(pat.lower())
        if idx != -1:
            if cut_pos is None or idx < cut_pos:
                cut_pos = idx
    if cut_pos is not None and cut_pos > 0:
        text = text[:cut_pos]

    # 해시태그 추출 + '#' 제거
    text, hashtags = extract_hashtags(text)

    # 너무 많은 연속 줄바꿈은 2개로 축소
    text = re.sub(r"\n{3,}", "\n\n", text)
    # 여러 공백을 1개로
    text = re.sub(r"[ \t]{2,}", " ", text)

    return text.strip(), hashtags


def clean_title(raw_title: str) -> str:
    """
    제목에서도 해시태그를 '#국민연금' → '국민연금' 처럼 정리.
    """
    if not raw_title:
        return ""
    text, _hashtags = extract_hashtags(raw_title)
    # 공백 정리 정도만 가볍게
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()


def build_text_clean(title: str, clean_description: str) -> str:
    """
    최종 text_clean (내부에서만 사용):
      - 설명이 있으면: "제목\n\n설명"
      - 없으면: "제목"만
    """
    t = (title or "").strip()
    d = (clean_description or "").strip()

    if d:
        return f"{t}\n\n{d}"
    return t


# ---------- 중복 처리 ----------

def deduplicate_records(records: List[FlattenedYoutubeVideo]) -> List[FlattenedYoutubeVideo]:
    """
    중복 영상 제거.
    기준:
      - key = id (있으면)
      - 없으면 title
    같은 key가 여러 개면:
      - (제목 + 설명) 길이가 더 긴 것 선택
    """
    chosen: Dict[tuple, FlattenedYoutubeVideo] = {}
    order: List[tuple] = []

    def make_key(rec: FlattenedYoutubeVideo) -> tuple:
        if rec.id:
            return ("id", rec.id)
        return ("title", rec.title or "")

    def text_len(rec: FlattenedYoutubeVideo) -> int:
        if rec.description:
            return len(rec.title) + 2 + len(rec.description)
        return len(rec.title)

    for rec in records:
        key = make_key(rec)
        if key not in chosen:
            chosen[key] = rec
            order.append(key)
        else:
            prev = chosen[key]
            if text_len(rec) > text_len(prev):
                chosen[key] = rec

    if len(order) != len(chosen):
        logger.info(
            "[INFO] YouTube 중복 제거: 원본 %d개 → 중복 제거 후 %d개",
            len(records),
            len(chosen),
        )

    return [chosen[k] for k in order]


# ---------- 핵심: RawYoutubeVideo → FlattenedYoutubeVideo ----------

def flatten_video(
    raw: RawYoutubeVideo,
    min_length: int = 0,
    max_length: Optional[int] = None,
) -> Optional[FlattenedYoutubeVideo]:
    """
    RawYoutubeVideo 하나를 전처리하여 FlattenedYoutubeVideo 로 변환.
    text_clean 길이 기준(min_length, max_length)에 걸리면 None 반환.
    (text_clean 자체는 파일에 저장하지 않고 내부 필터링/중복제거용으로만 사용)
    """
    # 제목: snippet.title 우선, 없으면 상위 title → 해시태그 정리 포함
    title_raw = raw.snippet_title or raw.title_top or ""
    title = clean_title(title_raw)

    # 설명: snippet.description 우선 사용
    description_raw = raw.snippet_description or ""
    description_clean, _hashtags = clean_description(description_raw)

    text_clean = build_text_clean(title, description_clean)
    length = len(text_clean)

    # 길이 필터링
    if min_length and length < min_length:
        return None
    if max_length is not None and length > max_length:
        return None

    # 날짜 선택
    published_at_iso, _pub_source = choose_published_at(
        top_published=raw.published_at_top,
        snippet_published=raw.snippet_published_at,
    )

    return FlattenedYoutubeVideo(
        id=raw.id,
        source=raw.source or "youtube",
        lang=raw.lang or "ko",
        title=title,
        description=description_clean,
        published_at=published_at_iso,
    )
