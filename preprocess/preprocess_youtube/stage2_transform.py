# preprocess/preprocess_youtube/stage2_transform.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, List, Optional
import logging
import re

from .stage1_models_io import RawYoutubeVideo, FlattenedYoutubeComment


logger = logging.getLogger(__name__)


# ---------- 날짜/시간 처리 ----------


def _normalize_iso_utc(s: Optional[str]) -> Optional[str]:
    """
    다양한 ISO 형태(끝에 Z, +09:00, tz 없는 경우)를 최대한
    'YYYY-MM-DDTHH:MM:SSZ' (UTC) 로 맞춰줌.
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
        logger.debug("날짜 파싱 실패, 원문 유지: %r", s)
        return None


def choose_published_at(
    top_published: Optional[str],
    snippet_published: Optional[str],
) -> Optional[str]:
    """
    최종 published_at 문자열(UTC Z)을 선택한다.

    우선순위:
      1. 상위 published_at
      2. snippet.publishedAt
    """
    # 1) 상위 published_at
    if top_published:
        norm = _normalize_iso_utc(top_published)
        if norm is not None:
            return norm

    # 2) snippet.publishedAt
    if snippet_published:
        norm = _normalize_iso_utc(snippet_published)
        if norm is not None:
            return norm

    return None


# ---------- 텍스트 클리닝 (YouTube 특화) ----------

YOUTUBE_TAIL_PATTERNS = [
    "yt-support-solutions-kr@google.com",
    "YouTube 상에 게시, 태그 또는 추천한 상품들은",
    "유튜브 상에 게시, 태그 또는 추천한 상품들은",
    "불법촬영물 신고",
    "저작권 침해 신고",
    "© 20",
    "Google LLC",
]

# 설명/본문에서 해시태그 제거용
HASHTAG_RE = re.compile(r"#(\w+)")
# 제목에서 '#단어' 토큰 전체를 날리기 위한 패턴
TITLE_HASHTAG_TOKEN_RE = re.compile(r"#\S+")


def _extract_hashtags_and_clean(text: str) -> str:
    """
    설명(text)에서 해시태그를 '#국민연금' → '국민연금'처럼 정리.
    (설명은 단어만 살리고 '#'만 제거)
    """
    if not text:
        return ""
    text = HASHTAG_RE.sub(lambda m: m.group(1), text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()


def clean_description(raw_desc: str) -> str:
    """
    유튜브 설명 클리닝:
      - 시스템 꼬리 문구(불법촬영 신고, Google LLC 안내 등) 이후 삭제
      - 해시태그에서 '#' 제거 (단어는 유지)
      - 공백/줄바꿈 정리
    """
    if not raw_desc:
        return ""

    text = raw_desc.replace("\r\n", "\n").replace("\r", "\n")

    lower_text = text.lower()
    cut_pos: Optional[int] = None
    for pat in YOUTUBE_TAIL_PATTERNS:
        idx = lower_text.find(pat.lower())
        if idx != -1:
            if cut_pos is None or idx < cut_pos:
                cut_pos = idx
    if cut_pos is not None and cut_pos > 0:
        text = text[:cut_pos]

    text = _extract_hashtags_and_clean(text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def clean_title(raw_title: str) -> str:
    """
    제목에서 해시태그 '#국민연금', '#연금수령' 같은 토큰을
    **통째로 제거**한다.

    예)
      '2025년 국민연금 예상수령액! 현실인가 #국민연금 #연금수령' ->
      '2025년 국민연금 예상수령액! 현실인가'
    """
    if not raw_title:
        return ""

    # 줄바꿈을 공백으로 통일
    text = raw_title.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")

    # '#단어' 토큰 전체 삭제 (앞 공백 포함해서 지워서 깔끔하게)
    text = TITLE_HASHTAG_TOKEN_RE.sub(" ", text)

    # 남은 공백 정리
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


def build_video_text(title: str, description: str) -> str:
    """
    영상 컨텍스트 텍스트:
      - 설명이 있으면: "제목\n\n설명"
      - 없으면: "제목"만 or "설명"만
    """
    t = (title or "").strip()
    d = (description or "").strip()

    if t and d:
        return f"{t}\n\n{d}"
    if t:
        return t
    return d


# ---------- 핵심: RawYoutubeVideo → FlattenedYoutubeComment 리스트 ----------


def flatten_video_to_comments(
    raw: RawYoutubeVideo,
    *,
    min_length: int = 0,
) -> List[FlattenedYoutubeComment]:
    """
    RawYoutubeVideo 하나를 "댓글 1개 = 1줄" 구조로 펼친다.

    스키마:
      - id, source, lang, title, text, published_at
      - comment_index, comment_text, comment_publishedAt

    댓글이 하나도 없는 영상도 반드시 1줄 생성:
      - comment_index = None
      - comment_text = None
      - comment_publishedAt = None
    """
    # 제목/설명 정리
    # ★ snippet_title이 있든 없든, 최종적으로 clean_title을 한 번 거치기 때문에
    #    모든 유튜브 행의 title에서 해시태그가 제거된다.
    title_raw = raw.snippet_title or raw.title_top or ""
    title = clean_title(title_raw)

    desc_source = raw.snippet_description or raw.text_top or ""
    description = clean_description(desc_source)

    video_text = build_video_text(title, description)

    # 영상 게시 시각
    video_published = choose_published_at(
        top_published=raw.published_at_top,
        snippet_published=raw.snippet_published_at,
    )

    # 댓글 가져오기
    yt = raw.extra.get("youtube") or {}
    if not isinstance(yt, dict):
        yt = {}
    comments = yt.get("comments") or []
    if not isinstance(comments, list):
        comments = []

    results: List[FlattenedYoutubeComment] = []

    # 1) 댓글이 아예 없는 영상도 반드시 1줄 생성
    if not comments:
        text = video_text

        if min_length and len(text) < min_length:
            return results  # 길이 기준 미달이면 이 영상 전체 스킵

        results.append(
            FlattenedYoutubeComment(
                id=raw.id,
                source=raw.source or "youtube",
                lang=raw.lang or "ko",
                title=title,
                text=text,
                published_at=video_published,
                comment_index=None,
                comment_text=None,
                comment_publishedAt=None,
            )
        )
        return results

    # 2) 댓글이 있는 경우: 댓글 개수만큼 row 생성
    for idx, c in enumerate(comments):
        if not isinstance(c, dict):
            continue

        c_text = (c.get("text") or "").strip()
        if not c_text:
            continue

        comment_published_raw = (
            c.get("publishedAt")
            or c.get("published_at")
            or c.get("published")
        )
        comment_published = _normalize_iso_utc(comment_published_raw)

        if video_text:
            text = f"{video_text}\n\n{c_text}"
        else:
            text = c_text

        if min_length and len(text) < min_length:
            continue

        results.append(
            FlattenedYoutubeComment(
                id=raw.id,
                source=raw.source or "youtube",
                lang=raw.lang or "ko",
                title=title,
                text=text,
                published_at=video_published,
                comment_index=idx,
                comment_text=c_text,
                comment_publishedAt=comment_published,
            )
        )

    return results


def flatten_many_videos_to_comments(
    raws: Iterable[RawYoutubeVideo],
    *,
    min_length: int = 0,
    lang_filter: Optional[List[str]] = None,
) -> List[FlattenedYoutubeComment]:
    """
    RawYoutubeVideo 이터러블 전체를 펼쳐서 FlattenedYoutubeComment 리스트로 만든다.

    - lang_filter: ["ko", "en"] 같이 lang 필터링
    - min_length: text (영상+댓글 or 영상만) 최소 길이
    - 댓글이 없는 영상도 반드시 1줄 생성
    """
    lang_set = {lng.lower() for lng in lang_filter} if lang_filter else None

    results: List[FlattenedYoutubeComment] = []
    total_videos = 0
    total_rows = 0

    for raw in raws:
        total_videos += 1

        if lang_set is not None and (raw.lang or "").lower() not in lang_set:
            continue

        rows = flatten_video_to_comments(raw, min_length=min_length)
        if not rows:
            continue

        results.extend(rows)
        total_rows += len(rows)

    logger.info(
        "[INFO] YouTube 원본 영상 %d개 → 댓글 기반 레코드 %d개 (댓글 없는 영상 포함)",
        total_videos,
        total_rows,
    )

    return results
