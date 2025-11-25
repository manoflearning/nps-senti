# preprocess/preprocess_dcinside/stage2_transform.py
from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from .stage1_models_io import RawPost, FlattenedRecord


# ---------- 제목 클리닝 ----------


def clean_dcinside_title(title: str) -> str:
    """
    디시인사이드 국민연금 갤러리 제목에서
    뒤에 붙는 ' - 국민연금 마이너 갤러리' suffix를 제거한다.
    """
    if not title:
        return ""
    title = title.strip()
    suffix = " - 국민연금 마이너 갤러리"
    if title.endswith(suffix):
        return title[: -len(suffix)].rstrip()
    return title


# ---------- 게시글 기준 시각 처리 ----------


def _parse_iso_datetime(value: str) -> Optional[datetime]:
    """
    ISO 8601 비슷한 문자열(published_at, crawl.fetched_at)을 datetime으로 파싱.
    예: "2025-11-16T12:38:35.315239Z" → 2025-11-16 12:38:35+00:00
    """
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def resolve_article_datetime(post: RawPost) -> tuple[Optional[datetime], Optional[str]]:
    """
    게시글 기준 시각과 그 시각을 문자열로 반환.
    우선순위:
      1. published_at
      2. crawl_fetched_at
    """
    dt = _parse_iso_datetime(post.published_at)
    if dt is None:
        dt = _parse_iso_datetime(post.crawl_fetched_at)

    if dt is None:
        return None, None

    dt = dt.replace(microsecond=0)
    return dt, dt.isoformat()


# ---------- 댓글 시각 파싱 ----------


def parse_comment_datetime(
    comment_raw: str, article_dt: Optional[datetime]
) -> Optional[datetime]:
    """
    댓글 시각 문자열을 datetime으로 파싱한다.

    지원 패턴:
      1) "YYYY.MM.DD HH:MM:SS"
      2) "MM.DD HH:MM:SS"  (연도 없으면 article_dt.year 사용)
    """
    if not comment_raw:
        return None

    s = comment_raw.strip()

    # 1) 연도가 있는 경우
    try:
        return datetime.strptime(s, "%Y.%m.%d %H:%M:%S")
    except Exception:
        pass

    # 2) 연도 없는 경우: 게시글 연도를 붙여서 사용
    try:
        mmdd = datetime.strptime(s, "%m.%d %H:%M:%S")
    except Exception:
        return None

    year = article_dt.year if article_dt is not None else datetime.utcnow().year
    return datetime(
        year=year,
        month=mmdd.month,
        day=mmdd.day,
        hour=mmdd.hour,
        minute=mmdd.minute,
        second=mmdd.second,
        microsecond=0,
    )


def format_comment_datetime(dt_value: Optional[datetime]) -> Optional[str]:
    """
    datetime → "YYYY-MM-DD HH:MM:SS" 문자열로 변환.
    """
    if dt_value is None:
        return None
    return dt_value.strftime("%Y-%m-%d %H:%M:%S")


# ---------- 텍스트 전처리 ----------


def center_truncate(text: str, max_len: int = 200) -> str:
    """
    너무 긴 텍스트는 앞/뒤를 남기고 가운데를 "..."로 줄인다.
    """
    text = (text or "").strip()
    if len(text) <= max_len:
        return text
    keep = max_len - 3
    head = keep // 2
    tail = keep - head
    return text[:head] + "..." + text[-tail:]


def build_combined_text(
    clean_title: str, comment_text: str | None, max_comment_len: int = 200
) -> str:
    """
    최종 combined_text 규칙:

    - 댓글이 없으면: 제목만
    - 댓글이 있으면:
      제목 + 공백줄 + "[댓글]" + 줄바꿈 + (적당히 줄인 댓글 내용)
    """
    title = (clean_title or "").strip()

    if not comment_text:
        return title

    short_comment = center_truncate(comment_text, max_len=max_comment_len)
    return f"{title}\n\n[댓글]\n{short_comment}"


# ---------- 핵심: RawPost 하나 → FlattenedRecord 여러 개 ----------


def flatten_post(post: RawPost, max_comment_len: int = 200) -> List[FlattenedRecord]:
    """
    RawPost 하나를:
      - 원문-only 레코드 1개 (doc_type='post', comment_index=None)
      - 각 댓글이 붙은 레코드 N개 (doc_type='comment', comment_index=0..N-1)
    로 펼친다.
    """
    records: List[FlattenedRecord] = []

    clean_title_str = clean_dcinside_title(post.title)
    article_dt, article_dt_str = resolve_article_datetime(post)

    # (1) 본문-only 레코드
    records.append(
        FlattenedRecord(
            id=post.id,
            source=post.source,
            doc_type="post",
            parent_id=None,
            title=clean_title_str,
            lang=post.lang or "ko",
            published_at=article_dt_str,
            comment_index=None,
            comment_text=None,
            comment_publishedAt=None,
            combined_text=build_combined_text(
                clean_title=clean_title_str,
                comment_text=None,
                max_comment_len=max_comment_len,
            ),
        )
    )

    # (2) 댓글 레코드들
    for idx, c in enumerate(post.comments):
        comment_dt_str: Optional[str] = None
        if c.published_at_raw:
            c_dt = parse_comment_datetime(c.published_at_raw, article_dt)
            comment_dt_str = format_comment_datetime(c_dt)

        combined = build_combined_text(
            clean_title=clean_title_str,
            comment_text=c.text,
            max_comment_len=max_comment_len,
        )

        records.append(
            FlattenedRecord(
                id=post.id,
                source=post.source,
                doc_type="comment",
                parent_id=post.id,
                title=clean_title_str,
                lang=post.lang or "ko",
                published_at=article_dt_str,
                comment_index=idx,
                comment_text=c.text,
                comment_publishedAt=comment_dt_str,
                combined_text=combined,
            )
        )

    return records
