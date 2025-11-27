from __future__ import annotations

from datetime import datetime
from typing import List, Optional
import re

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


# ---------- 게시글 본문 추출 (댓글 텍스트 제거) ----------


def extract_post_body(post: RawPost) -> str:
    """
    원본 post.raw_text 에는:
      - 갤러리 헤더/푸터 텍스트
      - 본문
      - 댓글 내용(이미 extra.forum.comments 로 따로 수집된 것)
    이 섞여 있기 때문에,

    1) 모든 댓글 텍스트(c.text)를 찾아서 제거한 뒤
    2) 공백/연속 줄바꿈을 가볍게 정리해서
    3) "게시글 본문에 가까운 텍스트"로 만든다.
    """
    text = (post.raw_text or "").strip()
    if not text:
        return ""

    # 1) 댓글 텍스트 제거
    for c in post.comments:
        t = (c.text or "").strip()
        if not t:
            continue
        if t in text:
            text = text.replace(t, "")

    # 2) 너무 많은 연속 줄바꿈 줄이기
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)

    # 3) 양쪽 공백 제거
    return text.strip()


# ---------- 댓글 id 선택 로직 ----------


def choose_comment_id(post: RawPost, comment_meta: dict, idx: int) -> str:
    """
    댓글 레코드에 사용할 id를 선택한다.

    우선순위 예시:
      1. meta["id"]          (댓글 id)
      2. meta["user_id"]
      3. meta["author"]
      4. meta["nickname"]
      5. 위가 다 없으면: f"{post.id}#c{idx}" (fallback)
    """
    meta = comment_meta or {}

    for key in ("id", "user_id", "author", "nickname"):
        value = meta.get(key)
        if value:
            return str(value)

    return f"{post.id}#c{idx}"


# ---------- 핵심: RawPost 하나 → FlattenedRecord 여러 개 ----------


def flatten_post(post: RawPost, max_comment_len: int = 200) -> List[FlattenedRecord]:
    """
    RawPost 하나를:
      - 원문-only 레코드 1개 (doc_type='post', comment_index=None)
      - 각 댓글이 붙은 레코드 N개 (doc_type='comment', comment_index=0..N-1)
    로 펼친다.

    핵심 규칙:
      - post 레코드 text  : "댓글 제거된 게시글 본문"
      - comment 레코드 text: "댓글 제거된 게시글 본문" (post와 동일)
      - 댓글 내용은 comment_text에만 들어간다.
    """
    records: List[FlattenedRecord] = []

    clean_title_str = clean_dcinside_title(post.title)
    article_dt, article_dt_str = resolve_article_datetime(post)

    # 게시글 본문 추출 (댓글 텍스트 제거)
    post_body = extract_post_body(post)

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
            text=post_body,          # 게시글 본문
            comment_index=None,
            comment_text=None,
            comment_publishedAt=None,
        )
    )

    # (2) 댓글 레코드들
    for idx, c in enumerate(post.comments):
        comment_dt_str: Optional[str] = None
        if c.published_at_raw:
            c_dt = parse_comment_datetime(c.published_at_raw, article_dt)
            comment_dt_str = format_comment_datetime(c_dt)

        comment_id = choose_comment_id(post, c.meta, idx)

        records.append(
            FlattenedRecord(
                id=comment_id,
                source=post.source,
                doc_type="comment",
                parent_id=post.id,
                title=clean_title_str,
                lang=post.lang or "ko",
                published_at=article_dt_str,
                text=post_body,          # ✅ post와 동일한 본문
                comment_index=idx,
                comment_text=c.text,     # ✅ 댓글 내용은 여기만
                comment_publishedAt=comment_dt_str,
            )
        )

    return records
