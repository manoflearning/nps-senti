from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Callable, Iterator, List, Dict, Iterable, Optional
import re

try:
    # ppomppu/theqoo 전처리 결과 제너레이터 가져오기
    from .format_ppomppu import iter_formatted_rows as iter_ppomppu_rows  # type: ignore
    from .format_theqoo import iter_formatted_rows as iter_theqoo_rows  # type: ignore
except ImportError:  # 직접 실행할 때 fallback
    from format_ppomppu import iter_formatted_rows as iter_ppomppu_rows  # type: ignore
    from format_theqoo import iter_formatted_rows as iter_theqoo_rows  # type: ignore


# 🔹 프로젝트 루트: .../nps-senti
# __file__ = preprocess/preprocess_forum4/format_forums_combined.py
# parents[0] = preprocess_forum4, parents[1] = preprocess, parents[2] = nps-senti
BASE_DIR = Path(__file__).resolve().parents[2]

# 입력 원본 디렉토리: 루트/data_crawl
DATA_DIR = BASE_DIR / "data_crawl"

# 출력 디렉토리: 루트/preprocess/preprocessing_data
PREPROCESSING_DIR = BASE_DIR / "preprocess" / "preprocessing_data"


# ---------------------------------------------------------------------------
# 공통 유틸
# ---------------------------------------------------------------------------


def read_jsonl(path: Path) -> Iterator[dict]:
    """UTF-8 JSONL을 한 줄씩 안전하게 읽는다."""
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open("r", encoding="utf-8") as f:
        for line_no, raw in enumerate(f, 1):
            line = raw.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as err:
                raise ValueError(f"{path} line {line_no}: {err}") from err


def collect_comment_rows(
    source: str,
    post_id: str,
    title: str,
    published_at: str | None,
    base_lang: str,
    comments: Iterable[dict] | None,
    base_text: Optional[str] = None,
) -> Iterator[dict]:
    """
    forum.extra.forum.comments 구조를 공통 스키마(comment)로 변환.

    공통 스키마:
      - id                = f"{post_id}_{idx}"   (0부터 시작하는 댓글 인덱스)
      - source
      - doc_type="comment"
      - parent_id         = post_id
      - title
      - lang
      - published_at      (게시글 기준)
      - text              (옵션: base_text가 주어지면 게시글 본문)
      - comment_index
      - comment_text
      - comment_publishedAt
    """
    if not comments:
        return
    for idx, comment in enumerate(comments):
        if not isinstance(comment, dict):
            continue

        comment_text = (comment.get("text") or "").strip()
        if not comment_text:
            continue

        # ✅ 디시/유튜브 스타일: comment id = "{post_id}_{idx}"
        comment_id = f"{post_id}_{idx}"
        comment_lang = comment.get("lang") or base_lang

        # 키 순서를 보장하기 위해 dict를 순서대로 생성
        row: Dict[str, object] = {}
        row["id"] = comment_id
        row["source"] = source
        row["doc_type"] = "comment"
        row["parent_id"] = post_id
        row["title"] = title
        # mlbpark/보배 등: 댓글 레코드에도 게시글 본문 text를 공유하고 싶을 때
        if base_text is not None:
            row["text"] = base_text
        row["lang"] = comment_lang
        row["published_at"] = published_at
        row["comment_index"] = idx
        row["comment_text"] = comment_text
        row["comment_publishedAt"] = comment.get("publishedAt")

        yield row


# ---------------------------------------------------------------------------
# mlbpark (디시 스타일로 맞추기)
# ---------------------------------------------------------------------------


def extract_post_body_mlbpark(post: dict) -> str:
    """
    mlbpark 원본 post["text"] 안에는
      - 게시글 본문
      - 사이트 크롬/기타
      - 댓글 내용 (이미 extra.forum.comments에 따로 있음)
    이 섞여 있을 수 있으므로,

    1) extra.forum.comments[*].text 를 찾아서 제거하고
    2) 줄바꿈/공백을 가볍게 정리해서
    3) '게시글 본문'에 가까운 텍스트만 남긴다.
    """
    text = (post.get("text") or "").strip()
    if not text:
        return ""

    extra = post.get("extra") or {}
    if not isinstance(extra, dict):
        extra = {}
    forum = extra.get("forum") or {}
    if not isinstance(forum, dict):
        forum = {}
    comments = forum.get("comments") or []
    if isinstance(comments, list):
        for c in comments:
            if not isinstance(c, dict):
                continue
            ct = (c.get("text") or "").strip()
            if not ct:
                continue
            if ct in text:
                text = text.replace(ct, "")

    # 줄바꿈/공백 정리
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)

    # 게시글 본문 끝에 붙는 '추천 N 공유' 형태 제거
    text = re.sub(r"\s*추천\s*\d+\s*공유\s*$", "", text)

    return text.strip()


def iter_mlbpark_rows() -> Iterator[dict]:
    """
    mlbpark 원본 forum_mlbpark.jsonl → 공통 스키마로 변환.

    post 레코드:
      - id            = 글 id
      - source        = "mlbpark"
      - doc_type      = "post"
      - parent_id     = None
      - title         = 제목
      - text          = 댓글 제거된 게시글 본문
      - lang          = lang (없으면 "ko")
      - published_at  = published_at 또는 date
      - comment_*     = None

    comment 레코드:
      - id                = f"{post_id}_{idx}"
      - source            = "mlbpark"
      - doc_type          = "comment"
      - parent_id         = 원글 id
      - title             = 원글 제목
      - text              = 댓글 제거된 게시글 본문 (post와 동일)
      - lang              = 댓글 lang 또는 게시글 lang
      - published_at      = 게시글 published_at (맥락용)
      - comment_index     = 0,1,2,...
      - comment_text      = 댓글 내용
      - comment_publishedAt = 댓글 시각(있으면)
    """
    path = DATA_DIR / "forum_mlbpark.jsonl"
    for post in read_jsonl(path):
        post_id = str(post.get("id") or "").strip()
        if not post_id:
            continue

        title = (post.get("title") or "").strip()
        # 제목 정리: 끝에 붙은 ' : MLBPARK' 또는 '추천 N 공유' 제거
        if title:
            title = re.sub(r"\s*:\s*MLBPARK\s*$", "", title, flags=re.I)
            title = re.sub(r"\s*추천\s*\d+\s*공유\s*$", "", title)
            title = title.strip()
        lang = post.get("lang") or "ko"
        published_at = post.get("published_at") or post.get("date")

        extra = post.get("extra") or {}
        if not isinstance(extra, dict):
            extra = {}
        forum = extra.get("forum") or {}
        if not isinstance(forum, dict):
            forum = {}
        comments = forum.get("comments") or []
        if not isinstance(comments, list):
            comments = []

        # 디시와 동일한 컨셉: 댓글을 제거한 게시글 본문
        post_body = extract_post_body_mlbpark(post)

        # 1) 게시글 레코드
        post_row: Dict[str, object] = {}
        post_row["id"] = post_id
        post_row["source"] = "mlbpark"
        post_row["doc_type"] = "post"
        post_row["parent_id"] = None
        post_row["title"] = title
        post_row["text"] = post_body
        post_row["lang"] = lang
        post_row["published_at"] = published_at
        post_row["comment_index"] = None
        post_row["comment_text"] = None
        post_row["comment_publishedAt"] = None

        yield post_row

        # 2) 댓글 레코드들 (댓글의 text = post_body, comment_text = 댓글 본문)
        yield from collect_comment_rows(
            "mlbpark",
            post_id,
            title,
            published_at,
            lang,
            comments,
            base_text=post_body,
        )


# ---------------------------------------------------------------------------
# bobaedream
# ---------------------------------------------------------------------------


def first_paragraph(text: str | None) -> str | None:
    if not text:
        return None
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    snippet = normalized.split("\n\n", 1)[0].strip()
    return snippet or None


def iter_bobaedream_rows() -> Iterator[dict]:
    """
    bobaedream 원본 forum_bobaedream.jsonl → 공통 스키마로 변환.

    comment 레코드 id 규칙:
      - id = f"{post_id}_{idx}"
    """
    path = DATA_DIR / "forum_bobaedream.jsonl"
    for post in read_jsonl(path):
        post_id = str(post.get("id") or "").strip()
        if not post_id:
            continue

        raw_title = (post.get("title") or "").strip()
        # "제목 | 기타" 구조일 때, 앞부분만 사용
        title = raw_title.split("|", 1)[0].strip()
        lang = "ko"
        published_at = post.get("published_at") or post.get("date")
        text = first_paragraph(post.get("text"))

        # 게시글 레코드
        post_row: Dict[str, object] = {}
        post_row["id"] = post_id
        post_row["source"] = "bobaedream"
        post_row["doc_type"] = "post"
        post_row["parent_id"] = None
        post_row["title"] = title
        post_row["text"] = text
        post_row["lang"] = lang
        post_row["published_at"] = published_at
        post_row["comment_index"] = None
        post_row["comment_text"] = None
        post_row["comment_publishedAt"] = None

        yield post_row

        # 댓글 레코드도 게시글 본문을 함께 공유하도록 함 (댓글의 `text` = post text)
        comments = (
            post.get("extra", {}).get("forum", {}).get("comments")
            if isinstance(post.get("extra"), dict)
            else []
        )
        yield from collect_comment_rows(
            "bobaedream", post_id, title, published_at, lang, comments, base_text=text
        )


# ---------------------------------------------------------------------------
# ppomppu / theqoo (개별 포맷터에서 가져옴)
# ---------------------------------------------------------------------------


FORMATTERS: Dict[str, Callable[[], Iterator[dict]]] = {
    "mlbpark": iter_mlbpark_rows,
    "ppomppu": iter_ppomppu_rows,
    "bobaedream": iter_bobaedream_rows,
    "theqoo": iter_theqoo_rows,
}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Format forum_* JSONL files into a combined dataset."
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        choices=sorted(FORMATTERS.keys()),
        help="Subset of forum sources to include (default: all).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=PREPROCESSING_DIR / "new_forum_combined_comments_formatted.jsonl",
        help="Path for the combined JSONL output.",
    )
    return parser


def main(argv: List[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    selected = (
        [src for src in args.sources if src in FORMATTERS]
        if args.sources
        else list(FORMATTERS.keys())
    )

    rows_written = 0
    args.output.parent.mkdir(parents=True, exist_ok=True)

    with args.output.open("w", encoding="utf-8") as f_out:
        for source in selected:
            formatter = FORMATTERS[source]
            for row in formatter():
                f_out.write(json.dumps(row, ensure_ascii=False) + "\n")
                rows_written += 1

    print(
        json.dumps(
            {
                "output": str(args.output),
                "sources": selected,
                "rows": rows_written,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
