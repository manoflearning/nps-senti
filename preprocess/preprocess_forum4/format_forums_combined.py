from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Callable, Iterator, List, Dict, Iterable

try:
    # ppomppu/theqoo 전처리 결과 제너레이터 가져오기
    from .format_ppomppu import iter_formatted_rows as iter_ppomppu_rows  # type: ignore
    from .format_theqoo import iter_formatted_rows as iter_theqoo_rows  # type: ignore
except ImportError:  # 직접 실행할 때 fallback
    from format_ppomppu import iter_formatted_rows as iter_ppomppu_rows  # type: ignore
    from format_theqoo import iter_formatted_rows as iter_theqoo_rows  # type: ignore


# 🔥 프로젝트 루트: .../nps-senti
# __file__ = preprocess/preprocess_forum4/format_forums_combined.py
# parents[0] = preprocess_forum4, parents[1] = preprocess, parents[2] = nps-senti
BASE_DIR = Path(__file__).resolve().parents[2]

# 입력 원본 디렉토리: 루트/data_crawl
DATA_DIR = BASE_DIR / "data_crawl"

# 출력 디렉토리: 루트/preprocess/preprocessing_data
PREPROCESSING_DIR = BASE_DIR / "preprocess" / "preprocessing_data"


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
) -> Iterator[dict]:
    """
    forum.extra.forum.comments 구조를 공통 스키마(comment)로 변환.
    """
    if not comments:
        return
    for idx, comment in enumerate(comments):
        comment_id = str(comment.get("id") or f"{post_id}_comment_{idx}")
        comment_text = (comment.get("text") or "").strip()
        if not comment_text:
            continue
        yield {
            "id": comment_id,
            "source": source,
            "doc_type": "comment",
            "parent_id": post_id,
            "title": title,
            "lang": (comment.get("lang") or base_lang),
            "published_at": published_at,
            "comment_index": idx,
            "comment_text": comment_text,
            "comment_publishedAt": comment.get("publishedAt"),
        }


# ---------- mlbpark ----------

def iter_mlbpark_rows() -> Iterator[dict]:
    path = DATA_DIR / "forum_mlbpark.jsonl"
    for post in read_jsonl(path):
        post_id = str(post.get("id") or "").strip()
        if not post_id:
            continue
        title = (post.get("title") or "").strip()
        lang = post.get("lang") or "ko"
        published_at = post.get("published_at") or post.get("date")

        # 게시글 레코드
        yield {
            "id": post_id,
            "source": "mlbpark",
            "doc_type": "post",
            "parent_id": None,
            "title": title,
            "lang": lang,
            "published_at": published_at,
            "comment_index": None,
            "comment_text": None,
            "comment_publishedAt": None,
        }

        # 댓글 레코드들
        comments = (
            post.get("extra", {})
            .get("forum", {})
            .get("comments")
            if isinstance(post.get("extra"), dict)
            else []
        )
        yield from collect_comment_rows(
            "mlbpark", post_id, title, published_at, lang, comments
        )


# ---------- bobaedream ----------

def first_paragraph(text: str | None) -> str | None:
    if not text:
        return None
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    snippet = normalized.split("\n\n", 1)[0].strip()
    return snippet or None


def iter_bobaedream_rows() -> Iterator[dict]:
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
        yield {
            "id": post_id,
            "source": "bobaedream",
            "doc_type": "post",
            "parent_id": None,
            "title": title,
            "text": text,
            "lang": lang,
            "published_at": published_at,
            "comment_index": None,
            "comment_text": None,
            "comment_publishedAt": None,
        }

        # 댓글 레코드들
        comments = (
            post.get("extra", {})
            .get("forum", {})
            .get("comments")
            if isinstance(post.get("extra"), dict)
            else []
        )
        yield from collect_comment_rows(
            "bobaedream", post_id, title, published_at, lang, comments
        )


# ---------- dcinside (이미 flatten 된 파일) ----------

def iter_dcinside_rows() -> Iterator[dict]:
    path = DATA_DIR / "forum_dcinside_post_plus_comments_combined_with_year.jsonl"
    for entry in read_jsonl(path):
        post_id = str(entry.get("id") or "").strip()
        if not post_id:
            continue
        lang = entry.get("lang") or "ko"
        published_at = entry.get("published_at") or entry.get("date")
        comment_index = entry.get("comment_index")

        # 게시글 행
        if comment_index is None:
            yield {
                "id": post_id,
                "source": "dcinside",
                "doc_type": "post",
                "parent_id": None,
                "title": (entry.get("title") or "").strip(),
                "lang": lang,
                "published_at": published_at,
                "comment_index": None,
                "comment_text": None,
                "comment_publishedAt": None,
            }
        else:
            # 댓글 행
            comment_text = (entry.get("comment_text") or "").strip()
            if not comment_text:
                continue
            comment_id = str(entry.get("comment_id") or f"{post_id}_comment_{comment_index}")
            yield {
                "id": comment_id,
                "source": "dcinside",
                "doc_type": "comment",
                "parent_id": post_id,
                "title": (entry.get("title") or "").strip(),
                "lang": lang,
                "published_at": published_at,
                "comment_index": comment_index,
                "comment_text": comment_text,
                "comment_publishedAt": entry.get("comment_publishedAt")
                or entry.get("comment_publishedAt_raw"),
            }


# ---------- ppomppu / theqoo (개별 포맷터에서 가져옴) ----------

FORMATTERS: Dict[str, Callable[[], Iterator[dict]]] = {
    "mlbpark": iter_mlbpark_rows,
    "ppomppu": iter_ppomppu_rows,
    "bobaedream": iter_bobaedream_rows,
    "theqoo": iter_theqoo_rows,
    "dcinside": iter_dcinside_rows,
}


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
