from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator


# 프로젝트 루트: .../nps-senti
BASE_DIR = Path(__file__).resolve().parents[2]

# 입력: 루트/data_crawl/forum_theqoo.jsonl
INPUT_PATH = BASE_DIR / "data_crawl" / "forum_theqoo.jsonl"

# 출력: 루트/preprocess/preprocessing_data/forum_theqoo_comments_formatted.jsonl
OUTPUT_PATH = BASE_DIR / "preprocess" / "preprocessing_data" / "forum_theqoo_comments_formatted.jsonl"

# 🔥 반드시 read_jsonl보다 위에 정의되어 있어야 함
ENCODINGS = ("utf-8", "utf-8-sig", "cp949", "euc-kr", "latin-1")


def read_jsonl(path: Path) -> Iterator[dict]:
    """JSONL 파일을 여러 인코딩 후보로 시도하면서 안전하게 읽는다."""
    last_err: Exception | None = None
    for enc in ENCODINGS:
        try:
            with path.open("r", encoding=enc) as f:
                for line_no, raw in enumerate(f, 1):
                    line = raw.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError as err:
                        raise ValueError(f"{path} line {line_no}: {err.msg}") from err
            return
        except UnicodeDecodeError as err:
            last_err = err

    if last_err:
        raise last_err
    raise UnicodeDecodeError("decode", b"", 0, 0, f"Unable to decode {path}")


def clean_title(title: str | None) -> str:
    """
    더쿠 제목 전처리:
    - "더쿠 - 제목..." / "theqoo - 제목..." 형태면 앞의 사이트명 프리픽스 제거
    """
    if not title:
        return ""
    cleaned = title.strip()
    if " - " in cleaned:
        prefix, suffix = cleaned.split(" - ", 1)
        if prefix.strip().lower().startswith(("더쿠", "theqoo")):
            cleaned = suffix.strip()
    return cleaned


def clean_text(text: str | None) -> str | None:
    """본문 텍스트를 기본적으로 정리."""
    if not text:
        return None
    cleaned = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    return cleaned or None


def iter_formatted_rows() -> Iterator[dict]:
    """
    forum_theqoo.jsonl → 공통 포맷(post-only)으로 변환.

    출력 스키마:
      - id
      - source = "theqoo"
      - doc_type = "post"
      - parent_id = None
      - title
      - text
      - lang
      - published_at
      - comment_index = None
      - comment_text = None
      - comment_publishedAt = None
    """
    for post in read_jsonl(INPUT_PATH):
        post_id = str(post.get("id") or "").strip()
        if not post_id:
            continue

        title = clean_title(post.get("title"))
        text = clean_text(post.get("text"))
        lang = post.get("lang") or "ko"
        published_at = post.get("published_at") or post.get("date")

        yield {
            "id": post_id,
            "source": "theqoo",
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


def main() -> None:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(INPUT_PATH)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    total = 0
    with OUTPUT_PATH.open("w", encoding="utf-8") as f_out:
        for row in iter_formatted_rows():
            f_out.write(json.dumps(row, ensure_ascii=False) + "\n")
            total += 1

    rel = OUTPUT_PATH.relative_to(BASE_DIR)
    print(f"Wrote {total} posts to {rel}")


if __name__ == "__main__":
    main()
