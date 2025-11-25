"""Normalize forum_theqoo.jsonl into new_forum_theqoo_comments_formatted.jsonl."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_PATH = BASE_DIR / "data_crawl" / "forum_theqoo.jsonl"
OUTPUT_PATH = BASE_DIR / "data_crawl" / "new_forum_theqoo_comments_formatted.jsonl"
ENCODINGS = ("utf-8", "utf-8-sig", "cp949", "euc-kr", "latin-1")


def read_jsonl(path: Path) -> Iterator[dict]:
    """Yield JSON objects with basic encoding fallbacks."""
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
    """Remove the leading '더쿠 -' style prefix from titles."""
    if not title:
        return ""
    cleaned = title.strip()
    if " - " in cleaned:
        prefix, suffix = cleaned.split(" - ", 1)
        if prefix.strip().lower().startswith(("더쿠", "theqoo")):
            cleaned = suffix.strip()
    return cleaned


def clean_text(text: str | None) -> str | None:
    if not text:
        return None
    cleaned = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    return cleaned or None


def iter_formatted_rows() -> Iterator[dict]:
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
