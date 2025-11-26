# preprocess/preprocess_forum4/format_ppomppu.py
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Iterator

BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_PATH = BASE_DIR / "data_crawl" / "forum_ppomppu.jsonl"
# 🔥 출력 경로를 preprocessing_data 로 변경
OUTPUT_PATH = BASE_DIR / "preprocess" / "preprocessing_data" / "forum_ppomppu_comments_formatted.jsonl"
ENCODINGS = ("utf-8", "utf-8-sig", "cp949", "euc-kr", "latin-1")
STOP_EXACT = {
    "목록보기",
    "목록으로",
    "이전글",
    "다음글",
    "첨부파일",
    "댓글쓰기",
    "댓글 쓰기",
    "글쓰기",
    "NO",
    "YES",
}
STOP_PREFIXES = (
    "추천하기",
    "다른의견",
    "신고",
    "댓글주소복사",
    "레벨",
    "알림",
    "짤방",
    "사진",
    "익명요구",
    "이미지추가",
    "코멘트",
    "갤러리",
)
PUNCT_ONLY = {"|", "△", "▽", "▶", "▲", "▼"}
EVENT_KEYWORDS = ("이벤트", "쿠폰", "체험단", "핫딜", "세일", "특가")


def read_jsonl(path: Path) -> Iterator[dict]:
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


def start_index_after_metadata(lines: list[str], title: str) -> int:
    title = (title or "").strip()
    idx = 0
    if title:
        for i, line in enumerate(lines):
            if line.strip() == title:
                idx = i + 1
                break
    for j in range(idx, len(lines)):
        token = lines[j]
        if "조회" in token:
            return j + 1
    return idx


def matches_ui_line(line: str) -> bool:
    if line in STOP_EXACT:
        return True
    return any(line.startswith(prefix) for prefix in STOP_PREFIXES)


def normalize_blank_lines(lines: list[str]) -> list[str]:
    normalized: list[str] = []
    for line in lines:
        if not line and normalized and not normalized[-1]:
            continue
        normalized.append(line)
    return normalized


def collect_comment_phrases(comments: list[dict]) -> set[str]:
    phrases: set[str] = set()
    for comment in comments:
        text = (comment.get("text") or "").strip()
        if not text:
            continue
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        for fragment in text.split("\n"):
            fragment = fragment.strip().rstrip("|").strip()
            if fragment:
                phrases.add(fragment)
    return phrases


def clean_post_text(raw_text: str, title: str, comment_phrases: set[str]) -> str:
    if not raw_text:
        return ""
    normalized = raw_text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [ln.strip() for ln in normalized.split("\n")]
    start_idx = start_index_after_metadata(lines, title)
    body: list[str] = []
    for line in lines[start_idx:]:
        stripped = line.strip()
        stripped = stripped.rstrip("|").strip()
        if not stripped:
            if body and body[-1]:
                body.append("")
            continue
        if stripped in PUNCT_ONLY:
            continue
        if stripped.isdigit():
            if body:
                break
            continue
        if matches_ui_line(stripped):
            if body:
                break
            continue
        # Only stop when we encounter a line that matches a comment phrase
        # if we've already collected some body text. This avoids dropping
        # the entire post when the first line is also present in comments.
        if comment_phrases and stripped in comment_phrases and body:
            break
        body.append(stripped)
    cleaned = "\n".join(normalize_blank_lines(body)).strip()
    return cleaned


def parse_post_datetime(raw: str | None) -> datetime | None:
    if not raw:
        return None
    candidate = raw.strip()
    if not candidate:
        return None
    candidate = candidate.replace("Z", "")
    try:
        return datetime.fromisoformat(candidate)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                return datetime.strptime(candidate, fmt)
            except ValueError:
                continue
    return None


def normalize_comment_timestamp(post_dt: datetime | None, comment_time: str | None) -> str | None:
    if not comment_time:
        return None
    comment_time = comment_time.strip()
    if not comment_time:
        return None
    if post_dt and ":" in comment_time and " " not in comment_time:
        return f"{post_dt.date()} {comment_time}"
    return comment_time


def should_skip_post(title: str) -> bool:
    title = (title or "").lower()
    for keyword in EVENT_KEYWORDS:
        if keyword.lower() in title:
            return True
    return False


def iter_formatted_rows() -> Iterator[dict]:
    for post in read_jsonl(INPUT_PATH):
        post_id = str(post.get("id") or "").strip()
        if not post_id:
            continue
        title = (post.get("title") or "").strip()
        if should_skip_post(title):
            continue
        lang = post.get("lang") or "ko"
        published_at = post.get("published_at") or post.get("date")
        post_dt = parse_post_datetime(published_at)
        comments_raw = (
            post.get("extra", {})
            .get("forum", {})
            .get("comments")
            or []
        )
        comment_phrases = collect_comment_phrases(comments_raw)
        body = clean_post_text(post.get("text") or "", title, comment_phrases)
        yield {
            "id": post_id,
            "source": "ppomppu",
            "doc_type": "post",
            "parent_id": None,
            "title": title,
            "text": body or None,
            "lang": lang,
            "published_at": published_at,
            "comment_index": None,
            "comment_text": None,
            "comment_publishedAt": None,
        }

        for idx, comment in enumerate(comments_raw):
            comment_id = str(comment.get("id") or f"{post_id}_comment_{idx}")
            comment_text = (comment.get("text") or "").strip() or None
            comment_time = normalize_comment_timestamp(post_dt, comment.get("publishedAt"))
            yield {
                "id": comment_id,
                "source": "ppomppu",
                "doc_type": "comment",
                "parent_id": post_id,
                "title": title,
                "text": body or None,
                "lang": lang,
                "published_at": published_at,
                "comment_index": idx,
                # `text` for comment records should mirror the post body
                
                "comment_text": comment_text,
                "comment_publishedAt": comment_time,
            }


def main() -> None:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(INPUT_PATH)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    total = 0
    posts = 0
    comments = 0
    with OUTPUT_PATH.open("w", encoding="utf-8") as f_out:
        for row in iter_formatted_rows():
            f_out.write(json.dumps(row, ensure_ascii=False) + "\n")
            total += 1
            if row["doc_type"] == "post":
                posts += 1
            else:
                comments += 1
    rel = OUTPUT_PATH.relative_to(BASE_DIR)
    print(f"Wrote {posts} posts and {comments} comments ({total} rows) to {rel}")


if __name__ == "__main__":
    main()
