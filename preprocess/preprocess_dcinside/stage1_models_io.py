from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional
import json


# ---------- 데이터 모델 ----------


@dataclass
class RawComment:
    text: str
    published_at_raw: str  # 예: "11.13 17:19:44" 또는 "2024.05.13 11:16:55"
    meta: Dict[str, Any]


@dataclass
class RawPost:
    """
    forum_dcinside.jsonl 한 줄을 구조화한 형태.
    extra.forum.comments 에서 댓글 목록을 가져온다.
    """

    id: str
    source: str
    title: str
    lang: str
    published_at: str          # 게시 시각(있을 수도, 없을 수도 있음)
    crawl_fetched_at: str      # 크롤링 시각(대체값)
    raw_text: str              # 원본 text (본문 + 사이트 크롬 + 댓글 등 섞여 있음)
    comments: List[RawComment]
    extra: Dict[str, Any]


@dataclass
class FlattenedRecord:
    """
    최종 JSONL 한 줄에 대응되는 구조.

    - doc_type: "post" 또는 "comment"
    - parent_id:
        * doc_type == "post"    → None
        * doc_type == "comment" → 원글 id
    - comment_index:
        * 본문 레코드   → None
        * 댓글 레코드   → 0,1,2,...

    중요한 필드:
      - id            : post 또는 comment 식별자
      - source        : "dcinside"
      - doc_type      : "post" / "comment"
      - parent_id     : 댓글이면 원글 id, 본문이면 None
      - title         : 클린된 제목
      - lang          : 언어 (대부분 "ko")
      - published_at  : 게시글 기준 시각 (ISO 문자열)
      - text          : ✅ 항상 "댓글 제거된 게시글 본문" (post/comment 공통)
      - comment_text  : 댓글 텍스트 (doc_type="comment" 일 때만 사용)
      - comment_publishedAt : 댓글 시각 "YYYY-MM-DD HH:MM:SS"
    """

    id: str
    source: str
    doc_type: str                  # "post" or "comment"
    parent_id: Optional[str]       # 댓글이면 원글 id, 본문이면 None
    title: str                     # 클린 제목
    lang: str
    published_at: Optional[str]    # "YYYY-MM-DDTHH:MM:SS+00:00"

    # 게시글 본문 텍스트 (post/comment 모두 같은 값)
    text: Optional[str]

    comment_index: Optional[int]
    comment_text: Optional[str]
    comment_publishedAt: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        """
        최종 JSONL로 내보낼 필드들.
        (요청에 따라 combined_text는 완전히 제거)
        """
        return {
            "id": self.id,
            "source": self.source,
            "doc_type": self.doc_type,
            "parent_id": self.parent_id,
            "title": self.title,
            "lang": self.lang,
            "published_at": self.published_at,
            "text": self.text,
            "comment_index": self.comment_index,
            "comment_text": self.comment_text,
            "comment_publishedAt": self.comment_publishedAt,
        }


# ---------- 입력: 원본 JSONL → RawPost ----------


def load_raw_posts(path: str | Path) -> Iterator[RawPost]:
    """
    forum_dcinside.jsonl 을 읽어서 RawPost 시퀀스로 반환.
    extra.forum.comments 에서 댓글 목록을 가져온다.
    """
    p = Path(path)
    with p.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj: Dict[str, Any] = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"{p}:{line_no} JSON 파싱 실패: {exc}") from exc

            extra = obj.get("extra") or {}
            if not isinstance(extra, dict):
                extra = {}

            forum = extra.get("forum") or {}
            if not isinstance(forum, dict):
                forum = {}

            comments_raw = forum.get("comments") or []
            comments: List[RawComment] = []
            if isinstance(comments_raw, list):
                for c in comments_raw:
                    if not isinstance(c, dict):
                        continue
                    text = c.get("text") or ""
                    published_raw = c.get("publishedAt") or ""
                    # text, publishedAt 외 나머지 메타(작성자, id, depth 등)를 meta로 묶어둠
                    meta = {
                        k: v for k, v in c.items() if k not in ("text", "publishedAt")
                    }
                    comments.append(
                        RawComment(
                            text=str(text),
                            published_at_raw=str(published_raw),
                            meta=meta,
                        )
                    )

            crawl = obj.get("crawl") or {}
            if not isinstance(crawl, dict):
                crawl = {}
            crawl_fetched_at = str(crawl.get("fetched_at", "") or "")

            yield RawPost(
                id=str(obj.get("id", "")),
                source=str(obj.get("source", "")),
                title=str(obj.get("title", "")),
                lang=str(obj.get("lang", "")) or "ko",
                published_at=str(obj.get("published_at", "")),
                crawl_fetched_at=crawl_fetched_at,
                raw_text=str(obj.get("text", "")),
                comments=comments,
                extra=extra,
            )


# ---------- 출력: FlattenedRecord → JSONL ----------


def write_flattened_jsonl(path: str | Path, records: Iterable[FlattenedRecord]) -> None:
    """
    FlattenedRecord 이터러블을 JSONL 로 저장.
    상위 디렉토리가 없으면 자동 생성.
    """
    p = Path(path)
    if p.parent and not p.parent.exists():
        p.parent.mkdir(parents=True, exist_ok=True)

    with p.open("w", encoding="utf-8") as fw:
        for rec in records:
            fw.write(json.dumps(rec.to_dict(), ensure_ascii=False))
            fw.write("\n")
