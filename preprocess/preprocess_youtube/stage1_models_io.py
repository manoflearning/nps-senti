# preprocess/preprocess_youtube/stage1_models_io.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Optional
import json
import logging

logger = logging.getLogger(__name__)


# ---------- 데이터 모델 ----------


@dataclass
class RawYoutubeVideo:
    """
    youtube.jsonl 한 줄을 구조화한 원본 모델.

    - 원본 필드는 넉넉하게 들고 있고
    - 실제 어떤 컬럼을 쓸지는 stage2에서 결정한다.
    """

    id: str
    source: str
    url: str
    lang: str

    # 상위 레벨 필드
    title_top: str
    text_top: str
    published_at_top: Optional[str]

    # extra.youtube.snippet
    snippet_title: Optional[str]
    snippet_description: Optional[str]
    snippet_published_at: Optional[str]

    # discovered_via
    keyword: Optional[str]

    # 이 외 전체 extra (댓글 포함)
    extra: Dict[str, Any]


@dataclass
class FlattenedYoutubeComment:
    """
    최종 전처리 후, 한 줄 = "영상 + 댓글 1개" 스키마.

    스키마:
      - id
      - source
      - lang
      - title
      - text
      - published_at
      - comment_index
      - comment_text
      - comment_publishedAt

    **중요**
    - 댓글이 있는 경우:
        comment_index = 0, 1, 2, ...
        comment_text = 실제 댓글
        comment_publishedAt = 댓글 시각
    - 댓글이 전혀 없는 영상의 경우:
        comment_index = None
        comment_text = None
        comment_publishedAt = None
    """

    id: str
    source: str
    lang: str

    title: str
    text: str
    published_at: Optional[str]

    comment_index: Optional[int]
    comment_text: Optional[str]
    comment_publishedAt: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "source": self.source,
            "lang": self.lang,
            "title": self.title,
            "text": self.text,
            "published_at": self.published_at,
            "comment_index": self.comment_index,
            "comment_text": self.comment_text,
            "comment_publishedAt": self.comment_publishedAt,
        }


# ---------- 입력: 안전 JSON 로더 ----------


def load_raw_youtube(path: str | Path) -> Iterator[RawYoutubeVideo]:
    """
    youtube.jsonl 을 한 줄씩 읽으면서 JSONDecodeError 방어하며 RawYoutubeVideo로 변환.
    깨진 줄/비어 있는 줄은 경고 로그만 남기고 스킵한다.
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
                logger.warning(
                    "[WARN] YouTube 라인 %d JSON 파싱 실패, 스킵: %s", line_no, str(exc)
                )
                continue

            extra = obj.get("extra") or {}
            if not isinstance(extra, dict):
                extra = {}

            yt = extra.get("youtube") or {}
            if not isinstance(yt, dict):
                yt = {}

            snippet = yt.get("snippet") or {}
            if not isinstance(snippet, dict):
                snippet = {}

            discovered = obj.get("discovered_via") or {}
            if not isinstance(discovered, dict):
                discovered = {}

            yield RawYoutubeVideo(
                id=str(obj.get("id", "")),
                source=str(obj.get("source", "")) or "youtube",
                url=str(obj.get("url", "")),
                lang=str(obj.get("lang", "")) or "ko",
                title_top=str(obj.get("title", "")),
                text_top=str(obj.get("text", "")),
                published_at_top=str(obj.get("published_at", "")) or None,
                snippet_title=str(snippet.get("title") or "") or None,
                snippet_description=str(snippet.get("description") or "") or None,
                snippet_published_at=str(snippet.get("publishedAt") or "") or None,
                keyword=str(discovered.get("keyword") or "") or None,
                extra=extra,
            )


# ---------- 출력: Flattened → JSONL ----------


def write_flattened_jsonl(
    path: str | Path, records: Iterable[FlattenedYoutubeComment]
) -> None:
    """
    FlattenedYoutubeComment 이터러블을 JSONL 로 저장.
    상위 디렉터리가 없으면 자동 생성.
    """
    p = Path(path)
    if p.parent and not p.parent.exists():
        p.parent.mkdir(parents=True, exist_ok=True)

    with p.open("w", encoding="utf-8") as fw:
        for rec in records:
            fw.write(json.dumps(rec.to_dict(), ensure_ascii=False))
            fw.write("\n")
