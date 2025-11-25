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
    (원본 필드들은 넉넉하게 들고 있고, 실제로 쓸지는 stage2에서 결정)
    """
    id: str
    source: str
    url: str
    lang: str

    # 상위 메타
    title_top: str
    text_top: str
    published_at_top: Optional[str]

    # extra.youtube.snippet
    snippet_published_at: Optional[str]
    snippet_title: Optional[str]
    snippet_description: Optional[str]
    channel_id: Optional[str]
    channel_title: Optional[str]

    # extra.youtube.contentDetails
    duration_raw: Optional[str]

    # extra.youtube.statistics
    view_count_raw: Optional[str]
    like_count_raw: Optional[str]
    comment_count_raw: Optional[str]

    # discovered_via
    keyword: Optional[str]

    extra: Dict[str, Any]


@dataclass
class FlattenedYoutubeVideo:
    """
    전처리 완료 후 감성분석에 바로 쓰일 최소 필드만 남긴 최종 모델.
    text_clean은 파일에 저장하지 않고, 나중에 Grok CLI에서
    title + description 으로 조립해서 사용한다.
    """
    id: str
    source: str
    lang: str

    title: str
    description: str
    published_at: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        """
        JSONL로 나갈 때는 진짜 최소 스키마만 유지.
        """
        return {
            "id": self.id,
            "source": self.source,
            "lang": self.lang,
            "title": self.title,
            "description": self.description,
            "published_at": self.published_at,
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

            content_details = yt.get("contentDetails") or {}
            if not isinstance(content_details, dict):
                content_details = {}

            statistics = yt.get("statistics") or {}
            if not isinstance(statistics, dict):
                statistics = {}

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

                snippet_published_at=str(snippet.get("publishedAt") or "") or None,
                snippet_title=str(snippet.get("title") or "") or None,
                snippet_description=str(snippet.get("description") or "") or None,
                channel_id=str(snippet.get("channelId") or "") or None,
                channel_title=str(snippet.get("channelTitle") or "") or None,

                duration_raw=str(content_details.get("duration") or "") or None,

                view_count_raw=str(statistics.get("viewCount") or "") or None,
                like_count_raw=str(statistics.get("likeCount") or "") or None,
                comment_count_raw=str(statistics.get("commentCount") or "") or None,

                keyword=str(discovered.get("keyword") or "") or None,
                extra=extra,
            )


# ---------- 출력: Flattened → JSONL ----------

def write_flattened_jsonl(path: str | Path, records: Iterable[FlattenedYoutubeVideo]) -> None:
    """
    FlattenedYoutubeVideo 이터러블을 JSONL 로 저장.
    상위 디렉터리가 없으면 자동 생성.
    """
    p = Path(path)
    if p.parent and not p.parent.exists():
        p.parent.mkdir(parents=True, exist_ok=True)

    with p.open("w", encoding="utf-8") as fw:
        for rec in records:
            fw.write(json.dumps(rec.to_dict(), ensure_ascii=False))
            fw.write("\n")
