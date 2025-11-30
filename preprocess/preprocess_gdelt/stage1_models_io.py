# preprocess/preprocess_gdelt/stage1_models_io.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Optional
import json
import logging

logger = logging.getLogger(__name__)


# ---------- 데이터 모델 ----------


@dataclass
class RawGdeltArticle:
    """
    gdelt.jsonl 한 줄을 구조화한 원본 모델.
    """

    id: str
    source: str
    lang: str

    title: str
    text: str

    published_at: Optional[str]

    # 보조 정보
    seendate: Optional[str]
    url: Optional[str]
    domain: Optional[str]
    sourcecountry: Optional[str]

    discovered_via: Dict[str, Any]
    extra: Dict[str, Any]


@dataclass
class FlattenedGdeltArticle:
    """
    전처리 완료 후 감성분석에 바로 쓰일 최종 모델.

    최종 JSONL 필드:
      - id
      - source
      - lang
      - title
      - text
      - published_at
    """

    id: str
    source: str
    lang: str

    title: str
    text: str
    published_at: Optional[str]

    # dedup에서만 쓰고 JSONL에는 내보내지 않을 필드
    url: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "source": self.source,
            "lang": self.lang,
            "title": self.title,
            "text": self.text,
            "published_at": self.published_at,
        }


# ---------- 입력: 안전 JSON 로더 ----------


def load_raw_gdelt(path: str | Path) -> Iterator[RawGdeltArticle]:
    """
    gdelt.jsonl 을 한 줄씩 읽으면서 JSONDecodeError 방어하며 RawGdeltArticle로 변환.
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
                    "[WARN] GDELT 라인 %d JSON 파싱 실패, 스킵: %s", line_no, str(exc)
                )
                continue

            extra = obj.get("extra") or {}
            if not isinstance(extra, dict):
                extra = {}

            gd = extra.get("gdelt") or {}
            if not isinstance(gd, dict):
                gd = {}

            discovered = obj.get("discovered_via") or {}
            if not isinstance(discovered, dict):
                discovered = {}

            _id = str(obj.get("id", "") or "")
            source = str(obj.get("source", "") or "gdelt")
            lang = str(obj.get("lang", "") or "en")

            title = str(obj.get("title", "") or "")
            text = str(obj.get("text", "") or "")

            published_at = str(obj.get("published_at") or "") or None

            seendate = None
            dv_seendate = discovered.get("seendate")
            if dv_seendate:
                seendate = str(dv_seendate)
            else:
                gd_seendate = gd.get("seendate")
                if gd_seendate:
                    seendate = str(gd_seendate)

            domain = str(gd.get("domain") or "") or None
            sourcecountry = str(gd.get("sourcecountry") or "") or None

            url = obj.get("url") or gd.get("url") or gd.get("sourceurl")
            url_str: Optional[str] = str(url) if url else None

            yield RawGdeltArticle(
                id=_id,
                source=source,
                lang=lang,
                title=title,
                text=text,
                published_at=published_at,
                seendate=seendate,
                url=url_str,
                domain=domain,
                sourcecountry=sourcecountry,
                discovered_via=discovered,
                extra=extra,
            )


# ---------- 출력: Flattened → JSONL ----------


def write_flattened_jsonl(
    path: str | Path, records: Iterable[FlattenedGdeltArticle]
) -> None:
    """
    FlattenedGdeltArticle 이터러블을 JSONL 로 저장.
    상위 디렉터리가 없으면 자동 생성.
    """
    p = Path(path)
    if p.parent and not p.parent.exists():
        p.parent.mkdir(parents=True, exist_ok=True)

    with p.open("w", encoding="utf-8") as fw:
        for rec in records:
            fw.write(json.dumps(rec.to_dict(), ensure_ascii=False))
            fw.write("\n")
