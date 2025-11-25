from __future__ import annotations

import argparse
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .grok_client import GrokClient


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
)


# ---------- 텍스트 & 메타 추출 ----------

@dataclass
class TextAndMeta:
    text: str
    meta: Dict[str, Any]


from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import logging

logger = logging.getLogger(__name__)


@dataclass
class TextAndMeta:
    text: str
    meta: Dict[str, Any]


def extract_text_and_meta(obj: Dict[str, Any]) -> TextAndMeta:
    """
    다양한 소스(dcinside, mlbpark, bobaedream, youtube, gdelt, etc.)를 공통 포맷으로 맞춰서
    GrokClient.analyze_sentiment 에 넘기기 위한 텍스트와 메타데이터를 만든다.

    ✅ 규칙 정리
    - dcinside, mlbpark:
      * doc_type == "post"    → 제목만 사용
      * doc_type == "comment" → 제목 + 댓글(comment_text) 사용
    - 그 외 소스는 기존 combined_text / text_clean / text / title 우선순위 유지
    """
    source = obj.get("source") or ""
    lang = obj.get("lang") or None
    published_at = obj.get("published_at") or None
    identifier = obj.get("id") or obj.get("_id") or None

    # doc_type 추론
    doc_type = obj.get("doc_type")
    if not doc_type:
        if source == "youtube":
            doc_type = "video"
        elif source in ("naver_news", "news", "gdelt"):
            doc_type = "article"
        else:
            doc_type = "post"

    # ---------- 1) dcinside / mlbpark 전용 규칙 ----------
    if source in ("dcinside", "mlbpark") and doc_type in ("post", "comment"):
        title = (obj.get("title") or "").strip()
        comment_text = (obj.get("comment_text") or "").strip()

        text_special = ""
        if doc_type == "post":
            # 게시글: 제목만 사용
            text_special = title
        elif doc_type == "comment":
            # 댓글: 제목 + 댓글
            if title and comment_text:
                text_special = f"{title}\n\n{comment_text}"
            elif comment_text:
                text_special = comment_text
            else:
                text_special = title

        if text_special:
            meta: Dict[str, Any] = {
                "id": identifier,
                "source": source,
                "doc_type": doc_type,
                "lang": lang,
                "published_at": published_at,
            }
            return TextAndMeta(text=text_special, meta=meta)
        # 제목/댓글이 둘 다 비어 있는 극단적 케이스면 아래 일반 로직으로 폴백

    # ---------- 2) 일반 포맷(기존 로직) ----------

    text_candidates: List[Optional[str]] = []

    # 2-1) 포럼류(보배 등): combined_text
    if "combined_text" in obj:
        text_candidates.append(obj.get("combined_text"))

    # 2-2) 이전 버전 전처리: text_clean
    if "text_clean" in obj:
        text_candidates.append(obj.get("text_clean"))

    # 2-3) 유튜브 (minimal): title + description
    if source == "youtube":
        title = (obj.get("title") or "").strip()
        desc = (obj.get("description") or "").strip()
        if title and desc:
            text_candidates.append(f"{title}\n\n{desc}")
        elif title:
            text_candidates.append(title)

    # 2-4) GDELT 기사: title + text
    if source == "gdelt":
        title = (obj.get("title") or "").strip()
        body = (obj.get("text") or "").strip()
        if title and body:
            text_candidates.append(f"{title}\n\n{body}")
        elif body:
            text_candidates.append(body)

    # 2-5) 댓글만 있는 경우: comment_text
    if "comment_text" in obj:
        text_candidates.append(obj.get("comment_text"))

    # 2-6) 일반 텍스트 필드: text, body, content
    for key in ("text", "body", "content"):
        if key in obj:
            text_candidates.append(obj.get(key))

    # 2-7) 그래도 없으면: title만이라도
    if "title" in obj:
        text_candidates.append(obj.get("title"))

    text = ""
    for cand in text_candidates:
        if cand and isinstance(cand, str) and cand.strip():
            text = cand.strip()
            break

    if not text:
        logger.warning("[WARN] id=%s 에 텍스트가 비어 있음, 제목만 사용합니다.", identifier)
        title_fallback = (obj.get("title") or "").strip()
        text = title_fallback

    meta: Dict[str, Any] = {
        "id": identifier,
        "source": source,
        "doc_type": doc_type,
        "lang": lang,
        "published_at": published_at,
    }

    # GDELT의 sourcecountry 메타 유지
    if "sourcecountry" in obj and obj.get("sourcecountry"):
        meta["sourcecountry"] = obj.get("sourcecountry")

    return TextAndMeta(text=text, meta=meta)

# ---------- JSONL 입출력 ----------

def read_jsonl(path: str | Path, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"입력 파일을 찾을 수 없습니다: {p}")

    records: List[Dict[str, Any]] = []
    with p.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                logger.warning("[WARN] 라인 %d JSON 파싱 실패, 스킵: %s", line_no, str(exc))
                continue
            records.append(obj)
            if limit is not None and len(records) >= limit:
                break

    logger.info("[INFO] 입력에서 %d개 레코드 로드", len(records))
    return records


def write_jsonl(path: str | Path, records: List[Dict[str, Any]]) -> None:
    p = Path(path)
    if p.parent and not p.parent.exists():
        p.parent.mkdir(parents=True, exist_ok=True)

    with p.open("w", encoding="utf-8") as f:
        for obj in records:
            f.write(json.dumps(obj, ensure_ascii=False))
            f.write("\n")


# ---------- 개별 레코드 분석 ----------

def analyze_one(
    client: GrokClient,
    index: int,
    obj: Dict[str, Any],
) -> Tuple[int, Dict[str, Any]]:
    """
    한 레코드에 대해 Grok 감성분석을 수행하고,
    원본 + sentiment 필드가 합쳐진 dict를 반환한다.
    """
    try:
        tm = extract_text_and_meta(obj)
        result = client.analyze_sentiment(tm.text, tm.meta)
    except Exception as exc:
        logger.warning("[WARN] 레코드 index=%d 분석 실패: %s", index, repr(exc))
        # 안전한 fallback (규칙에 맞게 무관 처리)
        result = {
            "is_related": False,
            "negative": 0.0,
            "neutral": 0.0,
            "positive": 0.0,
            "label": "무관",
            "explanation": "국민연금과 관련 없음",
        }

    merged = {**obj, **result}
    return index, merged


# ---------- 전체 파일 처리 ----------

def process_file(
    input_path: str | Path,
    output_path: str | Path,
    limit: Optional[int] = None,
    workers: int = 1,
) -> None:
    records = read_jsonl(input_path, limit=limit)
    total = len(records)
    if total == 0:
        logger.warning("[WARN] 입력에 유효한 레코드가 없습니다.")
        return

    logger.info("[INFO] 총 %d개 레코드 처리 예정, workers=%d", total, workers)

    client = GrokClient()

    results: Dict[int, Dict[str, Any]] = {}
    processed = 0

    if workers <= 1:
        for idx, obj in enumerate(records):
            _, merged = analyze_one(client, idx, obj)
            results[idx] = merged
            processed += 1
            if processed % 10 == 0 or processed == total:
                logger.info("[INFO] 처리 완료: %d/%d", processed, total)
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(analyze_one, client, idx, obj): idx
                for idx, obj in enumerate(records)
            }

            for future in as_completed(futures):
                idx = futures[future]
                try:
                    _, merged = future.result()
                except Exception as exc:
                    logger.warning("[WARN] index=%d future 처리 중 예외: %s", idx, repr(exc))
                    merged = {
                        **records[idx],
                        "is_related": False,
                        "negative": 0.0,
                        "neutral": 0.0,
                        "positive": 0.0,
                        "label": "무관",
                        "explanation": "국민연금과 관련 없음",
                    }
                results[idx] = merged
                processed += 1
                if processed % 10 == 0 or processed == total:
                    logger.info("[INFO] 처리 완료: %d/%d", processed, total)

    # 인덱스 순서대로 정렬해서 출력
    ordered_records = [results[i] for i in range(total)]
    write_jsonl(output_path, ordered_records)
    logger.info("[INFO] 모든 작업 완료. 결과: %s", Path(output_path).resolve())


# ---------- CLI 엔트리포인트 ----------

def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="국민연금 온라인 여론(댓글/게시글/영상)에 대한 Grok-4-fast 감성분석 CLI"
    )
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="입력 JSONL 경로 (예: preprocess/preprocessing_data/youtube_preprocessed_minimal.jsonl)",
    )
    parser.add_argument(
        "--output",
        "-o",
        required=True,
        help="출력 JSONL 경로 (예: sentiment_output_data/youtube_sentiment.jsonl)",
    )
    parser.add_argument(
        "--limit",
        "-n",
        type=int,
        default=None,
        help="처리할 최대 레코드 수 (디버그용, 기본: 전체)",
    )
    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=1,
        help="동시 요청에 사용할 워커 수 (기본: 1)",
    )

    args = parser.parse_args(argv)

    process_file(
        input_path=args.input,
        output_path=args.output,
        limit=args.limit,
        workers=args.workers,
    )


if __name__ == "__main__":
    main()