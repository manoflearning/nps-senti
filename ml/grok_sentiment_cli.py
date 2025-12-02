# ml/grok_sentiment_cli.py
from __future__ import annotations

import argparse
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .grok_client import GrokClient #, DCINSIDE_NPS_PATTERN  # 패턴 import


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


def extract_text_and_meta(obj: Dict[str, Any]) -> TextAndMeta:
    """
    다양한 소스(dcinside, bobaedream, youtube, gdelt, etc.)를 공통 포맷으로 맞춰서
    GrokClient.analyze_sentiment 에 넘기기 위한 텍스트와 메타데이터를 만든다.

    🔥 변경 핵심:
      - doc_type == "post": title + "\n\n" + text/body/content
      - doc_type == "comment": title + "\n\n" + text/body/content + "\n\n" + comment_text
      - dcinside: 짧은 텍스트도 그대로 (최소 길이 필터 off), 키워드 없으면 사전 무관 (효율성 ↑)
      - 다른 소스: min_len=5
    """
    source = obj.get("source") or ""
    lang = obj.get("lang") or None
    published_at = obj.get("published_at") or None
    identifier = obj.get("id") or obj.get("_id") or None
    doc_type = obj.get("doc_type") or "post"  # 기본 post

    title = (obj.get("title") or "").strip()
    text_body = ""
    for key in ("text", "body", "content", "combined_text", "text_clean"):
        if key in obj:
            text_body = (obj.get(key) or "").strip()
            break

    comment_text = (obj.get("comment_text") or "").strip()

    # ✅ doc_type별 텍스트 조합
    if doc_type == "post":
        text = f"{title}\n\n{text_body}" if title and text_body else title or text_body
    elif doc_type == "comment":
        text = f"{title}\n\n{text_body}\n\n{comment_text}" if title and text_body and comment_text else f"{title}\n\n{text_body or comment_text}"
    else:  # 기타 (video, article 등)
        text = f"{title}\n\n{text_body}" if title else text_body

    if not text:
        logger.warning(
            "[WARN] id=%s 에 텍스트가 비어 있음, 제목만 사용합니다.", identifier
        )
        text = title

    # # ✅ dcinside: 키워드 없으면 사전 무관 처리 (효율성: API 호출 피함)
    # if "dcinside" in source.lower():
    #     if not DCINSIDE_NPS_PATTERN.search(text):
    #         logger.info("[INFO] dcinside지만 NPS 키워드 없음: 무관 사전 처리. text='%s'", text[:50])
    #         text = ""  # 무관 트리거
    # else:
    # 다른 소스: 짧은 텍스트 fallback
    min_len = 5
    if len(text) < min_len:
        logger.warning(
            "[WARN] id=%s 텍스트 너무 짧음 (len=%d), 무관 처리.",
            identifier,
            len(text),
        )
        text = ""

    meta: Dict[str, Any] = {
        "id": identifier,
        "source": source,
        "doc_type": doc_type,
        "lang": lang,
        "published_at": published_at,
    }

    # GDELT의 sourcecountry도 메타에 포함
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
                logger.warning(
                    "[WARN] 라인 %d JSON 파싱 실패, 스킵: %s", line_no, str(exc)
                )
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
    workers: int = 4,  # 수정: 기본 4
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
    success_count = 0  # 추가: 성공률 로깅

    if workers <= 1:
        for idx, obj in enumerate(records):
            _, merged = analyze_one(client, idx, obj)
            results[idx] = merged
            processed += 1
            success_count += 1 if merged.get("is_related") else 0  # 예시
            if processed % 10 == 0 or processed == total:
                logger.info(
                    "[INFO] 처리 완료: %d/%d (성공률: %.2f%%)",
                    processed,
                    total,
                    (success_count / processed * 100) if processed > 0 else 0,
                )
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
                    success_count += 1 if merged.get("is_related") else 0
                except Exception as exc:
                    logger.warning(
                        "[WARN] index=%d future 처리 중 예외: %s", idx, repr(exc)
                    )
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
                    logger.info(
                        "[INFO] 처리 완료: %d/%d (성공률: %.2f%%)",
                        processed,
                        total,
                        (success_count / processed * 100) if processed > 0 else 0,
                    )

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
        default=4,  # 수정: 기본 4
        help="동시 요청에 사용할 워커 수 (기본: 4)",
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