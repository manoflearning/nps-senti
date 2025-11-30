# preprocess/preprocess_gdelt/stage3_cli.py
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import List, Optional, Set

from .stage1_models_io import (
    load_raw_gdelt,
    write_flattened_jsonl,
    FlattenedGdeltArticle,
)
from .stage2_transform import flatten_article, deduplicate_records


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
)


def preprocess_gdelt(
    input_path: str | Path,
    output_path: str | Path,
    min_length: int = 0,
    max_length: Optional[int] = None,
    lang_filter: Optional[List[str]] = None,
) -> None:
    """
    GDELT 원본 JSONL → 전처리 JSONL.

    - 깨진 JSON 라인 스킵
    - 텍스트 클리닝
    - (본문 text) 길이 기준 필터링
    - 언어 필터 (ko/en 등)
    - 중복 제거 (lang+title+date 기준)
    - 최종 출력: id, source, lang, title, text, published_at
    """
    in_path = Path(input_path).resolve()
    out_path = Path(output_path).resolve()

    if not in_path.exists():
        raise FileNotFoundError(f"입력 파일을 찾을 수 없습니다: {in_path}")

    lang_set: Optional[Set[str]] = None
    if lang_filter:
        lang_set = {s.strip() for s in lang_filter if s.strip()}
        if not lang_set:
            lang_set = None

    logger.info("[INFO] GDELT 입력: %s", in_path)
    logger.info("[INFO] GDELT 출력: %s", out_path)
    logger.info(
        "[INFO] min_length=%s, max_length=%s, lang_filter=%s",
        min_length,
        max_length,
        lang_set,
    )

    raw_iter = load_raw_gdelt(in_path)

    flattened: List[FlattenedGdeltArticle] = []
    total_raw = 0
    total_used = 0

    for raw in raw_iter:
        total_raw += 1

        if lang_set is not None:
            lang = (raw.lang or "").strip()
            if lang not in lang_set:
                continue

        rec = flatten_article(raw, min_length=min_length, max_length=max_length)
        if rec is None:
            continue

        flattened.append(rec)
        total_used += 1

    logger.info("[INFO] 원본 %d개 중 필터링 후 %d개 남음", total_raw, total_used)

    deduped = deduplicate_records(flattened)
    logger.info("[INFO] 중복 제거 후 최종 %d개", len(deduped))

    write_flattened_jsonl(out_path, deduped)
    logger.info("[INFO] GDELT 전처리 완료: %s", out_path)


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="GDELT 뉴스 데이터 전처리 (국민연금 프로젝트용)"
    )
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="입력 gdelt.jsonl 경로",
    )
    parser.add_argument(
        "--output",
        "-o",
        required=True,
        help="출력 JSONL 경로 (예: preprocess/preprocessing_data/gdelt_clean.jsonl)",
    )
    parser.add_argument(
        "--min-length",
        type=int,
        default=0,
        help="본문(text) 최소 길이 필터 (문자 수 기준, 기본=0: 필터 없음)",
    )
    parser.add_argument(
        "--max-length",
        type=int,
        default=None,
        help="본문(text) 최대 길이 필터 (문자 수 기준, 기본=None: 필터 없음)",
    )
    parser.add_argument(
        "--lang-filter",
        type=str,
        default=None,
        help='언어 필터 (예: "ko,en" → ko/en만 사용, 기본=None: 전체 사용)',
    )

    args = parser.parse_args(argv)

    lang_list: Optional[List[str]] = None
    if args.lang_filter:
        lang_list = [s.strip() for s in args.lang_filter.split(",") if s.strip()]

    preprocess_gdelt(
        input_path=args.input,
        output_path=args.output,
        min_length=args.min_length,
        max_length=args.max_length,
        lang_filter=lang_list,
    )


if __name__ == "__main__":
    main()
