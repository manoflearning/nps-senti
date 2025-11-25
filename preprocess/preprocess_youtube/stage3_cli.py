# preprocess/preprocess_youtube/stage3_cli.py
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import List, Optional

from .stage1_models_io import load_raw_youtube, write_flattened_jsonl, FlattenedYoutubeVideo
from .stage2_transform import flatten_video, deduplicate_records


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
)


def preprocess_youtube(
    input_path: str | Path,
    output_path: str | Path,
    min_length: int = 0,
    max_length: Optional[int] = None,
    lang_filter: Optional[List[str]] = None,
) -> None:
    """
    youtube.jsonl → (id/source/url/lang/title/description/text_clean/published_at/keyword/카운트/채널/길이)
    형태의 전처리 JSONL 생성.
    """
    # .../nps-senti-crawl/preprocess/preprocess_youtube/stage3_cli.py
    # parents[2] → .../nps-senti-crawl (repo root)
    repo_root = Path(__file__).resolve().parents[2]
    base_output_dir = repo_root / "preprocess" / "preprocessing_data"

    # ----- input 경로 처리 -----
    in_path = Path(input_path)
    if not in_path.is_absolute():
        in_path = (repo_root / in_path).resolve()

    if not in_path.exists():
        raise FileNotFoundError(f"입력 파일을 찾을 수 없습니다: {in_path}")

    # ----- output 경로 처리 -----
    out_arg = Path(output_path)
    if out_arg.is_absolute():
        out_path = out_arg
    else:
        if out_arg.parent == Path("."):
            # 파일명만 온 경우 → preprocessing_data 아래에 생성
            out_path = base_output_dir / out_arg.name
        else:
            out_path = (repo_root / out_arg).resolve()

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # 언어 필터 준비
    lang_set: Optional[set] = None
    if lang_filter:
        lang_set = {lng.strip() for lng in lang_filter if lng.strip()}

    # ----- 로드 + 전처리 -----
    flattened: List[FlattenedYoutubeVideo] = []
    total = 0
    kept = 0

    for raw in load_raw_youtube(in_path):
        total += 1

        # 언어 필터 (예: lang_filter=["ko"])
        if lang_set is not None and raw.lang not in lang_set:
            continue

        rec = flatten_video(
            raw,
            min_length=min_length,
            max_length=max_length,
        )
        if rec is None:
            continue

        flattened.append(rec)
        kept += 1

    logger.info("[INFO] YouTube 원본 %d개 중 %d개 유지 (언어/길이 필터 적용)", total, kept)

    # ----- 중복 제거 -----
    deduped = deduplicate_records(flattened)

    # ----- 저장 -----
    write_flattened_jsonl(out_path, deduped)
    logger.info("[INFO] 최종 YouTube 전처리 결과: %s (총 %d개 레코드)", out_path, len(deduped))


def main(argv: List[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="YouTube 원본 JSONL → 감성분석/메타 분석용 전처리 JSONL 생성"
    )
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="YouTube 원본 JSONL 경로 (예: data_crawl/youtube.jsonl)",
    )
    parser.add_argument(
        "--output",
        "-o",
        required=True,
        help=(
            "전처리 결과 JSONL 파일명 또는 경로.\n"
            "파일명만 주면 preprocess/preprocessing_data/ 아래에 생성됨."
        ),
    )
    parser.add_argument(
        "--min-length",
        type=int,
        default=0,
        help="text_clean 길이 기준 최소 길이 필터. 0이면 필터 없음. 예: 30",
    )
    parser.add_argument(
        "--max-length",
        type=int,
        default=None,
        help="text_clean 길이 기준 최대 길이 필터. None이면 필터 없음.",
    )
    parser.add_argument(
        "--lang-filter",
        type=str,
        default=None,
        help="특정 언어만 사용하고 싶을 때. 예: ko 또는 ko,en",
    )

    args = parser.parse_args(argv)

    lang_list: Optional[List[str]] = None
    if args.lang_filter:
        lang_list = args.lang_filter.split(",")

    preprocess_youtube(
        input_path=args.input,
        output_path=args.output,
        min_length=args.min_length,
        max_length=args.max_length,
        lang_filter=lang_list,
    )


if __name__ == "__main__":
    main()
