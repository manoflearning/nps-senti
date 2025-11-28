# preprocess/preprocess_youtube/stage3_cli.py
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import List, Optional

from .stage1_models_io import (
    load_raw_youtube,
    write_flattened_jsonl,
)
from .stage2_transform import flatten_many_videos_to_comments


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
)


def preprocess_youtube_comments(
    input_path: str | Path,
    output_path: str | Path,
    *,
    min_length: int = 0,
    lang_filter: Optional[List[str]] = None,
) -> None:
    """
    youtube.jsonl → "영상 + 댓글 1개 = 1줄" 스키마 JSONL 생성.

    출력 스키마(각 레코드):

      {
        "id": <str>,                     # 원본 영상 id
        "source": "youtube",
        "lang": "ko",
        "title": <영상 제목>,
        "text": <영상제목+설명 + (있다면 댓글)>,
        "published_at": <영상 게시 시각 (UTC, Z)>,

        "comment_index": <int|null>,     # 댓글 있는 경우: 0,1,2,... / 없는 경우: null
        "comment_text": <str|null>,      # 댓글 없는 경우: null
        "comment_publishedAt": <str|null> # 댓글 없는 경우: null
      }
    """
    in_path = Path(input_path)
    out_path = Path(output_path)

    if not in_path.exists():
        raise FileNotFoundError(f"입력 파일을 찾을 수 없습니다: {in_path}")

    out_path.parent.mkdir(parents=True, exist_ok=True)

    raw_iter = load_raw_youtube(in_path)
    records = flatten_many_videos_to_comments(
        raw_iter,
        min_length=min_length,
        lang_filter=lang_filter,
    )

    write_flattened_jsonl(out_path, records)
    logger.info(
        "[INFO] 최종 YouTube 댓글 기반 레코드: %s (총 %d개)",
        out_path,
        len(records),
    )


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="YouTube 원본 JSONL → (영상+댓글 1개=1줄) 전처리 JSONL 생성"
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
        help="전처리 결과 JSONL 경로",
    )
    parser.add_argument(
        "--min-length",
        type=int,
        default=0,
        help="text(영상제목+설명+댓글 또는 영상만) 최소 길이. 0이면 필터 없음.",
    )
    parser.add_argument(
        "--lang-filter",
        type=str,
        default="ko",
        help="lang 필터 (쉼표로 여러 개 가능). 예: ko 또는 ko,en. 빈 문자열이면 필터 없음.",
    )

    args = parser.parse_args(argv)

    lang_list: Optional[List[str]] = None
    if args.lang_filter is not None and args.lang_filter.strip():
        lang_list = [x.strip() for x in args.lang_filter.split(",") if x.strip()]

    preprocess_youtube_comments(
        input_path=args.input,
        output_path=args.output,
        min_length=args.min_length,
        lang_filter=lang_list,
    )


if __name__ == "__main__":
    main()
