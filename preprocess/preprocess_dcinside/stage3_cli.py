from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, List

from .stage1_models_io import load_raw_posts, write_flattened_jsonl, FlattenedRecord
from .stage2_transform import flatten_post


def preprocess_dcinside(
    input_path: str | Path,
    output_path: str | Path,
    max_comment_len: int = 200,
) -> None:
    """
    forum_dcinside.jsonl → (본문+댓글+시간, 클린 제목, doc_type/parent_id 포함) 전처리 JSONL.
    경로 규칙:
      - input_path:
          * 절대경로면 그대로
          * 상대경로면 repo 루트 기준
      - output_path:
          * 절대경로면 그대로
          * 상대경로인데 디렉터리 없이 파일명만 있으면:
              → repo_root/preprocess/preprocessing_data/ 아래에 생성
          * 디렉터리 포함 상대경로면:
              → repo_root 기준 그대로 사용
    """
    # .../nps-senti-crawl/preprocess/preprocess_dcinside/stage3_cli.py
    # parents[2] → .../nps-senti-crawl (repo root)
    repo_root = Path(__file__).resolve().parents[2]
    base_output_dir = repo_root / "preprocess" / "preprocessing_data"

    # ----- input 경로 처리 -----
    in_path = Path(input_path)
    if not in_path.is_absolute():
        in_path = (repo_root / in_path).resolve()

    # ----- output 경로 처리 -----
    out_arg = Path(output_path)
    if out_arg.is_absolute():
        out_path = out_arg
    else:
        if out_arg.parent == Path("."):
            # 파일명만 온 경우 → preprocessing_data 아래에 생성
            out_path = base_output_dir / out_arg.name
        else:
            # 디렉터리가 포함된 경우 → repo_root 기준으로 그대로 사용
            out_path = (repo_root / out_arg).resolve()

    out_path.parent.mkdir(parents=True, exist_ok=True)

    def generate_records() -> Iterable[FlattenedRecord]:
        for post in load_raw_posts(in_path):
            for rec in flatten_post(post, max_comment_len=max_comment_len):
                yield rec

    write_flattened_jsonl(out_path, generate_records())


def main(argv: List[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="dcinside 원본 JSONL → (본문+댓글+연도, 클린 제목, doc_type/parent_id 포함) 전처리 JSONL"
    )
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="크롤링 원본 JSONL 경로 (forum_dcinside.jsonl)",
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
        "--max-comment-len",
        "-m",
        type=int,
        default=200,
        help="combined_text에 포함될 댓글 내용 최대 길이 (길면 가운데를 ... 으로 축약)",
    )

    args = parser.parse_args(argv)
    preprocess_dcinside(
        input_path=args.input,
        output_path=args.output,
        max_comment_len=args.max_comment_len,
    )


if __name__ == "__main__":
    main()