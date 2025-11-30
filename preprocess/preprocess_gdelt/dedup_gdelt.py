"""
Deduplicate a JSONL file of GDELT-preprocessed rows using:

1) Exact-text dedup (fast, hard duplicates)
2) Token-based candidate filtering + SequenceMatcher for near-duplicates

Usage:
  python -m preprocess.preprocess_gdelt.dedup_gdelt \
    --input <in.jsonl> --output <out.jsonl> [--threshold 0.90]
"""

from __future__ import annotations

import argparse
import json
import re
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, List, Set

RE_WHITESPACE = re.compile(r"\s+")
RE_PUNCT = re.compile(r"[\W_]+", flags=re.UNICODE)


def normalize_text(s: str) -> str:
    """
    텍스트 정규화:
      - 소문자
      - 줄바꿈 → 공백
      - 구두점 제거
      - 공백 여러 개 → 하나
    """
    if not s:
        return ""
    s2 = s.lower()
    s2 = s2.replace("\n", " ")
    s2 = RE_PUNCT.sub(" ", s2)
    s2 = RE_WHITESPACE.sub(" ", s2).strip()
    return s2


def row_key_text(row: Dict) -> str:
    """
    dedup 기준으로 사용할 text:
      title + text 를 합쳐서 하나의 문장으로 보고 처리.
    """
    title = row.get("title") or ""
    text = row.get("text") or ""
    return normalize_text(title + " \n " + text)


def tokenise(s: str) -> List[str]:
    """
    간단한 토큰화: 공백 기준 split.
    이미 normalize_text 를 거쳐 알파벳/숫자/공백 정도만 남아있다.
    """
    if not s:
        return []
    return s.split()


def is_near_duplicate_with_candidates(
    s: str,
    candidates_idx: List[int],
    kept_texts: List[str],
    threshold: float,
) -> bool:
    """
    후보 인덱스 리스트에 대해서만 SequenceMatcher를 돌리며,
    threshold 이상이면 near-duplicate로 간주.
    """
    for idx in candidates_idx:
        c = kept_texts[idx]

        # 길이가 너무 다르면 굳이 SequenceMatcher 돌릴 필요 없음 (간단한 프리필터)
        if abs(len(s) - len(c)) > max(200, int(0.5 * max(len(s), len(c)))):
            continue

        r = SequenceMatcher(None, s, c).ratio()
        if r >= threshold:
            return True
    return False


def dedup_jsonl(
    input_path: Path,
    output_path: Path,
    threshold: float = 0.90,
    max_tokens_for_index: int = 8,
) -> Dict:
    """
    GDELT 전처리 JSONL 파일에서 near-duplicate를 제거한다.

    - 1단계: exact-text dedup
        같은 normalize_text(title+text)를 가진 행은 바로 중복으로 간주하고 스킵.
    - 2단계: token-based candidate 필터 + SequenceMatcher
        완전 동일은 아니지만, 매우 비슷한 텍스트를 threshold 기준으로 제거.

    params
    -------
    threshold: SequenceMatcher similarity threshold (0~1).
    max_tokens_for_index:
        한 문서에 대해서 역색인에 등록/조회에 사용할 토큰 수 상한.
    """
    kept_texts: List[str] = []  # 정규화된 전체 텍스트
    kept_tokens: List[Set[str]] = []  # 인덱싱에 사용된 토큰 집합
    inverted_index: Dict[str, Set[int]] = {}  # token -> {kept index}

    # 🔥 exact-text dedup 용: 정규화된 text → 첫 번째 인덱스
    exact_text_index: Dict[str, int] = {}

    kept_count = 0
    total = 0
    duplicates_near = 0
    duplicates_exact = 0

    with (
        input_path.open("r", encoding="utf-8") as infile,
        output_path.open("w", encoding="utf-8") as outfile,
    ):
        for line in infile:
            total += 1
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                # 깨진 라인은 스킵
                continue

            s = row_key_text(row)

            # ---------- 1) exact-text dedup ----------
            if s in exact_text_index:
                # text(제목+본문)까지 완전히 같은 경우 → 무조건 중복 처리
                duplicates_exact += 1
                continue

            if not s:
                # 텍스트가 전혀 없으면 비교가 어려우니 그냥 살린다.
                outfile.write(json.dumps(row, ensure_ascii=False) + "\n")
                kept_texts.append("")
                kept_tokens.append(set())
                exact_text_index[""] = kept_count
                kept_count += 1
                continue

            toks = tokenise(s)
            if not toks:
                # 토큰화가 안되면(전부 숫자/공백 등) 그냥 살린다.
                outfile.write(json.dumps(row, ensure_ascii=False) + "\n")
                kept_texts.append(s)
                kept_tokens.append(set())
                exact_text_index[s] = kept_count
                kept_count += 1
                continue

            # ---------- 2) token 기반 후보 수집 ----------
            tokens_for_index = toks[:max_tokens_for_index]
            candidate_indices: Set[int] = set()
            for t in tokens_for_index:
                idx_set = inverted_index.get(t)
                if idx_set:
                    candidate_indices.update(idx_set)

            # 후보가 하나라도 있으면 SequenceMatcher로 near-duplicate 검사
            if candidate_indices:
                if is_near_duplicate_with_candidates(
                    s, list(candidate_indices), kept_texts, threshold
                ):
                    duplicates_near += 1
                    continue

            # ---------- keep ----------
            outfile.write(json.dumps(row, ensure_ascii=False) + "\n")
            cur_idx = kept_count

            kept_texts.append(s)
            tokset = set(tokens_for_index)
            kept_tokens.append(tokset)

            # exact-text 인덱스 갱신
            exact_text_index[s] = cur_idx

            # 역색인 갱신
            for t in tokset:
                if t not in inverted_index:
                    inverted_index[t] = set()
                inverted_index[t].add(cur_idx)

            kept_count += 1

    return {
        "total": total,
        "kept": kept_count,
        "duplicates_exact": duplicates_exact,
        "duplicates_near": duplicates_near,
        "output": str(output_path),
    }


def main(argv=None):
    ap = argparse.ArgumentParser(
        description=(
            "Deduplicate a GDELT-preprocessed JSONL file "
            "(exact + near-duplicates, faster version)."
        )
    )
    ap.add_argument("--input", "-i", required=True, help="Input JSONL path")
    ap.add_argument("--output", "-o", required=True, help="Output JSONL path")
    ap.add_argument(
        "--threshold",
        "-t",
        type=float,
        default=0.90,
        help="Similarity threshold (0-1) for SequenceMatcher (near-duplicates)",
    )
    ap.add_argument(
        "--max-tokens",
        type=int,
        default=8,
        help="Number of tokens per document to index for candidate search (default: 8)",
    )
    args = ap.parse_args(argv)

    inp = Path(args.input)
    out = Path(args.output)
    if not inp.exists():
        raise SystemExit(f"Input not found: {inp}")

    stats = dedup_jsonl(
        inp, out, threshold=args.threshold, max_tokens_for_index=args.max_tokens
    )
    print(
        "Dedup complete:"
        f" total={stats['total']},"
        f" kept={stats['kept']},"
        f" exact_dups={stats['duplicates_exact']},"
        f" near_dups={stats['duplicates_near']}"
    )


if __name__ == "__main__":
    main()
