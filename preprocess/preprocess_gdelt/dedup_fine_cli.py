from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Tuple, Set


# -----------------------------
# 기본 유틸
# -----------------------------


def read_jsonl(path: Path) -> Iterator[Dict]:
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open("r", encoding="utf-8") as f:
        for line_no, raw in enumerate(f, 1):
            line = raw.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as e:
                raise ValueError(f"{path} line {line_no}: {e}") from e


def write_jsonl(path: Path, records: Iterable[Dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


# -----------------------------
# 제목/텍스트 정규화 & 유사도
# -----------------------------


def normalize_title(title: str) -> str:
    """
    제목 꼬리 제거 + 공백 정리 + 소문자화.

    예:
      "국민연금 보험료 인상 논란 | 연합뉴스" -> "국민연금 보험료 인상 논란"
      "A brief history of NPS | CNN"       -> "a brief history of nps"
    """
    t = (title or "").strip()

    # 연속된 " | 매체명" 꼬리 잘라내기 (길이 짧은 꼬리만 제거)
    # 예: "제목 | 연합뉴스", "제목 | CNN"
    while True:
        m = re.search(r"\s*\|\s*([^|]+)$", t)
        if not m:
            break
        tail = m.group(1).strip()
        # 너무 긴 꼬리는 진짜 제목일 수 있어서 남겨둔다 (대략 30자 컷)
        if len(tail) <= 30:
            t = t[: m.start()].rstrip()
        else:
            break

    # 공백 압축 + 소문자
    t = re.sub(r"\s+", " ", t)
    return t.lower()


def tokenize(text: str) -> Set[str]:
    """
    간단 토큰화: 영문/숫자/한글 기준으로 토큰 추출 후 집합으로.
    """
    if not text:
        return set()
    # \w는 숫자/영문/언더스코어 + 유니코드 문자 포함
    tokens = re.findall(r"\w+", text.lower())
    return set(tokens)


def jaccard_similarity(a: str, b: str) -> float:
    """
    Jaccard 유사도: token 교집합 / 합집합
    """
    ta = tokenize(a)
    tb = tokenize(b)
    if not ta or not tb:
        return 0.0
    inter = ta & tb
    union = ta | tb
    return len(inter) / len(union)


# -----------------------------
# 2차 미세 중복 제거 로직
# -----------------------------


@dataclass
class ArticleWrapper:
    idx: int
    record: Dict
    lang: str
    title_norm: str
    text: str


def build_wrappers(records: List[Dict]) -> List[ArticleWrapper]:
    wrappers: List[ArticleWrapper] = []
    for idx, rec in enumerate(records):
        lang = str(rec.get("lang") or "unknown")
        title = str(rec.get("title") or "")
        text = str(rec.get("text") or "")
        wrappers.append(
            ArticleWrapper(
                idx=idx,
                record=rec,
                lang=lang,
                title_norm=normalize_title(title),
                text=text,
            )
        )
    return wrappers


def fine_deduplicate(
    records: List[Dict],
    text_sim_threshold: float = 0.90,
) -> List[Dict]:
    """
    2차 미세 중복 제거:

    1) lang + normalize_title(title) 기준으로 그룹을 만든다.
    2) 그룹 내에서:
       - 텍스트 길이가 가장 긴 기사를 기준(canonical)으로 잡고
       - 나머지 기사들과 Jaccard(text) 유사도를 계산한다.
       - 유사도 >= text_sim_threshold 이면 거의 같은 기사로 보고 제거한다.

    => 제목 꼬리( | 연합뉴스 등) 제거 + 본문 유사도 기반 "거의 같은 기사"만 날리고,
       내용이 다른 기사들은 남겨두어 정보 손실을 최소화한다.
    """
    wrappers = build_wrappers(records)

    # (lang, title_norm) 기준 그룹핑
    groups: Dict[Tuple[str, str], List[ArticleWrapper]] = {}
    for w in wrappers:
        key = (w.lang, w.title_norm)
        groups.setdefault(key, []).append(w)

    keep_flags = [True] * len(records)
    removed_pairs = 0

    for (lang, title_norm), ws in groups.items():
        if len(ws) <= 1:
            continue

        # 텍스트가 긴 순으로 정렬해서 가장 긴 것 하나를 canonical로
        ws_sorted = sorted(ws, key=lambda w: len(w.text), reverse=True)
        canonical = ws_sorted[0]

        for other in ws_sorted[1:]:
            if not keep_flags[other.idx]:
                continue
            sim = jaccard_similarity(canonical.text, other.text)
            if sim >= text_sim_threshold:
                # 거의 같은 기사로 보고 제거
                keep_flags[other.idx] = False
                removed_pairs += 1

    deduped = [rec for rec, keep in zip(records, keep_flags) if keep]
    print(
        f"[INFO] 2차 미세 중복 제거: 원본 {len(records)}개 -> {len(deduped)}개 "
        f"(제거 {len(records) - len(deduped)}개, 유사도 페어 {removed_pairs}건)"
    )
    return deduped


# -----------------------------
# CLI
# -----------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="GDELT 전처리 결과에 대해 제목 꼬리 + 본문 유사도 기반 2차 미세 중복 제거를 수행합니다."
    )
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="입력 JSONL (예: preprocess/preprocessing_data/gdelt_preprocessed_all.jsonl 또는 gdelt_dedup2.jsonl)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="2차 dedup 결과 JSONL 출력 경로",
    )
    parser.add_argument(
        "--text-sim-threshold",
        type=float,
        default=0.90,
        help="본문 Jaccard 유사도 임계값 (기본 0.90: 거의 완전 동일한 기사만 제거)",
    )
    return parser


def main(argv: List[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    print(f"[INFO] 입력: {args.input}")
    print(f"[INFO] 출력: {args.output}")
    print(f"[INFO] 텍스트 유사도 임계값: {args.text_sim_threshold}")

    records = list(read_jsonl(args.input))
    print(f"[INFO] 원본 레코드 수: {len(records)}")

    deduped = fine_deduplicate(records, text_sim_threshold=args.text_sim_threshold)
    write_jsonl(args.output, deduped)

    print(f"[INFO] 최종 레코드 수: {len(deduped)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
