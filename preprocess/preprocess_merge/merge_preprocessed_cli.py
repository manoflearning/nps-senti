#!/usr/bin/env python
from __future__ import annotations

import argparse
import glob
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ---------- 데이터 모델 (검사용) ----------

# 최소 공통 분모 스키마 (너가 만든 per-site 전처리 결과 기준)
REQUIRED_KEYS = {"id", "source", "lang", "title", "text", "published_at"}


@dataclass
class UnifiedRow:
    """
    통합 전처리 후 한 줄의 공통 스키마.

    - YouTube/디시/포럼 등 전처리 결과를 모두 이 스키마로 통합
    - raw에는 원본 dict를 그대로 보존해서, 추가 메타데이터가 날아가지 않도록 함
    """

    id: str
    source: str
    lang: str
    title: str
    text: str
    published_at: Optional[str]

    comment_index: Optional[int]
    comment_text: Optional[str]
    comment_publishedAt: Optional[str]

    # doc_type / parent_id 등 추가 메타는 raw에 보존
    raw: Dict[str, Any]

    @classmethod
    def from_raw(cls, obj: Dict[str, Any]) -> Optional["UnifiedRow"]:
        """
        원시 JSON 객체를 검증/정규화해서 UnifiedRow로 변환.
        필수 키(id, source, lang, title, text, published_at)가 없으면 None (스킵).
        """

        # 필수 키 체크 (너가 만든 per-site 전처리 포맷 기준)
        missing = REQUIRED_KEYS - obj.keys()
        if missing:
            logger.warning(
                "[WARN] 필수 컬럼 누락(id/source/lang/title/text/published_at): 누락=%s, row=%r",
                ", ".join(sorted(missing)),
                obj,
            )
            return None

        # 기본 필드들 문자열 캐스팅 + trim
        def _str(x: Any) -> str:
            return str(x).strip()

        id_ = _str(obj.get("id"))
        source = _str(obj.get("source") or "unknown")
        lang = _str(obj.get("lang") or "ko")
        title = _str(obj.get("title") or "")
        text = _str(obj.get("text") or "")

        # published_at: 빈 문자열이면 None
        published_at_raw = obj.get("published_at")
        if published_at_raw in ("", None):
            published_at = None
        else:
            published_at = str(published_at_raw).strip()

        # comment_index: int 또는 None
        ci_raw = obj.get("comment_index", None)
        if ci_raw in ("", None):
            comment_index: Optional[int] = None
        else:
            try:
                comment_index = int(ci_raw)
            except Exception:
                logger.debug("comment_index 파싱 실패, None 처리: %r", ci_raw)
                comment_index = None

        # comment_text: 빈 문자열이면 None으로 통일
        ct_raw = obj.get("comment_text", None)
        if ct_raw in ("", None):
            comment_text: Optional[str] = None
        else:
            ct = str(ct_raw).strip()
            comment_text = ct or None

        # comment_publishedAt: 빈 문자열이면 None
        cp_raw = obj.get("comment_publishedAt", None)
        if cp_raw in ("", None):
            comment_publishedAt: Optional[str] = None
        else:
            comment_publishedAt = str(cp_raw).strip()

        return cls(
            id=id_,
            source=source,
            lang=lang,
            title=title,
            text=text,
            published_at=published_at,
            comment_index=comment_index,
            comment_text=comment_text,
            comment_publishedAt=comment_publishedAt,
            raw=obj,
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        최종 JSONL로 쓸 때는 raw를 기반으로 하되,
        핵심 스키마 필드들은 위에서 정규화한 값으로 덮어쓴다.
        (doc_type, parent_id 등 다른 메타 컬럼은 그대로 유지)
        """
        data = dict(self.raw)
        data["id"] = self.id
        data["source"] = self.source
        data["lang"] = self.lang
        data["title"] = self.title
        data["text"] = self.text
        data["published_at"] = self.published_at
        data["comment_index"] = self.comment_index
        data["comment_text"] = self.comment_text
        data["comment_publishedAt"] = self.comment_publishedAt
        return data


# ---------- JSONL 로딩 ----------


def iter_jsonl(path: Path) -> Iterator[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                logger.warning(
                    "[WARN] %s 라인 %d JSON 파싱 실패, 스킵: %s",
                    path,
                    line_no,
                    str(exc),
                )
                continue
            if not isinstance(obj, dict):
                logger.warning("[WARN] %s 라인 %d: dict가 아님, 스킵", path, line_no)
                continue
            yield obj


# ---------- dedup / sort ----------


def make_key(row: UnifiedRow) -> Tuple[Any, ...]:
    """
    중복 판별용 key.

    - 기본: (source, id, comment_index, comment_text)

    이유:
      - comment id를 디시/유튜브/포럼 모두 "{post_id}#c{idx}"로 통일해서,
        id만 써도 충분히 고유하지만,
      - 혹시 같은 id에 text만 다른 중복이 섞였을 때도 안정적으로 잡기 위해
        comment_index, comment_text를 함께 사용.
    """
    return (
        row.source,
        row.id,
        row.comment_index,
        row.comment_text,
    )


def choose_better_row(a: UnifiedRow, b: UnifiedRow) -> UnifiedRow:
    """
    두 레코드가 같은 key를 가질 때, 어느 것을 선택할지 결정.
    기준:
      1) text 길이가 더 긴 것
      2) published_at이 더 구체적인/긴 문자열인 것
    """
    len_a = len(a.text or "")
    len_b = len(b.text or "")

    if len_a > len_b:
        return a
    if len_b > len_a:
        return b

    pa = a.published_at or ""
    pb = b.published_at or ""
    if len(pa) >= len(pb):
        return a
    return b


def deduplicate_rows(rows: List[UnifiedRow]) -> List[UnifiedRow]:
    chosen: Dict[Tuple[Any, ...], UnifiedRow] = {}
    order: List[Tuple[Any, ...]] = []

    for row in rows:
        key = make_key(row)
        if key not in chosen:
            chosen[key] = row
            order.append(key)
        else:
            better = choose_better_row(chosen[key], row)
            chosen[key] = better

    if len(order) != len(chosen):
        logger.info(
            "[INFO] 통합 과정에서 중복 제거: 원본 %d → %d (key = source,id,comment_index,comment_text)",
            len(rows),
            len(chosen),
        )

    return [chosen[k] for k in order]


def parse_iso_for_sort(s: Optional[str]) -> Optional[datetime]:
    """
    ISO-8601 문자열을 datetime으로 파싱 (정렬용).
    published_at / comment_publishedAt 둘 다 여기에 들어올 수 있음.
    실패하면 None. 반환되는 datetime은 항상 offset-naive (UTC).
    """
    if not s:
        return None
    s = s.strip()
    if not s:
        return None
    try:
        # Python 3.11+ fromisoformat은 Z를 직접 못 읽으니 +00:00로 치환
        if s.endswith("Z"):
            s2 = s.replace("Z", "+00:00")
        else:
            s2 = s
        dt = datetime.fromisoformat(s2)
        # offset-aware면 offset-naive로
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        return dt
    except Exception:
        return None


def sort_rows(rows: List[UnifiedRow]) -> List[UnifiedRow]:
    """
    정렬 기준:
      1) comment_publishedAt (있으면)
      2) 없으면 published_at
      3) 그래도 없으면 원래 순서 유지
    """

    def sort_key(idx_row: Tuple[int, UnifiedRow]):
        idx, r = idx_row
        dt_comment = parse_iso_for_sort(r.comment_publishedAt)
        dt_main = parse_iso_for_sort(r.published_at)
        dt = dt_comment or dt_main
        # datetime.min은 offset-naive이므로 안전 (parse_iso_for_sort도 offset-naive 반환)
        return (dt or datetime.min, idx)

    indexed = list(enumerate(rows))
    indexed.sort(key=sort_key)
    return [r for _, r in indexed]


# ---------- 통합 메인 로직 ----------


def expand_input_paths(patterns: List[str]) -> List[Path]:
    """
    --inputs 에 들어온 값들을 glob 패턴으로 확장.
    예:
      -i preprocess/preprocessing_data/forum_*.jsonl
      -i "preprocess/preprocessing_data/*.jsonl"
    둘 다 지원.
    """
    paths: List[Path] = []
    seen: set[Path] = set()
    for pat in patterns:
        matched = [Path(p) for p in glob.glob(pat)]
        if not matched:
            p = Path(pat)
            if p.exists():
                matched = [p]
        for m in matched:
            if m not in seen:
                seen.add(m)
                paths.append(m)

    # 항상 정렬해서 deterministic한 순서 보장
    paths.sort()
    return paths


def summarize_by_source(rows: List[UnifiedRow]) -> Dict[str, Dict[str, int]]:
    """
    소스 / doc_type별 개수 집계.
    doc_type이 없으면 "unknown"으로 칠함.
    """
    summary: Dict[str, Dict[str, int]] = {}
    for r in rows:
        src = r.source or "unknown"
        doc_type = str(r.raw.get("doc_type") or "unknown")
        bucket = summary.setdefault(src, {})
        bucket[doc_type] = bucket.get(doc_type, 0) + 1
    return summary


def log_summary(prefix: str, rows: List[UnifiedRow]) -> None:
    summary = summarize_by_source(rows)
    logger.info("[INFO] %s 레코드 요약:", prefix)
    for src, type_counts in sorted(summary.items()):
        parts = [f"{dt}:{cnt}" for dt, cnt in sorted(type_counts.items())]
        logger.info("  - %s → %s", src, ", ".join(parts))


def merge_preprocessed(
    input_patterns: List[str],
    output_path: str | Path,
    *,
    drop_duplicates: bool = True,
    sort_by_time: bool = True,
) -> None:
    paths = expand_input_paths(input_patterns)
    if not paths:
        raise FileNotFoundError(f"입력 파일을 찾을 수 없습니다: {input_patterns}")

    logger.info("[INFO] 통합 대상 파일 %d개:", len(paths))
    for p in paths:
        logger.info("  - %s", p)

    unified_rows: List[UnifiedRow] = []
    total_raw = 0
    total_valid = 0

    for p in paths:
        for obj in iter_jsonl(p):
            total_raw += 1
            row = UnifiedRow.from_raw(obj)
            if row is None:
                continue
            unified_rows.append(row)
            total_valid += 1

    logger.info(
        "[INFO] 원본 레코드 %d개 중 유효 레코드 %d개",
        total_raw,
        total_valid,
    )

    if not unified_rows:
        logger.warning(
            "[WARN] 유효한 레코드가 없습니다. 출력 파일을 생성하지 않습니다."
        )
        return

    log_summary("통합 전", unified_rows)

    if drop_duplicates:
        unified_rows = deduplicate_rows(unified_rows)

    if sort_by_time:
        unified_rows = sort_rows(unified_rows)

    log_summary("중복제거/정렬 후", unified_rows)

    out_path = Path(output_path)
    if out_path.parent and not out_path.parent.exists():
        out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", encoding="utf-8") as fw:
        for row in unified_rows:
            fw.write(json.dumps(row.to_dict(), ensure_ascii=False))
            fw.write("\n")

    logger.info(
        "[INFO] 최종 통합 결과: %s (총 %d개 레코드)",
        out_path,
        len(unified_rows),
    )


# ---------- CLI ----------


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="여러 전처리 JSONL 파일을 하나로 통합하는 스크립트"
    )
    parser.add_argument(
        "--inputs",
        "-i",
        nargs="+",
        required=True,
        help=(
            "통합할 전처리 JSONL 경로들 (여러 개 가능, glob 패턴 허용). "
            '예: -i "preprocess/preprocessing_data/*.jsonl" '
            "또는 -i yt_comments1.jsonl yt_comments2.jsonl"
        ),
    )
    parser.add_argument(
        "--output",
        "-o",
        required=True,
        help="통합 결과 JSONL 경로",
    )
    parser.add_argument(
        "--no-dedup",
        action="store_true",
        help="중복 제거를 하지 않으려면 지정",
    )
    parser.add_argument(
        "--no-sort",
        action="store_true",
        help="시간 기준 정렬을 하지 않으려면 지정",
    )

    args = parser.parse_args(argv)

    merge_preprocessed(
        input_patterns=args.inputs,
        output_path=args.output,
        drop_duplicates=not args.no_dedup,
        sort_by_time=not args.no_sort,
    )


if __name__ == "__main__":
    main()
