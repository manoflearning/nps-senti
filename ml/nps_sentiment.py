from __future__ import annotations

import json
import logging  # 추가: 바이어스 로그
from dataclasses import asdict, dataclass
from typing import Any, Dict

from .grok_client import GrokClient


logger = logging.getLogger(__name__)  # 추가
# # DCInside용 연금 관련 키워드 패턴 (grok_client와 동일하게 유지, 확장)
# DCINSIDE_NPS_PATTERN = re.compile(
#     r"(국민연금|연금공단|\bNPS\b|national pension|연금|기금|고갈|수익률|보험료|수급|노후|소득대체율|미납|개혁|다단계|파산)",
#     re.IGNORECASE,
# )


@dataclass
class SentimentResult:
    """
    한 건에 대한 국민연금 감성 분석 결과.
    - is_related: 국민연금 관련 여부
    - negative / neutral / positive: 0.00 ~ 1.00, 합은 1.00 (무관이면 전부 0.0)
    - label: "부정" / "중립" / "긍정" / "무관"
    - explanation: 한국어 한~두 줄 설명
    """

    is_related: bool
    negative: float
    neutral: float
    positive: float
    label: str  # "부정" / "중립" / "긍정" / "무관"
    explanation: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        v = value.strip().lower()
        if v in ("true", "1", "yes", "y"):
            return True
        if v in ("false", "0", "no", "n"):
            return False
    return False


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _extract_json(text: str) -> Dict[str, Any]:
    """
    Grok 응답 텍스트에서 JSON 객체만 뽑아낸다.
    - ```json ... ``` 같은 코드블록이 섞여 있어도 처리
    - 앞뒤 잡소리가 조금 있어도 첫 '{' 와 마지막 '}' 기준으로 잘라 시도
    """
    if text is None:
        raise ValueError("응답이 None 입니다.")

    text = str(text).strip()

    if not text:
        raise ValueError("응답이 비어 있습니다.")

    # 1) 코드블록(```` ... ````) 안에 있는 부분만 우선 시도
    if "```" in text:
        inside = False
        buf_lines: list[str] = []
        for line in text.splitlines():
            line_stripped = line.strip()
            if line_stripped.startswith("```"):
                inside = not inside
                continue
            if inside:
                buf_lines.append(line)
        if buf_lines:
            candidate = "\n".join(buf_lines).strip()
            # 코드블록 안에서 바로 JSON 파싱 시도
            if candidate.startswith("{") and candidate.endswith("}"):
                try:
                    return json.loads(candidate)
                except json.JSONDecodeError:
                    # 실패하면 아래 일반 로직으로 폴백
                    text = candidate

    # 2) 전체 텍스트에서 첫 '{' ~ 마지막 '}' 구간을 잘라 시도
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and start < end:
        candidate = text[start : end + 1]
        try:
            return json.loads(candidate)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"JSON 파싱 실패: {exc}. candidate={candidate[:200]}"
            ) from exc

    # 3) 아무 JSON 객체도 못 찾은 경우
    raise ValueError(f"JSON 객체를 찾을 수 없음: {text[:200]}")


def _renormalize_probs(
    neg: float, neu: float, pos: float
) -> tuple[float, float, float]:
    """
    음수는 0으로 클리핑한 뒤 합을 1로 정규화하고,
    다시 0.XX 형식(두 자리 소수)으로 만들면서
    negative + neutral + positive 합이 정확히 1.00이 되도록 조정한다.
    """
    # 1) 음수 제거
    neg = max(0.0, neg)
    neu = max(0.0, neu)
    pos = max(0.0, pos)

    s = neg + neu + pos
    if s <= 0.0:
        # 완전 이상한 경우엔 중립 1.0으로 설정
        return 0.0, 1.0, 0.0

    # 2) 먼저 합 1.0이 되도록 정규화
    neg /= s
    neu /= s
    pos /= s

    # 3) 퍼센트(0~100) 정수로 변환
    p_neg = round(neg * 100)
    p_neu = round(neu * 100)
    p_pos = round(pos * 100)

    total = p_neg + p_neu + p_pos
    diff = 100 - total

    # 4) 합이 100이 아니면, 가장 큰 값에 diff를 더해 합 100 맞추기
    if diff != 0:
        triples = [("neg", p_neg), ("neu", p_neu), ("pos", p_pos)]
        triples.sort(key=lambda x: x[1], reverse=True)
        largest_label, largest_value = triples[0]
        largest_value += diff  # diff만큼 더해 합을 100으로 보정

        if largest_label == "neg":
            p_neg = largest_value
        elif largest_label == "neu":
            p_neu = largest_value
        else:
            p_pos = largest_value

    # 5) 다시 0~1 사이 두 자리 소수로 변환
    neg_f = round(p_neg / 100.0, 2)
    neu_f = round(p_neu / 100.0, 2)
    pos_f = round(p_pos / 100.0, 2)

    return neg_f, neu_f, pos_f


def _decide_label(neg: float, neu: float, pos: float) -> str:
    """
    세 확률 중 가장 큰 값으로 한국어 라벨을 정한다.
    동점 시 부정 우선 (국민연금 부정이 많음, dcinside 샘플 기반 강화).
    return: "부정" / "중립" / "긍정"
    """
    max_val = max(neg, neu, pos)
    if neg == max_val:
        return "부정"  # 부정 우선 강화
    if pos == max_val:
        return "긍정"
    return "중립"


def _format_explanation(label_ko: str, explanation: str | None) -> str:
    """
    설명이 비었으면 기본 문장을 채우고,
    있으면 앞에 [부정]/[중립]/[긍정] 라벨을 붙인다.
    """
    base = (explanation or "").strip()

    if not base:
        return f"[{label_ko}] 국민연금 제도에 대한 전반적인 감성에 근거해 분포를 부여했습니다."

    # 이미 '[' 로 시작하면 그대로 사용
    if base.startswith("["):
        return base

    # 이미 '부정', '중립', '긍정'으로 시작하면 중복 붙이지 않음
    if base.startswith("부정") or base.startswith("중립") or base.startswith("긍정"):
        return base

    return f"[{label_ko}] {base}"


def parse_grok_response(raw_text: str | Dict[str, Any]) -> SentimentResult:
    """
    Grok가 반환한 텍스트 또는 딕셔너리를 SentimentResult로 변환하고
    규칙에 맞게 값들을 정리한다.
    추가: dcinside 보정 강화, positive 과다 시 바이어스 경고 로그 (윤리성 ↑).
    """
    if isinstance(raw_text, str):
        data = _extract_json(raw_text)
    else:
        data = raw_text

    text = str(data.get("text") or "")

    orig_is_related = _coerce_bool(data.get("is_related", False))
    is_related = orig_is_related

    # # ✅ DCInside 관련성 보정 (키워드 있으면 true 강제, 로그 기록)
    # if "dcinside" in source.lower():
    #     if not is_related and DCINSIDE_NPS_PATTERN.search(text):
    #         is_related = True
    #         logger.info(f"[보정] dcinside 텍스트에 NPS 키워드 감지: is_related false → true. text='{text[:50]}...'")

    negative = _coerce_float(data.get("negative", 0.0))
    neutral = _coerce_float(data.get("neutral", 0.0))
    positive = _coerce_float(data.get("positive", 0.0))
    explanation = str(data.get("explanation", "")).strip()

    # 관련 없으면 규칙대로 강제 설정
    if not is_related:
        if text and len(text) < 5:
            logger.info("[INFO] 짧은 텍스트로 무관 처리: len=%d", len(text))
        return SentimentResult(
            is_related=False,
            negative=0.0,
            neutral=0.0,
            positive=0.0,
            label="무관",
            explanation="국민연금과 관련 없음",
        )

    # 관련(true)이면 확률 재정규화
    negative, neutral, positive = _renormalize_probs(negative, neutral, positive)

    # 바이어스 체크: positive >0.7 시 로그 (dcinside 부정 많아서 강화)
    if positive > 0.7:
        logger.warning("[WARN] High positive bias detected: positive=%.2f", positive)

    label_ko = _decide_label(negative, neutral, positive)
    explanation = _format_explanation(label_ko, explanation)

    return SentimentResult(
        is_related=True,
        negative=negative,
        neutral=neutral,
        positive=positive,
        label=label_ko,
        explanation=explanation,
    )


def analyze_single_comment(
    comment: str,
    source: str,
    client: GrokClient | None = None,
) -> SentimentResult:
    """
    한 개의 댓글/본문 문자열에 대해 국민연금 감성 분석을 수행한다.
    """
    if client is None:
        client = GrokClient()

    meta = {"source": source}
    result = client.analyze_sentiment(text=comment, meta=meta)

    # ✅ source도 같이 넘겨서 parse_grok_response에서 dcinside 보정 가능
    return parse_grok_response({**result, "text": comment, "source": source})
