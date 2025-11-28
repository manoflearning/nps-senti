# ml/grok_client.py
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional

from openai import OpenAI
from dotenv import load_dotenv  # ✅ .env 읽기용


from ml.prompts import SYSTEM_PROMPT_NPS

SYSTEM_PROMPT = SYSTEM_PROMPT_NPS


@dataclass
class GrokConfig:
    api_key: str
    base_url: str
    model: str


def load_config() -> GrokConfig:
    # ✅ .env 파일 읽기 (프로젝트 루트의 .env)
    load_dotenv()

    api_key = os.getenv("XAI_API_KEY") or os.getenv("GROK_API_KEY")
    if not api_key:
        raise RuntimeError(
            "xAI API key not found. 환경변수 XAI_API_KEY 또는 GROK_API_KEY 를 설정해 주세요."
        )

    base_url = os.getenv("XAI_BASE_URL", "https://api.x.ai/v1")
    model = os.getenv("GROK_MODEL", "grok-4-fast-reasoning")

    return GrokConfig(api_key=api_key, base_url=base_url, model=model)


class GrokClient:
    def __init__(self, config: Optional[GrokConfig] = None) -> None:
        self.config = config or load_config()
        self.client = OpenAI(
            base_url=self.config.base_url,
            api_key=self.config.api_key,
        )
        self.model = self.config.model

    def _extract_json(self, text: str) -> Dict[str, Any]:
        """
        응답 텍스트에서 JSON 객체만 안전하게 추출.
        """
        text = text.strip()
        if text.startswith("{") and text.endswith("}"):
            return json.loads(text)

        m = re.search(r"\{.*\}", text, re.DOTALL)
        if not m:
            raise ValueError(f"JSON 객체를 찾을 수 없음: {text[:200]}")
        return json.loads(m.group(0))

    def _normalize_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        확률/라벨/설명 후처리: 0~1 범위, 합≈1, 0.00 포맷에 맞게 정리.
        """
        is_related = bool(result.get("is_related", False))

        if not is_related:
            return {
                "is_related": False,
                "negative": 0.0,
                "neutral": 0.0,
                "positive": 0.0,
                "label": "무관",
                "explanation": "국민연금과 관련 없음",
            }

        neg = float(result.get("negative", 0.0) or 0.0)
        neu = float(result.get("neutral", 0.0) or 0.0)
        pos = float(result.get("positive", 0.0) or 0.0)

        neg = max(0.0, neg)
        neu = max(0.0, neu)
        pos = max(0.0, pos)

        s = neg + neu + pos
        if s <= 0.0:
            neg, neu, pos = 0.0, 1.0, 0.0
        else:
            neg /= s
            neu /= s
            pos /= s

        neg = round(neg, 2)
        neu = round(neu, 2)
        pos = round(pos, 2)

        s2 = neg + neu + pos
        if s2 > 0:
            neg = round(neg / s2, 2)
            neu = round(neu / s2, 2)
            pos = round(pos / s2, 2)

        label = str(result.get("label", "")).strip()
        if label not in ("부정", "중립", "긍정", "무관"):
            if neg >= neu and neg >= pos:
                label = "부정"
            elif pos >= neg and pos >= neu:
                label = "긍정"
            else:
                label = "중립"

        explanation = str(result.get("explanation", "")).strip()
        if not explanation:
            if label == "무관":
                explanation = "국민연금과 관련 없음"
            else:
                explanation = "국민연금 제도에 대한 전반적인 감성을 요약한 설명입니다."

        return {
            "is_related": True,
            "negative": float(f"{neg:.2f}"),
            "neutral": float(f"{neu:.2f}"),
            "positive": float(f"{pos:.2f}"),
            "label": label,
            "explanation": explanation,
        }

    def build_user_prompt(self, text: str, meta: Dict[str, Any]) -> str:
        source = meta.get("source")
        doc_type = meta.get("doc_type")
        lang = meta.get("lang")
        published_at = meta.get("published_at")
        identifier = meta.get("id")

        header_lines = [
            f"id: {identifier}",
            f"source: {source}",
            f"doc_type: {doc_type}",
            f"lang: {lang}",
            f"published_at: {published_at}",
            "",
            "아래는 분석 대상 텍스트입니다.",
            "COMMENT_START",
            text,
            "COMMENT_END",
        ]
        return "\n".join(str(x) for x in header_lines)

    def analyze_sentiment(self, text: str, meta: Dict[str, Any]) -> Dict[str, Any]:
        if not text or not text.strip():
            return {
                "is_related": False,
                "negative": 0.0,
                "neutral": 0.0,
                "positive": 0.0,
                "label": "무관",
                "explanation": "국민연금과 관련 없음",
            }

        user_content = self.build_user_prompt(text, meta)

        completion = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT_NPS},
                {"role": "user", "content": user_content},
            ],
            temperature=0.0,
            max_tokens=512,
        )

        raw = completion.choices[0].message.content or ""
        parsed = self._extract_json(raw)
        normalized = self._normalize_result(parsed)
        return normalized
