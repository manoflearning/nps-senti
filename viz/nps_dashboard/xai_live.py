# nps_dashboard/xai_live.py
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import List, Literal, Dict, Any, Tuple

from dotenv import load_dotenv
from xai_sdk import Client
from xai_sdk.chat import user, system
from xai_sdk.search import SearchParameters, web_source

# .env 로컬 크리덴셜 로드
load_dotenv()

GROK_MODEL = "grok-4-1-fast-reasoning"
_DATE_WINDOW = timedelta(days=1)

_client: Client | None = None


def _get_xai_client() -> Client | None:
    """
    XAI_API_KEY가 없으면 None을 반환해서,
    상위 레벨에서 앱이 죽지 않고 메시지로만 안내하도록 함.
    """
    global _client
    if _client is not None:
        return _client

    api_key = os.environ.get("XAI_API_KEY")
    if not api_key:
        return None

    client_kwargs: Dict[str, Any] = {}
    api_host = os.environ.get("XAI_API_HOST")
    if api_host:
        client_kwargs["api_host"] = api_host

    _client = Client(api_key=api_key, **client_kwargs)
    return _client


def _build_prompt(
    kind: Literal[
        "daily_score",
        "hourly_score",
        "daily_volume",
        "hourly_volume",
        "daily_article_volume",
        "policy_direction",
    ],
    label: str,
    stats: Dict[str, Any],
    sample_rows: List[Dict[str, Any]],
) -> str:
    """
    Grok에게 넘길 프롬프트 텍스트 생성.
    kind, label, 통계 요약, 댓글 explanation 샘플을 정리해서 넘긴다.
    """
    kind_map = {
        "daily_score": "날짜별 감성 스코어",
        "hourly_score": "시간대별 감성 스코어",
        "daily_volume": "날짜별 댓글 작성량",
        "hourly_volume": "시간대별 댓글 작성량",
        "daily_article_volume": "날짜별 기사 발행량",
        "policy_direction": "선택 사이트 기반 국민연금 정책 방향성 요약",
    }
    kind_ko = kind_map.get(kind, kind)

    lines: List[str] = [
        f"[분석 대상] {kind_ko}",
        f"[구간 라벨] {label}",
        "",
        "[통계 요약]",
    ]
    for k, v in stats.items():
        lines.append(f"- {k}: {v}")

    if sample_rows:
        lines.append("")
        lines.append("[댓글 표본(텍스트 + sentiment_label + explanation) 일부]")
        for r in sample_rows[:5]:
            txt = (r.get("text") or "")[:120].replace("\n", " ")
            exp = (r.get("explanation") or "")[:160].replace("\n", " ")
            sent = r.get("sentiment_label", "?")
            lines.append(f"- ({sent}) text={txt} / explanation={exp}")
    else:
        lines.append("")
        lines.append(
            "[댓글 표본 없음] explanation 컬럼이 비어 있거나 샘플을 찾지 못했습니다."
        )

    lines.append("")
    lines.append(
        "위 정보를 참고해서, 이 구간에서 왜 이런 양상이 나타났는지 한국어로 3~5줄 안에서 설명해 주세요."
    )
    lines.append(
        "특히, xAI Live Search 기능을 사용해서 '국민연금' 관련 이벤트/뉴스/정책 변화를 찾아보고,"
        " 그 이벤트와 댓글 양상의 관련성을 추론해 주세요."
    )

    if kind in {"hourly_score", "hourly_volume"}:
        lines.append(
            f"이번 분석은 하루 중 '{label}' 시간대(KST 기준)에 해당합니다. "
            "다른 시간대 사건을 가져와서 설명하지 말고, 이 시간대와 직접 연결되는 이유만 서술하세요."
        )
        lines.append(
            f"웹 검색 시에는 '국민연금 {label}' 또는 해당 시간대(예: '{label} 국민연금')를 명시해서 같은 시간 전후의 이슈를 찾아보세요."
        )
    else:
        lines.append(
            f"웹 검색 시에는 반드시 '국민연금' 키워드를 포함하고, 가능하면 '{label}' 구간의 날짜 정보를 함께 검색하세요."
        )

    if kind == "daily_article_volume":
        lines.append(
            "이 구간은 기사 발행량 데이터이므로, 제공된 기사 제목(title)과 본문(text)을 참고하여 "
            "해당 날짜에 어떤 이슈가 있었는지를 요약해 주세요. 댓글 감성 데이터가 없다는 점을 명확히 인지하세요."
        )
    if kind == "policy_direction":
        lines.append(
            "이 분석은 현재 필터에 선택된 온라인 커뮤니티/포털에서 수집한 댓글 explanation을 바탕으로 합니다."
        )
        lines.append(
            "댓글 표본에 담긴 요구, 우려, 제안, 칭찬을 근거로 국민연금 제도/홍보/서비스 측면에서 취할 수 있는 정책 방향을 3가지 내외로 제안하세요."
        )
        lines.append(
            "가능하면 커뮤니케이션 메시지, 제도 개선, 고객 경험 개선 등으로 카테고리를 나누고, 각 제안마다 기대 효과를 간단히 덧붙이세요."
        )
        lines.append("꼭 아래 두 개 파트로 Markdown 형식을 지켜 작성하세요:")
        lines.append(
            "1. **선택 사이트 여론 종합** – 제공된 explanation과 웹 검색을 결합해 주요 여론/근거를 bullet로 정리"
        )
        lines.append(
            "2. **정책 제안** – 번호 매겨진 리스트로 2~3개의 실행 제안과 기대 효과를 기술"
        )
    lines.append(
        "검색에 시간이 다소 걸리더라도 xAI Live Search 결과를 실제로 확인한 뒤, 발견한 이벤트와 감성 데이터의 상관성을 요약해 주세요."
    )

    return "\n".join(lines)


def analyze_bucket_with_grok(
    kind: Literal[
        "daily_score",
        "hourly_score",
        "daily_volume",
        "hourly_volume",
        "daily_article_volume",
        "policy_direction",
    ],
    label: str,
    stats: Dict[str, Any],
    sample_rows: List[Dict[str, Any]],
) -> Tuple[str, List[str]]:
    """
    Grok 4.1 Fast + xai_sdk Live Search를 사용해서
    특정 구간(날짜/시간대 등)의 양상이 왜 나타났는지 분석.

    Returns:
        analysis_text: 한국어 분석 요약 문자열
        citations: Live Search가 탐색 과정에서 본 URL 리스트
    """
    client = _get_xai_client()
    if client is None:
        # 키 없는 경우에도 앱이 죽지 않도록 안전하게 처리
        return (
            "⚠️ Grok/XAI API 키(XAI_API_KEY)가 설정되어 있지 않아 자동 분석을 건너뜁니다. "
            "환경변수 XAI_API_KEY를 설정해 주세요.",
            [],
        )

    prompt = _build_prompt(kind, label, stats, sample_rows)
    search_parameters = _build_search_parameters(kind, label)

    chat = client.chat.create(
        model=GROK_MODEL,
        messages=[
            system(
                "당신은 한국의 국민연금제도에 대한 온라인 여론(댓글/기사)을 분석하는 데이터 분석가입니다. "
                "답변은 항상 한국어로, 3~5문장 정도의 짧은 단락으로 정리해 주세요. "
                "가능하다면 웹 검색 결과 중 신뢰도 높은 출처(정부/언론)을 우선적으로 참고하세요."
            ),
            user(prompt),
        ],
        search_parameters=search_parameters,
    )

    # 스트리밍 말고 한 번에 최종 응답만 필요하므로 sample() 사용
    response = chat.sample()

    analysis_text = (response.content or "").strip()
    citations: List[str] = list(response.citations or [])

    if not analysis_text:
        analysis_text = "분석 결과를 가져오지 못했습니다."

    return analysis_text, citations


def _parse_label_date(label: str) -> datetime | None:
    try:
        # ISO 날짜 문자열(YYYY-MM-DD) 위주로 들어오지만 실패해도 무시
        return datetime.fromisoformat(label)
    except ValueError:
        return None


def _build_search_parameters(
    kind: Literal[
        "daily_score",
        "hourly_score",
        "daily_volume",
        "hourly_volume",
        "daily_article_volume",
        "policy_direction",
    ],
    label: str,
) -> SearchParameters:
    """
    공식 SDK SearchParameters 를 이용해 Live Search 동작을 제어한다.
    날짜 기반 지표인 경우 label 문자열을 날짜로 간주해 ±1일 범위만 검색하도록 제한한다.
    """
    from_date = to_date = None
    parsed = _parse_label_date(label)
    if parsed and kind in {"daily_score", "daily_volume", "daily_article_volume"}:
        from_date = parsed - _DATE_WINDOW
        to_date = parsed + _DATE_WINDOW

    return SearchParameters(
        sources=[web_source(country="KR")],
        mode="on",
        from_date=from_date,
        to_date=to_date,
        max_search_results=8,
        return_citations=True,
    )
