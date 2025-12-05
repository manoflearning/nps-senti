from __future__ import annotations

from html import escape
from typing import Any, Callable, Dict, List, Literal, cast

import altair as alt
import pandas as pd
import streamlit as st

from nps_dashboard.xai_live import analyze_bucket_with_grok

SelectionParser = Callable[[Any], Any]
MaskBuilder = Callable[[pd.DataFrame, Any], pd.Series]
LabelFormatter = Callable[[Any], str]
BucketKind = Literal[
    "daily_score",
    "hourly_score",
    "daily_volume",
    "hourly_volume",
    "daily_article_volume",
]


def format_hour_label(value: Any) -> str:
    """
    24시간제 숫자(0~23)를 오전/오후 표기 + 24시각으로 변환해서 반환.
    잘못된 값이 들어오면 문자열로 강제 변환한다.
    """
    try:
        hour = int(value)
    except (TypeError, ValueError):
        return str(value)

    hour = hour % 24
    meridiem = "오전" if 0 <= hour < 12 else "오후"
    hour_12 = hour % 12 or 12
    special = ""
    if hour == 0:
        special = " (자정)"
    elif hour == 12:
        special = " (정오)"

    return f"{meridiem} {hour_12}시{special}"


def render_chart_with_selection(
    chart: alt.Chart,
    *,
    selection_name: str,
    selection_field: str,
    key: str,
    parser: SelectionParser | None = None,
    use_container_width: bool = True,
) -> Any | None:
    """
    Altair chart를 Streamlit에 렌더링하고 selection event에서 원하는 필드를 추출한다.
    """
    _ensure_multiview_selection_support()
    event = st.altair_chart(
        chart,
        use_container_width=use_container_width,
        on_select="rerun",
        selection_mode=selection_name,
        key=key,
    )
    event_payload = event
    if event_payload is None and key:
        event_payload = st.session_state.get(key)

    raw_value = _extract_selection_field(event_payload, selection_name, selection_field)
    if raw_value is None:
        return None

    if parser is None:
        return raw_value

    try:
        return parser(raw_value)
    except Exception:
        return None


def show_bucket_analysis_for_selection(
    selected_value: Any | None,
    *,
    heading_template: str,
    df_comments: pd.DataFrame,
    mask_builder: MaskBuilder,
    kind: BucketKind,
    label_builder: LabelFormatter | None = None,
) -> None:
    """
    selection 결과(selected_value)가 있을 때에만 Grok 분석을 실행하고 heading + 결과 블록을 출력한다.
    """
    if selected_value is None:
        return

    mask = mask_builder(df_comments, selected_value)
    label = label_builder(selected_value) if label_builder else str(selected_value)

    st.markdown(heading_template.format(value=label))
    show_grok_analysis_for_bucket(
        kind=kind,
        label=label,
        df_comments=df_comments,
        mask=mask,
    )


def show_grok_analysis_for_bucket(
    kind,
    label,
    df_comments,
    mask=None,
    *,
    override_stats: Dict[str, Any] | None = None,
    override_samples: List[Dict[str, Any]] | None = None,
):
    if mask is None:
        bucket_df = df_comments.copy()
    else:
        bucket_df = df_comments[mask].copy()

    if bucket_df.empty and not override_samples:
        st.info("해당 구간에 대한 표본이 없어 LLM 분석을 건너뜁니다.")
        return

    stats: Dict[str, Any] = override_stats.copy() if override_stats is not None else {}
    if not stats:
        if "sentiment_score" in bucket_df:
            stats["평균 감성 스코어"] = round(bucket_df["sentiment_score"].mean(), 3)
        stats["데이터 수"] = int(len(bucket_df))
        if "sentiment_label" in bucket_df:
            for lab in ["negative", "neutral", "positive"]:
                ratio = (bucket_df["sentiment_label"] == lab).mean()
                stats[f"{lab} 비율"] = f"{ratio:.1%}"

    if kind in {"hourly_score", "hourly_volume"}:
        stats.setdefault("선택 시간대(KST)", label)
        if "date" in bucket_df.columns:
            ts = pd.to_datetime(bucket_df["date"], errors="coerce")
            ts = ts[ts.notna()]
            if not ts.empty:
                stats.setdefault(
                    "데이터 기간",
                    f"{ts.min().date()} ~ {ts.max().date()}",
                )
                date_counts = (
                    ts.dt.date.value_counts().sort_values(ascending=False).head(3)
                )
                if not date_counts.empty:
                    stats.setdefault(
                        "댓글 집중 날짜",
                        ", ".join(
                            f"{idx}({count:,}건)" for idx, count in date_counts.items()
                        ),
                    )
    else:
        stats.setdefault("선택 구간", label)

    sample_rows: List[Dict[str, Any]] = []
    if override_samples is not None:
        sample_rows = override_samples
    elif "explanation" in bucket_df.columns:
        sample_rows = (
            bucket_df.dropna(subset=["explanation"])
            .head(5)[["text", "explanation", "sentiment_label"]]
            .to_dict(orient="records")
        )

    with st.spinner(
        "LLM이 웹 검색과 댓글 표본을 바탕으로 이유를 분석하는 중입니다..."
    ):
        analysis, citations = analyze_bucket_with_grok(
            kind=kind,
            label=label,
            stats=stats,
            sample_rows=sample_rows,
        )

    if sample_rows:
        items = "".join(
            f"<li>{escape((r.get('display_explanation') or r.get('explanation', '') or '').strip())}</li>"
            for r in sample_rows
        )
        sample_html = f"""
<div style="
    background-color:#f5f5f5;
    border:1px solid #e0e0e0;
    border-radius:8px;
    padding:12px 14px;
    margin-top:10px;
">
    <div style="font-weight:bold;margin-bottom:6px;">💬 표본 explanation (일부)</div>
    <ul style="padding-left:18px;margin:0;">
        {items}
    </ul>
</div>
"""
        st.markdown(sample_html, unsafe_allow_html=True)

    escaped_analysis = escape(analysis or "분석 결과를 가져오지 못했습니다.").replace(
        "\n", "<br>"
    )
    citations_html = ""
    if citations:
        cite_items = "".join(
            f'<li><a href="{escape(url)}" target="_blank">{escape(url)}</a></li>'
            for url in citations
        )
        citations_html = (
            '<div style="margin-top:12px;"><strong>🔗 참고한 웹 페이지</strong>'
            f"<ul style='padding-left:20px;margin-top:6px;margin-bottom:0;'>{cite_items}</ul></div>"
        )

    card_html = f"""
<div style="
    background-color:#f7f2ff;
    border:1px solid #d8c8ff;
    border-radius:10px;
    padding:14px 16px;
    margin-top:6px;
">
    <div style="font-weight:bold;margin-bottom:8px;">💡 LLM 분석 요약</div>
    <div>{escaped_analysis}</div>
    {citations_html}
</div>
"""
    st.markdown(card_html, unsafe_allow_html=True)


def _extract_selection_field(
    event: Any,
    selection_name: str,
    field: str,
) -> Any | None:
    if not isinstance(event, dict):
        return None

    selection_payload = event.get("selection")
    if not isinstance(selection_payload, dict):
        return None

    target = selection_payload.get(selection_name)
    if target is None:
        # Fallback: pick the first available selection payload.
        for val in selection_payload.values():
            if isinstance(val, (dict, list)):
                target = val
                break

    if target is None:
        return None

    return _dig_selection_value(target, field)


def _dig_selection_value(obj: Any, field: str) -> Any | None:
    if obj is None:
        return None

    if isinstance(obj, dict):
        direct = obj.get(field)
        if direct is not None and not isinstance(direct, (dict, list)):
            return direct

        if "values" in obj:
            values = obj["values"]
            result = _dig_selection_value(values, field)
            if result is not None:
                return result

        if {"fields", "values"} <= obj.keys():
            fields = obj.get("fields")
            values = obj.get("values")
            if isinstance(fields, list) and isinstance(values, list):
                for idx, name in enumerate(fields):
                    if name == field and idx < len(values):
                        return values[idx]

        for key in ("value", "datum"):
            if key in obj:
                result = _dig_selection_value(obj[key], field)
                if result is not None:
                    return result

        for value in obj.values():
            if isinstance(value, (dict, list)):
                result = _dig_selection_value(value, field)
                if result is not None:
                    return result
    elif isinstance(obj, list):
        for item in obj:
            result = _dig_selection_value(item, field)
            if result is not None:
                return result

    return None


def parse_date_selection_value(raw: Any) -> Any | None:
    """
    Altair selections sometimes return epoch milliseconds (numbers),
    ISO strings, or dict(year=..., month=..., date=...). 이를 모두 처리해서 date 객체로 변환한다.
    """
    ts = _coerce_timestamp(raw)
    if ts is None or pd.isna(ts):
        return None
    return ts.date()


def _coerce_timestamp(raw: Any) -> pd.Timestamp | None:
    if raw is None:
        return None

    try:
        if isinstance(raw, pd.Timestamp):
            if pd.isna(raw):
                return None
            return raw

        if isinstance(raw, dict):
            ts_kwargs: dict[str, Any] = {}
            for key, value in raw.items():
                if value is None:
                    continue
                normalized_key = "day" if key == "date" else key
                ts_kwargs[normalized_key] = value
            candidate = pd.Timestamp(**ts_kwargs)  # type: ignore[arg-type]
            if pd.isna(candidate):
                return None
            return candidate

        if isinstance(raw, str):
            ts = pd.to_datetime(raw, errors="coerce")
            if ts is None or pd.isna(ts):
                return None
            return cast(pd.Timestamp, ts)

        if isinstance(raw, (int, float)):
            if pd.isna(raw):
                return None
            value = float(raw)
            # Altair temporal selections commonly return ms since epoch.
            if value > 1e12:
                ts = pd.to_datetime(value, unit="ms", origin="unix", errors="coerce")
            elif value > 1e9:
                ts = pd.to_datetime(value, unit="s", origin="unix", errors="coerce")
            else:
                ts = pd.to_datetime(value, errors="coerce")

            if ts is None or pd.isna(ts):
                return None
            return cast(pd.Timestamp, ts)

        ts = pd.to_datetime(raw, errors="coerce")
        if ts is None or pd.isna(ts):
            return None
        return cast(pd.Timestamp, ts)
    except Exception:
        return None


def _ensure_multiview_selection_support() -> None:
    """
    Streamlit은 multi-view + selection을 막아두었으므로 해당 검사 함수를 우회한다.
    """
    if st.session_state.get("_altair_multiview_patch"):
        return

    from streamlit.elements import vega_charts as _vega_charts

    if hasattr(_vega_charts, "_disallow_multi_view_charts"):
        _vega_charts._disallow_multi_view_charts = lambda spec: None  # type: ignore[attr-defined]
    st.session_state["_altair_multiview_patch"] = True
