from __future__ import annotations

import pandas as pd
import numpy as np
import streamlit as st
import altair as alt

from nps_dashboard.config import DATA_PATH, ARTICLE_SOURCES, SENTIMENT_OPTIONS
from nps_dashboard.data import load_data
from nps_dashboard.wordcloud_tools import generate_wordcloud_image

# ----------------------
# 0. Streamlit 기본 설정
# ----------------------
st.set_page_config(
    page_title="국민연금 여론 대시보드",
    layout="wide",
)

st.title("국민연금 인터넷 여론 분석 대시보드")
st.subheader("by. TEAM FullRunAI")

# ----------------------
# 1. 데이터 로딩
# ----------------------
df_raw = load_data(DATA_PATH)

# ------------------------------------------------------------
# 글로벌 필터: 소스 + 기간
# ------------------------------------------------------------
st.markdown("### 🔎 필터 (전체 적용)")

available_sources_all = sorted(df_raw["source"].dropna().unique().tolist())
filter_left, filter_right, filter_meta = st.columns([1.6, 1.6, 1.8])

with filter_left:
    selected_sources_global = st.multiselect(
        "포함할 사이트",
        options=available_sources_all,
        default=available_sources_all,
    )

with filter_right:
    picked_range = None
    if "date" in df_raw.columns and df_raw["date"].notna().any():
        valid_dates = df_raw["date"].dropna()
        min_date = valid_dates.min().date()
        max_date = valid_dates.max().date()
        default_start = max_date - pd.Timedelta(days=90)
        default_start = max(default_start, min_date)
        picked_range = st.date_input(
            "기간 선택 (기본: 최근 90일)",
            value=(default_start, max_date),
            min_value=min_date,
            max_value=max_date,
            key="global_date_range",
        )
    else:
        st.info("날짜 정보가 없어 기간 필터를 적용할 수 없습니다.")

# 필터 적용 (전체)
df_filtered = df_raw.copy()

if selected_sources_global:
    df_filtered = df_filtered[
        df_filtered["source"].isin(selected_sources_global)
    ].copy()

if picked_range and isinstance(picked_range, (list, tuple)) and len(picked_range) == 2:
    start_date, end_date = picked_range
    start_ts = pd.Timestamp(start_date)
    end_ts = (
        pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
    )
    df_filtered = df_filtered[
        (df_filtered["date"].notna())
        & (df_filtered["date"].between(start_ts, end_ts, inclusive="both"))
    ].copy()

if df_filtered.empty:
    st.warning("선택한 조건에 해당하는 데이터가 없습니다. 필터를 조정해주세요.")
    st.stop()

# 댓글/기사 데이터 분리
df_comments = df_filtered[~df_filtered["source"].isin(ARTICLE_SOURCES)].copy()
df_articles = df_filtered[df_filtered["source"].isin(ARTICLE_SOURCES)].copy()

# 기존 코드 호환용: df는 댓글 데이터
df = df_comments

with filter_meta:
    st.metric("필터 적용 댓글 수", f"{len(df_comments):,}")
    st.caption(f"기사(gdelt) {len(df_articles):,}건은 별도 섹션에서 요약")

# ============================================================
# 2. 종합 분석 (단독)
# ============================================================
st.markdown("## 1️⃣ 종합 분석 (전체)")

total_comments = len(df)
if total_comments > 0:
    neg_ratio = (df["sentiment_label"] == "negative").mean()
    pos_ratio = (df["sentiment_label"] == "positive").mean()
else:
    neg_ratio = pos_ratio = 0.0

comment_data_available = total_comments > 0
article_count = len(df_articles)

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("전체 분석 댓글 수", total_comments)
with col2:
    st.metric("부정 비율", f"{neg_ratio * 100:.1f}%")
with col3:
    st.metric("긍정 비율", f"{pos_ratio * 100:.1f}%")
with col4:
    st.metric("기사(gdelt) 수", article_count)

if not comment_data_available:
    st.info("댓글 데이터가 없습니다. 아래 기사(gdelt) 섹션을 확인하세요.")

st.divider()

if comment_data_available:
    st.markdown("### 전체 감성분석 요약 차트")

    pie_col1, pie_col2 = st.columns(2)

    # (1) 전체 긍/중립/부정 비율 파이차트
    with pie_col1:
        sent_pie = (
            df[df["sentiment_label"].isin(SENTIMENT_OPTIONS)]
            .groupby("sentiment_label")
            .size()
            .reindex(SENTIMENT_OPTIONS, fill_value=0)
            .rename("count")
            .reset_index()
        )

        pie1 = (
            alt.Chart(sent_pie)
            .mark_arc()
            .encode(
                theta="count:Q",
                color=alt.Color(
                    "sentiment_label:N",
                    title="감성",
                    scale=alt.Scale(
                        domain=["negative", "neutral", "positive"],
                        range=["#e74c3c", "#bdc3c7", "#3498db"],
                    ),
                ),
                tooltip=["sentiment_label", "count"],
            )
            .properties(title="전체 긍·중립·부정 비율", width=350, height=300)
        )
        st.altair_chart(pie1, use_container_width=False)

    # (2) 사이트별 댓글 비율 파이차트
    with pie_col2:
        site_counts = (
            df["source"].value_counts().rename_axis("source").reset_index(name="count")
        )
        pie2 = (
            alt.Chart(site_counts)
            .mark_arc()
            .encode(
                theta="count:Q",
                color=alt.Color("source:N", title="사이트"),
                tooltip=["source", "count"],
            )
            .properties(title="사이트별 댓글 비율", width=350, height=300)
        )
        st.altair_chart(pie2, use_container_width=False)

    st.markdown("### 워드클라우드 (한글/영어, 전체 기준)")

    df_wc = df.copy()

    wc_ctrl1, wc_ctrl2, _ = st.columns([1, 1, 2])
    with wc_ctrl1:
        min_freq_ko = st.slider(
            "한글 최소 등장", min_value=1, max_value=20, value=3, step=1
        )
    with wc_ctrl2:
        min_freq_en = st.slider(
            "영어 최소 등장", min_value=1, max_value=20, value=3, step=1
        )

    wc_col_ko, wc_col_en = st.columns([1, 1])

    with wc_col_ko:
        st.write("#### 워드클라우드 (한글)")
        img_ko = generate_wordcloud_image(df_wc, lang="ko", min_freq=min_freq_ko)
        if img_ko is None:
            st.warning("한글 워드클라우드를 만들 충분한 단어가 없습니다.")
        else:
            st.image(img_ko, width=430)

    with wc_col_en:
        st.write("#### Wordcloud (EN)")
        img_en = generate_wordcloud_image(df_wc, lang="en", min_freq=min_freq_en)
        if img_en is None:
            st.warning("영어 워드클라우드를 만들 충분한 단어가 없습니다.")
        else:
            st.image(img_en, width=430)

    st.divider()

# ============================================================
# 3. 종합 분석 (사이트별)
# ============================================================
if comment_data_available:
    st.markdown("## 2️⃣ 종합 분석 (사이트별)")

    GROUPS = {
        "videos": ["youtube"],
        "forums": ["bobaedream", "dcinside", "mlbpark", "theqoo"],
    }

    available_sources = sorted(df["source"].dropna().unique().tolist())
    GROUPS["forums"] = sorted(
        [s for s in available_sources if s not in set(GROUPS["videos"])]
    )
    df_sites = df.copy()

    if df_sites.empty:
        st.warning("댓글 데이터가 없습니다.")
    else:
        source_sent = (
            df_sites.groupby(["source", "sentiment_label"])
            .size()
            .reset_index(name="count")
        )

        source_order = (
            source_sent.groupby("source")["count"]
            .sum()
            .sort_values(ascending=False)
            .index.tolist()
        )

        stack_chart = (
            alt.Chart(source_sent)
            .transform_joinaggregate(total="sum(count)", groupby=["source"])
            .transform_calculate(pct="datum.count / datum.total")
            .mark_bar()
            .encode(
                x=alt.X("source:N", title="사이트", sort=source_order),
                y=alt.Y(
                    "count:Q",
                    stack="normalize",
                    title="비율",
                    axis=alt.Axis(format="%"),
                ),
                color=alt.Color(
                    "sentiment_label:N",
                    title="감성",
                    scale=alt.Scale(
                        domain=["negative", "neutral", "positive"],
                        range=["#e74c3c", "#bdc3c7", "#3498db"],
                    ),
                ),
                tooltip=[
                    alt.Tooltip("source:N", title="사이트"),
                    alt.Tooltip("sentiment_label:N", title="감성"),
                    alt.Tooltip("count:Q", title="댓글 수"),
                    alt.Tooltip("total:Q", title="사이트 총 댓글"),
                    alt.Tooltip("pct:Q", title="비율", format=".1%"),
                ],
            )
            .properties(
                height=340,
                width=900,
                title=alt.TitleParams(
                    "사이트별 감성 레이블 분포 (100% 스택)", fontSize=16
                ),
            )
        )

        top_counts = (
            df_sites["source"]
            .value_counts()
            .reset_index(name="count")
            .rename(columns={"index": "source"})
            .head(5)
        )
        bar_top = (
            alt.Chart(top_counts)
            .mark_bar()
            .encode(
                y=alt.Y("source:N", sort="-x", title="사이트"),
                x=alt.X("count:Q", title="댓글 수"),
                tooltip=["source", "count"],
            )
            .properties(height=200, title=alt.TitleParams("댓글 수 TOP5", fontSize=16))
        )

        st.altair_chart(stack_chart | bar_top, use_container_width=True)

    st.markdown("### 리커트 차트 (사이트별 부정/중립/긍정 균형)")

    if not df_sites.empty:
        df_likert = (
            df_sites[df_sites["sentiment_label"].isin(SENTIMENT_OPTIONS)]
            .groupby(["source", "sentiment_label"])
            .size()
            .unstack(fill_value=0)
            .reindex(columns=SENTIMENT_OPTIONS, fill_value=0)
        )

        df_likert["total"] = df_likert.sum(axis=1)
        max_total = df_likert["total"].max() or 1

        segments = []
        for src, row in df_likert.iterrows():
            total = int(row["total"]) or 1
            scale = total / max_total

            neg = (row.get("negative", 0) / total) * scale
            neu = (row.get("neutral", 0) / total) * scale
            pos = (row.get("positive", 0) / total) * scale

            neu_left = -neu / 2
            neu_right = neu / 2

            segments.append(
                {
                    "source": src,
                    "sentiment": "negative",
                    "x0": neu_left - neg,
                    "x1": neu_left,
                    "total": total,
                }
            )
            segments.append(
                {
                    "source": src,
                    "sentiment": "neutral",
                    "x0": neu_left,
                    "x1": neu_right,
                    "total": total,
                }
            )
            segments.append(
                {
                    "source": src,
                    "sentiment": "positive",
                    "x0": neu_right,
                    "x1": neu_right + pos,
                    "total": total,
                }
            )

        likert_df = pd.DataFrame(segments)

        likert_chart = (
            alt.Chart(likert_df)
            .mark_bar()
            .encode(
                y=alt.Y("source:N", title="사이트"),
                x=alt.X(
                    "x0:Q",
                    title="← 부정 / 중립 / 긍정 →",
                    scale=alt.Scale(domain=[-1, 1]),
                ),
                x2="x1:Q",
                color=alt.Color(
                    "sentiment:N",
                    title="감성",
                    scale=alt.Scale(
                        domain=["negative", "neutral", "positive"],
                        range=["#e74c3c", "#bdc3c7", "#3498db"],
                    ),
                ),
                tooltip=["source", "sentiment", "total", "x0", "x1"],
            )
            .properties(height=320)
        )

        zero_line = (
            alt.Chart(pd.DataFrame({"x": [0]})).mark_rule(color="#666").encode(x="x:Q")
        )
        st.altair_chart(likert_chart + zero_line, use_container_width=True)
    else:
        st.info("리커트 차트를 표시할 댓글 데이터가 없습니다.")
else:
    st.info("댓글 데이터가 없어 사이트별 분석을 건너뜁니다.")

st.divider()

# ============================================================
# 4. 기간별 분석
# ============================================================
if comment_data_available:
    st.markdown("## 3️⃣ 기간별 분석")

    if "date" in df.columns and df["date"].notna().any():
        df_time = df[df["date"].notna()].copy()

        if df_time.empty:
            st.warning("해당 기간의 댓글 데이터가 없습니다.")
        else:
            df_sc = df_time.copy()

            prob_cols = [
                "sentiment.negative",
                "sentiment.neutral",
                "sentiment.positive",
            ]
            if all(c in df_sc.columns for c in prob_cols):
                for c in prob_cols:
                    df_sc[c] = pd.to_numeric(df_sc[c], errors="coerce").fillna(0.0)

                s = (
                    df_sc["sentiment.negative"]
                    + df_sc["sentiment.neutral"]
                    + df_sc["sentiment.positive"]
                )
                s = s.replace(0, np.nan)
                df_sc["sentiment.negative"] = (df_sc["sentiment.negative"] / s).fillna(
                    0.0
                )
                df_sc["sentiment.neutral"] = (df_sc["sentiment.neutral"] / s).fillna(
                    0.0
                )
                df_sc["sentiment.positive"] = (df_sc["sentiment.positive"] / s).fillna(
                    0.0
                )

                df_sc["sentiment_score"] = (
                    df_sc["sentiment.positive"] - df_sc["sentiment.negative"]
                ).astype("float32")
                df_sc["sentiment_score"] = df_sc["sentiment_score"].clip(-1.0, 1.0)
            else:
                SENTIMENT_TO_SCORE = {"negative": -1.0, "neutral": 0.0, "positive": 1.0}
                df_sc = df_sc[df_sc["sentiment_label"].isin(SENTIMENT_OPTIONS)].copy()
                df_sc["sentiment_score"] = (
                    df_sc["sentiment_label"].map(SENTIMENT_TO_SCORE).astype("float32")
                )

            st.markdown("### 날짜별 감성 스코어 (-1 ~ +1)")

            MA_DAYS = 7
            daily_score = (
                df_sc.dropna(subset=["date", "sentiment_score"])
                .groupby("date")["sentiment_score"]
                .agg(score="mean", n="size")
                .reset_index()
                .sort_values("date")
            )
            daily_score["ma"] = (
                daily_score["score"].rolling(MA_DAYS, min_periods=1).mean()
            )

            base = (
                alt.Chart(daily_score)
                .transform_calculate(
                    bar_color="datum.score > 0.05 ? '#3498db' : (datum.score < -0.05 ? '#e74c3c' : '#bdc3c7')"
                )
                .encode(x=alt.X("date:T", title="날짜"))
            )

            bars = base.mark_bar().encode(
                y=alt.Y(
                    "score:Q",
                    title="감성 스코어(기대값)",
                    scale=alt.Scale(domain=[-1, 1]),
                ),
                color=alt.Color("bar_color:N", scale=None, legend=None),
                tooltip=[
                    alt.Tooltip("date:T"),
                    alt.Tooltip("score:Q"),
                    alt.Tooltip("ma:Q", title=f"MA({MA_DAYS})"),
                    alt.Tooltip("n:Q", title="표본 수"),
                ],
            )

            ma_line = base.mark_line(color="black").encode(y="ma:Q")
            zero = (
                alt.Chart(pd.DataFrame({"y": [0]}))
                .mark_rule(color="#666")
                .encode(y="y:Q")
            )
            st.altair_chart(
                (bars + ma_line + zero).properties(height=320).interactive(),
                use_container_width=True,
            )

            st.markdown("### 시간대별 감성 스코어 (-1 ~ +1)")

            MA_HOURS = 3
            hour_score = (
                df_sc.dropna(subset=["hour", "sentiment_score"])
                .groupby("hour")["sentiment_score"]
                .agg(score="mean", n="size")
                .reset_index()
            )
            hour_score["hour"] = hour_score["hour"].astype(int)
            hour_score = hour_score.sort_values("hour")
            hour_score["ma"] = (
                hour_score["score"].rolling(MA_HOURS, min_periods=1).mean()
            )

            base_h = (
                alt.Chart(hour_score)
                .transform_calculate(
                    bar_color="datum.score > 0.05 ? '#3498db' : (datum.score < -0.05 ? '#e74c3c' : '#bdc3c7')"
                )
                .encode(x=alt.X("hour:O", title="시간(시)", sort=list(range(24))))
            )

            bars_h = base_h.mark_bar().encode(
                y=alt.Y(
                    "score:Q",
                    title="감성 스코어(기대값)",
                    scale=alt.Scale(domain=[-1, 1]),
                ),
                color=alt.Color("bar_color:N", scale=None, legend=None),
                tooltip=[
                    alt.Tooltip("hour:O"),
                    alt.Tooltip("score:Q"),
                    alt.Tooltip("ma:Q", title=f"MA({MA_HOURS})"),
                    alt.Tooltip("n:Q", title="표본 수"),
                ],
            )

            ma_line_h = base_h.mark_line(color="black").encode(y="ma:Q")
            zero_h = (
                alt.Chart(pd.DataFrame({"y": [0]}))
                .mark_rule(color="#666")
                .encode(y="y:Q")
            )
            st.altair_chart(
                (bars_h + ma_line_h + zero_h).properties(height=320).interactive(),
                use_container_width=True,
            )

            st.markdown("### 댓글 작성량 변화 (날짜 / 시간대)")

            bar_col1, bar_col2 = st.columns(2)

            with bar_col1:
                daily_counts = df_time.groupby("date").size().reset_index(name="count")
                bar_date = (
                    alt.Chart(daily_counts)
                    .mark_bar()
                    .encode(
                        x=alt.X("date:T", title="날짜"),
                        y=alt.Y(
                            "count:Q", title="댓글 수", scale=alt.Scale(domainMin=0)
                        ),
                        tooltip=["date", "count"],
                    )
                    .properties(height=300)
                    .interactive()
                )
                st.altair_chart(bar_date, use_container_width=True)

            with bar_col2:
                if "hour" in df_time.columns:
                    hour_counts = (
                        df_time.groupby("hour").size().reset_index(name="count")
                    )
                    bar_hour = (
                        alt.Chart(hour_counts)
                        .mark_bar()
                        .encode(
                            x=alt.X("hour:O", title="시간대 (시)"),
                            y=alt.Y(
                                "count:Q", title="댓글 수", scale=alt.Scale(domainMin=0)
                            ),
                            tooltip=["hour", "count"],
                        )
                        .properties(height=300)
                        .interactive()
                    )
                    st.altair_chart(bar_hour, use_container_width=True)
                else:
                    st.info("시간 정보가 없어 시간대별 댓글 수를 볼 수 없습니다.")
    else:
        st.info("published_at / date 정보가 없어 기간별 분석을 할 수 없습니다.")
else:
    st.info("댓글 데이터가 없어 기간별 분석을 건너뜁니다.")

# ============================================================
# 5. 기사(gdelt) 요약
# ============================================================
st.divider()
st.markdown("## 📰 기사 인사이트 (gdelt)")

if df_articles.empty:
    st.info("선택한 조건에 해당하는 기사(gdelt) 데이터가 없습니다.")
else:
    article_count = len(df_articles)
    date_min = df_articles["date"].min() if "date" in df_articles else None
    date_max = df_articles["date"].max() if "date" in df_articles else None

    col_a1, col_a2 = st.columns(2)
    with col_a1:
        st.metric("기사 수", f"{article_count:,}")
    with col_a2:
        if pd.notna(date_min) and pd.notna(date_max):
            st.metric("기간", f"{date_min.date()} ~ {date_max.date()}")
        else:
            st.metric("기간", "날짜 정보 없음")

    st.markdown("### 기사 발행량 추이")
    if "date" in df_articles and df_articles["date"].notna().any():
        daily_articles = (
            df_articles.groupby("date")
            .size()
            .reset_index(name="count")
            .sort_values("date")
        )
        chart_articles = (
            alt.Chart(daily_articles)
            .mark_bar()
            .encode(
                x=alt.X("date:T", title="날짜"),
                y=alt.Y("count:Q", title="기사 수", scale=alt.Scale(domainMin=0)),
                tooltip=["date", "count"],
            )
            .properties(height=300)
            .interactive()
        )
        st.altair_chart(chart_articles, use_container_width=True)
    else:
        st.info("기사 날짜 정보가 없어 추이를 표시할 수 없습니다.")

    if "sentiment_label" in df_articles:
        st.markdown("### 기사 감성 분포")
        art_sent = (
            df_articles[df_articles["sentiment_label"].isin(SENTIMENT_OPTIONS)]
            .groupby("sentiment_label")
            .size()
            .reindex(SENTIMENT_OPTIONS, fill_value=0)
            .reset_index(name="count")
        )
        pie_articles = (
            alt.Chart(art_sent)
            .mark_arc()
            .encode(
                theta="count:Q",
                color=alt.Color(
                    "sentiment_label:N",
                    scale=alt.Scale(
                        domain=["negative", "neutral", "positive"],
                        range=["#e74c3c", "#bdc3c7", "#3498db"],
                    ),
                ),
                tooltip=["sentiment_label", "count"],
            )
            .properties(width=350, height=300, title="기사 감성 비율")
        )
        st.altair_chart(pie_articles, use_container_width=False)
    else:
        st.info("기사 감성 레이블이 없어 감성 분포를 표시할 수 없습니다.")
