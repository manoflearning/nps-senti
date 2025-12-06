from __future__ import annotations

from collections import Counter, defaultdict
from typing import Protocol, cast

import pandas as pd
import plotly.graph_objects as go
from wordcloud import WordCloud

from .config import FONT_PATH
from .text_processing import (
    clean_text,
    get_en_stopwords,
    is_english_word,
    is_korean_word,
    strip_web_noise,  # ✅ 추가: Thread/URL/boilerplate 제거
    is_gibberish_en,  # ✅ 추가: 난수 토큰 제거 휴리스틱
)


class OktLike(Protocol):
    def nouns(self, phrase: str) -> list[str]: ...


try:
    from konlpy.tag import Okt as _Okt
except Exception as e:  # pragma: no cover
    _Okt = None
    _KONLPY_IMPORT_ERROR = e
else:
    _KONLPY_IMPORT_ERROR = None


def _require_okt() -> OktLike:
    if _Okt is None:
        raise ImportError(
            "한국어 워드클라우드는 konlpy(Okt)가 필요합니다. "
        ) from _KONLPY_IMPORT_ERROR

    return cast(OktLike, _Okt())


def _iter_row_text(row: pd.Series, df_cols: list[str]) -> str | None:
    """
    - doc_type == 'comment'면 comment_text만 사용 (없으면 comment fallback)
    - 그 외에는 존재하는 텍스트 컬럼들을 합쳐 사용
    """
    label = row.get("sentiment_label") or row.get("label")
    if label not in ("negative", "neutral", "positive"):
        return None

    doc_type = row.get("doc_type")

    if doc_type == "comment":
        v = row.get("comment_text")
        if v is None or (isinstance(v, float) and pd.isna(v)):
            v = row.get("comment")
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        if isinstance(v, (list, tuple)):
            v = " ".join(map(str, v))
        s = str(v).strip()
        return s or None

    parts: list[str] = []
    for c in df_cols:
        v = row.get(c)
        if v is None:
            continue
        try:
            if pd.isna(v):
                continue
        except Exception:
            pass
        if isinstance(v, (list, tuple)):
            v = " ".join(map(str, v))
        s = str(v).strip()
        if s:
            parts.append(s)

    merged = " ".join(parts).strip()
    return merged or None


def compute_word_stats(
    df_subset: pd.DataFrame,
    top_n: int | None = None,
    lang: str = "ko",
    min_freq: int = 2,
):
    """
    ko: KoNLPy(Okt) nouns 기반(한국어 불용어 파일 X)
        - ✅ strip_web_noise 적용 (URL/boilerplate 제거)
    en: wordcloud 기본 STOPWORDS + (선택) stopwords-en.txt
        - ✅ strip_web_noise 적용
        - ✅ is_gibberish_en 휴리스틱 적용 (난수 토큰 제거)
    """
    en_sw = get_en_stopwords()

    # (선택) 정말 남기고 싶은 영어 약어를 예외 처리하고 싶으면 여기에 추가
    # 예: {"nps", "oecd"}
    en_whitelist: set[str] = set()

    freq: Counter[str] = Counter()
    sent_counts: dict[str, dict[str, int]] = defaultdict(
        lambda: {"negative": 0, "neutral": 0, "positive": 0}
    )

    text_cols_priority = [
        c
        for c in ["comment", "comment_text", "text", "title"]
        if c in df_subset.columns
    ]
    if not text_cols_priority:
        return [], {}, {}

    if lang == "ko":
        okt = _require_okt()

        for _, row in df_subset.iterrows():
            label = row.get("sentiment_label") or row.get("label")
            if label not in ("negative", "neutral", "positive"):
                continue

            raw_text = _iter_row_text(row, text_cols_priority)
            if not raw_text:
                continue

            # ✅ 추가: 웹/플랫폼 잡음 줄단위 및 URL 제거
            raw_text = strip_web_noise(raw_text)

            cleaned = clean_text(raw_text)
            if not cleaned:
                continue

            tokens = okt.nouns(cleaned)
            for tok in tokens:
                tok = str(tok).strip()
                if len(tok) < 2:
                    continue
                if not is_korean_word(tok):
                    continue
                freq[tok] += 1
                sent_counts[tok][label] += 1

    elif lang == "en":
        for _, row in df_subset.iterrows():
            label = row.get("sentiment_label") or row.get("label")
            if label not in ("negative", "neutral", "positive"):
                continue

            raw_text = _iter_row_text(row, text_cols_priority)
            if not raw_text:
                continue

            # ✅ 추가: Thread/Threads 홍보줄, URL, 저작권 boilerplate, dc official app 제거
            raw_text = strip_web_noise(raw_text)

            cleaned = clean_text(raw_text)
            if not cleaned:
                continue

            for tok in cleaned.split():
                tok = tok.lower().strip()

                if not is_english_word(tok):
                    continue
                if tok in en_sw:
                    continue

                # ✅ 추가: 난수/ID/도메인 조각 제거
                if is_gibberish_en(tok, whitelist=en_whitelist):
                    continue

                freq[tok] += 1
                sent_counts[tok][label] += 1

    else:
        return [], {}, {}

    words = [w for w, c in freq.items() if c >= min_freq]
    if not words:
        return [], {}, {}

    words.sort(key=lambda w: freq[w], reverse=True)
    if top_n is not None:
        words = words[:top_n]

    return (
        words,
        {w: int(freq[w]) for w in words},
        {w: dict(sent_counts[w]) for w in words},
    )


def generate_wordcloud_image(
    df_subset: pd.DataFrame,
    lang: str = "ko",
    min_freq: int = 2,
    top_n: int = 80,
    max_words: int = 40,
    width: int = 900,
    height: int = 800,
    max_font_size: int = 90,
):
    words, freq_dict, sent_counts = compute_word_stats(
        df_subset,
        top_n=top_n,
        lang=lang,
        min_freq=min_freq,
    )

    if len(words) < 3:
        return None

    sentiment_avg: dict[str, float] = {}
    for w in words:
        counts = sent_counts.get(w, {"negative": 0, "neutral": 0, "positive": 0})
        total = sum(counts.values()) or 1
        score = (counts["positive"] - counts["negative"]) / total
        sentiment_avg[w] = score

    def color_func(word, font_size, position, orientation, random_state=None, **kwargs):
        score = sentiment_avg.get(word, 0.0)
        if score > 0.05:
            return "blue"
        if score < -0.05:
            return "red"
        return "gray"

    try:
        if lang == "ko" and FONT_PATH is None:
            return None

        # ✅ 기존 로직 유지: ko는 FONT_PATH 지정, en은 font_path 없어도 돌아가게
        wc_kwargs = dict(
            width=width,
            height=height,
            background_color="white",
            max_words=max_words,
            max_font_size=max_font_size,
            collocations=False,
            prefer_horizontal=1.0,
        )
        if lang == "ko":
            wc_kwargs["font_path"] = FONT_PATH  # type: ignore

        wc = WordCloud(**wc_kwargs).generate_from_frequencies(freq_dict)  # type: ignore

    except (ValueError, OSError):
        return None

    wc = wc.recolor(color_func=color_func)
    return wc.to_array()


def build_sankey_top_words(df_subset: pd.DataFrame, top_n: int = 8):
    words, _, sent_counts = compute_word_stats(df_subset, top_n=top_n, lang="ko")
    if len(words) < 3:
        return None

    sentiment_labels = ["negative", "neutral", "positive"]
    node_labels = words + sentiment_labels
    node_idx = {lab: i for i, lab in enumerate(node_labels)}

    sources: list[int] = []
    targets: list[int] = []
    values: list[int] = []
    link_colors: list[str] = []

    for w in words:
        counts = sent_counts[w]
        for s in sentiment_labels:
            v = int(counts.get(s, 0))
            if v <= 0:
                continue
            sources.append(node_idx[w])
            targets.append(node_idx[s])
            values.append(v)
            if s == "negative":
                link_colors.append("rgba(231, 76, 60, 0.6)")
            elif s == "positive":
                link_colors.append("rgba(52, 152, 219, 0.6)")
            else:
                link_colors.append("rgba(189, 195, 199, 0.6)")

    if not sources:
        return None

    fig = go.Figure(
        data=[
            go.Sankey(
                node=dict(
                    pad=15,
                    thickness=15,
                    line=dict(color="black", width=0.3),
                    label=node_labels,
                ),
                link=dict(
                    source=sources,
                    target=targets,
                    value=values,
                    color=link_colors,
                ),
            )
        ]
    )
    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=450)
    return fig
