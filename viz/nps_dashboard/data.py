from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd
import streamlit as st

JSONObj = dict[str, Any]


def _read_one_file(p: Path) -> list[JSONObj]:
    """단일 파일(.jsonl / .json)에서 list[dict] 형태로 로드"""
    suffix = p.suffix.lower()

    if suffix == ".jsonl":
        out: list[JSONObj] = []
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                if isinstance(obj, dict):
                    out.append(cast(JSONObj, obj))
        return out

    if suffix == ".json":
        with p.open("r", encoding="utf-8") as f:
            loaded = json.load(f)

        if isinstance(loaded, list):
            return [cast(JSONObj, x) for x in loaded if isinstance(x, dict)]
        if isinstance(loaded, dict):
            return [cast(JSONObj, loaded)]
        return []

    return []


@st.cache_data
def load_data(path: str) -> pd.DataFrame:
    p = Path(path)

    records: list[JSONObj] = []

    if p.is_dir():
        files = sorted([*p.rglob("*.jsonl"), *p.rglob("*.json")])
        if not files:
            raise FileNotFoundError(f"데이터 폴더에 .jsonl/.json 파일이 없습니다: {p}")

        for fp in files:
            records.extend(_read_one_file(fp))
    else:
        if not p.exists():
            raise FileNotFoundError(f"데이터 파일/폴더를 찾을 수 없습니다: {p}")
        records = _read_one_file(p)

    df: pd.DataFrame = pd.json_normalize(cast(list[dict[str, Any]], records))

    if "is_related" in df.columns:
        df = df[df["is_related"].fillna(False).astype(bool)].copy()

    sentiment_cols = ["sentiment.negative", "sentiment.neutral", "sentiment.positive"]

    if "sentiment" in df.columns and not set(sentiment_cols).issubset(df.columns):
        sent_norm = pd.json_normalize(df["sentiment"])  # pyright: ignore[reportArgumentType]
        sent_norm.columns = [
            f"sentiment.{c}" if not str(c).startswith("sentiment.") else str(c)
            for c in sent_norm.columns
        ]
        df = df.join(sent_norm)

    candidates = {
        "sentiment.negative": ["negative", "neg", "sentiment_negative", "neg_score"],
        "sentiment.neutral": ["neutral", "neu", "sentiment_neutral", "neu_score"],
        "sentiment.positive": ["positive", "pos", "sentiment_positive", "pos_score"],
    }
    for tgt, keys in candidates.items():
        if tgt in df.columns:
            continue
        for k in keys:
            if k in df.columns:
                df[tgt] = df[k]
                break

    keep = [
        "doc_type",
        "comment",
        "comment_text",
        "text",
        "title",
        "source",
        "published_at",
        "comment_publishedAt",
        "is_related",
        "sentiment_label",
        "sentiment.negative",
        "sentiment.neutral",
        "sentiment.positive",
    ]
    cols = [c for c in keep if c in df.columns]
    df = df[cols].copy()

    dt_raw = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns, UTC]")
    src = df["source"] if "source" in df.columns else pd.Series("", index=df.index)

    # gdelt
    if "published_at" in df.columns:
        mask_gdelt = src.eq("gdelt")
        if mask_gdelt.any():
            dt_raw.loc[mask_gdelt] = pd.to_datetime(
                df.loc[mask_gdelt, "published_at"],
                errors="coerce",
                utc=True,
            )

    # youtube
    if "comment_publishedAt" in df.columns:
        mask_youtube = src.eq("youtube")
        if mask_youtube.any():
            dt_raw.loc[mask_youtube] = pd.to_datetime(
                df.loc[mask_youtube, "comment_publishedAt"],
                errors="coerce",
                utc=True,
            )

    # others
    mask_other = ~src.isin(["gdelt", "youtube"])
    if mask_other.any():
        if "comment_publishedAt" in df.columns:
            dt_raw.loc[mask_other] = pd.to_datetime(
                df.loc[mask_other, "comment_publishedAt"],
                errors="coerce",
                utc=True,
            )
        if "published_at" in df.columns:
            fill_mask = mask_other & dt_raw.isna()
            if fill_mask.any():
                dt_raw.loc[fill_mask] = pd.to_datetime(
                    df.loc[fill_mask, "published_at"],
                    errors="coerce",
                    utc=True,
                )

    dt_idx = pd.DatetimeIndex(dt_raw)
    dt_local = dt_idx.tz_convert("Asia/Seoul")

    df["datetime"] = dt_local.tz_localize(None)

    dt_naive = pd.DatetimeIndex(df["datetime"])
    df["date"] = dt_naive.normalize()
    df["hour"] = dt_naive.hour

    if "source" in df.columns:
        df["source"] = df["source"].astype("category")

    for c in sentiment_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("float32")

    df["sentiment_label"] = "unknown"
    existing = [c for c in sentiment_cols if c in df.columns]
    if existing:
        idx = df[existing].idxmax(axis=1)
        mask = idx.notna()
        df.loc[mask, "sentiment_label"] = idx[mask].astype(str).str.split(".").str[-1]
        df["sentiment_label"] = pd.Categorical(
            df["sentiment_label"],
            categories=["negative", "neutral", "positive", "unknown"],
            ordered=True,
        )

    return df
