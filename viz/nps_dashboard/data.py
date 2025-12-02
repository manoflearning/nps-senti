from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import numpy as np
import streamlit as st

def _read_one_file(p: Path) -> list[dict]:
    """단일 파일(.jsonl/.json)에서 list[dict] 형태로 로드"""
    if p.suffix.lower() == ".jsonl":
        out = []
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                out.append(json.loads(line))
        return out

    if p.suffix.lower() == ".json":
        try:
            with p.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return [data]
            return []
        except json.JSONDecodeError:
            out = []
            with p.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    out.append(json.loads(line))
            return out

    return []

@st.cache_data
def load_data(path: str) -> pd.DataFrame:
    p = Path(path)

    if p.is_dir():
        files = sorted(
            [*p.rglob("*.jsonl"), *p.rglob("*.json")]
        )
        if not files:
            raise FileNotFoundError(f"데이터 폴더에 .jsonl/.json 파일이 없습니다: {p}")

        data = []
        for fp in files:
            data.extend(_read_one_file(fp))
    else:
        if not p.exists():
            raise FileNotFoundError(f"데이터 파일/폴더를 찾을 수 없습니다: {p}")
        data = _read_one_file(p)

    df = pd.json_normalize(data)

    if "is_related" in df.columns:
        df = df[df["is_related"] == True].copy()

    sentiment_cols = ["sentiment.negative", "sentiment.neutral", "sentiment.positive"]

    if "sentiment" in df.columns and not set(sentiment_cols).issubset(df.columns):
        sent_norm = pd.json_normalize(df["sentiment"])
        sent_norm.columns = [
            f"sentiment.{c}" if not str(c).startswith("sentiment.") else str(c)
            for c in sent_norm.columns
        ]
        df = df.join(sent_norm)

    candidates = {
        "sentiment.negative": ["negative", "neg", "sentiment_negative", "neg_score"],
        "sentiment.neutral":  ["neutral", "neu", "sentiment_neutral", "neu_score"],
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
        "comment", "comment_text", "text", "title",
        "source", "published_at", "comment_publishedAt", "is_related",
        "sentiment_label",
        "sentiment.negative", "sentiment.neutral", "sentiment.positive",
    ]
    cols = [c for c in keep if c in df.columns]
    df = df[cols].copy()

    dt_raw = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns, UTC]")
    src = df["source"] if "source" in df.columns else pd.Series("", index=df.index)

    if "published_at" in df.columns:
        mask_gdelt = src.eq("gdelt")
        if mask_gdelt.any():
            dt_raw.loc[mask_gdelt] = pd.to_datetime(df.loc[mask_gdelt, "published_at"], errors="coerce", utc=True)

    if "comment_publishedAt" in df.columns:
        mask_youtube = src.eq("youtube")
        if mask_youtube.any():
            dt_raw.loc[mask_youtube] = pd.to_datetime(df.loc[mask_youtube, "comment_publishedAt"], errors="coerce", utc=True)

    mask_other = ~src.isin(["gdelt", "youtube"])
    if mask_other.any():
        if "comment_publishedAt" in df.columns:
            dt_raw.loc[mask_other] = pd.to_datetime(df.loc[mask_other, "comment_publishedAt"], errors="coerce", utc=True)
        if "published_at" in df.columns:
            fill_mask = mask_other & dt_raw.isna()
            if fill_mask.any():
                dt_raw.loc[fill_mask] = pd.to_datetime(df.loc[fill_mask, "published_at"], errors="coerce", utc=True)

    dt = dt_raw.dt.tz_convert("Asia/Seoul")
    df["datetime"] = dt.dt.tz_localize(None)
    df["date"] = df["datetime"].dt.normalize()
    df["hour"] = df["datetime"].dt.hour

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
        df.loc[mask, "sentiment_label"] = idx[mask].str.split(".").str[-1]
        df["sentiment_label"] = pd.Categorical(
            df["sentiment_label"],
            categories=["negative", "neutral", "positive", "unknown"],
            ordered=True,
        )

    return df
