from __future__ import annotations

from pathlib import Path

from .config import Config

RAW_FILENAME = "seed.jsonl"
CLEAN_FILENAME = "clean.jsonl"
DEDUP_FILENAME = "dedup.jsonl"
FEATURES_FILENAME = "features.jsonl"
MODEL_FILENAME = "model.json"
PREDICTIONS_FILENAME = "predictions.jsonl"
KEYWORDS_FILENAME = "keywords.json"
TOPICS_FILENAME = "topics.json"
ANALYTICS_FILENAME = "analytics.json"
EVENTS_FILENAME = "events.json"
CORRELATION_FILENAME = "correlation.json"
CHARTS_FILENAME = "charts.json"
DASHBOARD_FILENAME = "dashboard.html"
ONNX_FILENAME = "model.onnx.json"


def raw_path(cfg: Config) -> Path:
    return cfg.raw_dir / RAW_FILENAME


def clean_path(cfg: Config) -> Path:
    return cfg.processed_dir / CLEAN_FILENAME


def dedup_path(cfg: Config) -> Path:
    return cfg.processed_dir / DEDUP_FILENAME


def features_path(cfg: Config) -> Path:
    return cfg.artifacts_dir / FEATURES_FILENAME


def model_path(cfg: Config) -> Path:
    return cfg.artifacts_dir / MODEL_FILENAME


def predictions_path(cfg: Config) -> Path:
    return cfg.artifacts_dir / PREDICTIONS_FILENAME


def keywords_path(cfg: Config) -> Path:
    return cfg.artifacts_dir / KEYWORDS_FILENAME


def topics_path(cfg: Config) -> Path:
    return cfg.artifacts_dir / TOPICS_FILENAME


def analytics_path(cfg: Config) -> Path:
    return cfg.reports_dir / ANALYTICS_FILENAME


def events_path(cfg: Config) -> Path:
    return cfg.reports_dir / EVENTS_FILENAME


def correlation_path(cfg: Config) -> Path:
    return cfg.reports_dir / CORRELATION_FILENAME


def charts_path(cfg: Config) -> Path:
    return cfg.reports_dir / CHARTS_FILENAME


def dashboard_path(cfg: Config) -> Path:
    return cfg.reports_dir / DASHBOARD_FILENAME


def onnx_path(cfg: Config) -> Path:
    return cfg.artifacts_dir / ONNX_FILENAME
