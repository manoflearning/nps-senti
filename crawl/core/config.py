from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional

import yaml


@dataclass(slots=True)
class TimeWindow:
    start_date: datetime
    end_date: Optional[datetime]


@dataclass(slots=True)
class OutputConfig:
    root: Path
    file_name: Optional[str] = None


@dataclass(slots=True)
class CrawlLimits:
    max_candidates_per_source: int
    request_timeout_sec: int


@dataclass(slots=True)
class QualityConfig:
    min_keyword_hits: int


@dataclass(slots=True)
class RuntimeParams:
    run_id: str


@dataclass(slots=True)
class GdeltSourceConfig:
    max_records_per_keyword: int
    chunk_days: int
    overlap_days: int
    pause_between_requests: float = 1.0
    max_attempts: int = 3
    rate_limit_backoff_sec: float = 5.0
    enabled: bool = True
    max_concurrency: int = 4
    max_days_back: Optional[int] = None


@dataclass(slots=True)
class CommonCrawlSourceConfig:
    max_indexes: int
    per_domain_limit: int
    pause_between_requests: float


@dataclass(slots=True)
class ForumSiteConfig:
    enabled: bool = False
    boards: List[str] = field(default_factory=list)
    max_pages: int = 1
    per_board_limit: int = 50
    pause_between_requests: float = 0.5
    obey_robots: bool = True


@dataclass(slots=True)
class ForumsSourceConfig:
    sites: dict[str, ForumSiteConfig] = field(default_factory=dict)


@dataclass(slots=True)
class CrawlerConfig:
    keywords: List[str]
    allow_domains: List[str]
    lang: List[str]
    time_window: TimeWindow
    output: OutputConfig
    runtime: RuntimeParams
    limits: CrawlLimits
    quality: QualityConfig
    gdelt: GdeltSourceConfig
    forums: ForumsSourceConfig = field(default_factory=ForumsSourceConfig)


def _load_keywords(path: Path) -> List[str]:
    if not path.exists():
        return []
    keywords: List[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        keywords.append(line)
    return keywords


def _load_allow_domains(path: Path) -> List[str]:
    if not path.exists():
        return []
    domains: List[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        domains.append(line.lower())
    return domains


def _ensure_run_id(run_id: Optional[str]) -> str:
    if run_id:
        return run_id
    now = datetime.now(timezone.utc)
    return now.strftime("%Y%m%d-%H%M%S")


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid ISO8601 value: {value}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def load_config(
    base_dir: Path | None = None,
    params_path: Path | None = None,
) -> CrawlerConfig:
    base_dir = base_dir or Path(__file__).resolve().parents[1]
    config_dir = base_dir / "config"
    params_path = params_path or config_dir / "params.yaml"

    with params_path.open("r", encoding="utf-8") as fh:
        params = yaml.safe_load(fh)

    keywords_file = config_dir / "keywords.txt"
    allow_domains_file = config_dir / "domains_allowlist.txt"

    # Prefer inline YAML list if provided; otherwise fallback to file
    keywords_param = params.get("keywords")
    if isinstance(keywords_param, (list, tuple)) and keywords_param:
        keywords = [str(k).strip() for k in keywords_param if str(k).strip()]
    else:
        keywords = _load_keywords(keywords_file)
    # Prefer inline YAML list if provided; otherwise fallback to file
    allow_domains_param = params.get("allow_domains")
    if isinstance(allow_domains_param, (list, tuple)) and allow_domains_param:
        allow_domains = [
            str(d).strip().lower() for d in allow_domains_param if str(d).strip()
        ]
    else:
        allow_domains = _load_allow_domains(allow_domains_file)

    time_window_cfg = params.get("time_window", {})
    start_date = _parse_datetime(time_window_cfg.get("start_date"))
    if start_date is None:
        raise ValueError("time_window.start_date must be set in params.yaml")
    end_date = _parse_datetime(time_window_cfg.get("end_date"))

    lang: Iterable[str] = params.get("lang", ["ko"])
    lang_list = [entry.lower() for entry in lang]

    output_cfg = params.get("output", {})
    output_root = Path(output_cfg.get("root", "data_crawl"))
    output_file_name = output_cfg.get("file_name")

    crawl_cfg = params.get("crawl", {})
    run_id = _ensure_run_id(crawl_cfg.get("run_id"))

    limits_cfg = params.get("limits", {})
    limits = CrawlLimits(
        max_candidates_per_source=int(limits_cfg.get("max_candidates_per_source", 500)),
        request_timeout_sec=int(limits_cfg.get("request_timeout_sec", 30)),
    )

    quality_cfg = params.get("quality", {})
    quality = QualityConfig(
        min_keyword_hits=int(quality_cfg.get("min_keyword_hits", 1)),
    )

    sources_cfg = params.get("sources", {})
    gdelt_cfg = sources_cfg.get("gdelt", {})
    forums_cfg = sources_cfg.get("forums", {})

    gdelt = GdeltSourceConfig(
        enabled=bool(gdelt_cfg.get("enabled", True)),
        max_records_per_keyword=int(gdelt_cfg.get("max_records_per_keyword", 100)),
        chunk_days=int(gdelt_cfg.get("chunk_days", 30)),
        overlap_days=int(gdelt_cfg.get("overlap_days", 0)),
        pause_between_requests=float(gdelt_cfg.get("pause_between_requests", 1.0)),
        max_attempts=int(gdelt_cfg.get("max_attempts", 3)),
        rate_limit_backoff_sec=float(gdelt_cfg.get("rate_limit_backoff_sec", 5.0)),
        max_concurrency=int(gdelt_cfg.get("max_concurrency", 4)),
        max_days_back=(
            int(gdelt_cfg.get("max_days_back"))
            if gdelt_cfg.get("max_days_back") is not None
            else None
        ),
    )

    # Forums: dynamically map unknown site keys into ForumSiteConfig instances
    forums_sites: dict[str, ForumSiteConfig] = {}
    if isinstance(forums_cfg, dict):
        for site, raw in forums_cfg.items():
            if not isinstance(raw, dict):
                continue
            forums_sites[site] = ForumSiteConfig(
                enabled=bool(raw.get("enabled", False)),
                boards=[str(u) for u in raw.get("boards", []) if str(u).strip()],
                max_pages=int(raw.get("max_pages", 1)),
                per_board_limit=int(raw.get("per_board_limit", 50)),
                pause_between_requests=float(raw.get("pause_between_requests", 0.5)),
                obey_robots=bool(raw.get("obey_robots", True)),
            )
    forums = ForumsSourceConfig(sites=forums_sites)

    return CrawlerConfig(
        keywords=keywords,
        allow_domains=allow_domains,
        lang=lang_list,
        time_window=TimeWindow(start_date=start_date, end_date=end_date),
        output=OutputConfig(root=output_root, file_name=output_file_name),
        runtime=RuntimeParams(run_id=run_id),
        limits=limits,
        quality=quality,
        gdelt=gdelt,
        forums=forums,
    )
