from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from ..config import CrawlerConfig
from .state import AutoState


@dataclass(slots=True)
class RoundPlan:
    # time windows per source (start, end)
    windows: Dict[str, List[Tuple[datetime, datetime]]]
    # youtube keywords to use this round (subset to control quota)
    youtube_keywords: List[str]
    include_forums: bool
    max_fetch: Optional[int]


def _month_start(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return datetime(dt.year, dt.month, 1, tzinfo=timezone.utc)


def _next_month(dt: datetime) -> datetime:
    dt = _month_start(dt)
    year = dt.year + (1 if dt.month == 12 else 0)
    month = 1 if dt.month == 12 else dt.month + 1
    return datetime(year, month, 1, tzinfo=timezone.utc)


def _latest_month_start(now: Optional[datetime] = None) -> datetime:
    now = now or datetime.now(timezone.utc)
    return _month_start(now)


def _iter_recent_months(n: int, now: Optional[datetime] = None) -> List[str]:
    now = now or datetime.now(timezone.utc)
    cur = _month_start(now)
    buckets: List[str] = []
    for _ in range(n):
        buckets.append(f"{cur.year:04d}-{cur.month:02d}")
        # go back one month
        prev_month = cur.month - 1 or 12
        prev_year = cur.year - (1 if cur.month == 1 else 0)
        cur = datetime(prev_year, prev_month, 1, tzinfo=timezone.utc)
    return buckets


def compute_deficits(
    base_config: CrawlerConfig,
    state: AutoState,
    *,
    months_back: int,
    monthly_target_per_source: int,
) -> Tuple[List[str], Dict[str, Dict[str, int]]]:
    recent_buckets = _iter_recent_months(months_back)
    deficits: Dict[str, Dict[str, int]] = {}
    for bucket in recent_buckets:
        by_src = state.counts.get(bucket, {})
        d: Dict[str, int] = {}
        for src in ("gdelt", "youtube", "forums"):
            cur = int(by_src.get(src, 0))
            d[src] = max(0, monthly_target_per_source - cur)
        deficits[bucket] = d
    return recent_buckets, deficits


def plan_round(
    base_config: CrawlerConfig,
    state: AutoState,
    *,
    months_back: int,
    monthly_target_per_source: int,
    round_max_fetch: Optional[int],
    max_gdelt_windows: int,
    max_youtube_windows: int,
    max_forums_windows: int,
    max_youtube_keywords: int,
    include_forums: bool,
) -> RoundPlan:
    # Determine deficits by bucket and source
    now = datetime.now(timezone.utc)
    recent_buckets, deficits = compute_deficits(
        base_config,
        state,
        months_back=months_back,
        monthly_target_per_source=monthly_target_per_source,
    )

    # Rank buckets by total deficit with slight recency bias
    def _score(bucket: str) -> float:
        # newer buckets slightly preferred
        idx = recent_buckets.index(bucket)
        age_weight = 1.0 - (idx * 0.03)  # 3% decay per month back
        total_def = sum(deficits[bucket].values())
        return total_def * age_weight

    ranked = sorted(recent_buckets, key=_score, reverse=True)
    # Rotate ranked list by bucket_cursor to avoid repeating the same bucket
    cursor = max(0, int(state.bucket_cursor)) % max(1, len(ranked))
    ranked = ranked[cursor:] + ranked[:cursor]

    windows: Dict[str, List[Tuple[datetime, datetime]]] = {
        "gdelt": [],
        "youtube": [],
        "forums": [],
    }

    # Choose up to N windows per source from top-deficit buckets
    for bucket in ranked:
        # skip buckets under cooldown for a source
        cool = state.cooldowns.get(bucket, {})
        if (
            len(windows["gdelt"]) < max_gdelt_windows
            and deficits[bucket]["gdelt"] > 0
            and not cool.get("gdelt")
        ):
            # month window
            year, month = map(int, bucket.split("-"))
            start = datetime(year, month, 1, tzinfo=timezone.utc)
            end = _next_month(start)
            if end > now:
                end = now
            if end > start:
                windows["gdelt"].append((start, end))
        if (
            len(windows["youtube"]) < max_youtube_windows
            and deficits[bucket]["youtube"] > 0
            and not cool.get("youtube")
        ):
            year, month = map(int, bucket.split("-"))
            start = datetime(year, month, 1, tzinfo=timezone.utc)
            end = _next_month(start)
            if end > now:
                end = now
            if end > start:
                windows["youtube"].append((start, end))
        if (
            include_forums
            and len(windows["forums"]) < max_forums_windows
            and deficits[bucket]["forums"] > 0
            and not cool.get("forums")
        ):
            year, month = map(int, bucket.split("-"))
            start = datetime(year, month, 1, tzinfo=timezone.utc)
            end = _next_month(start)
            if end > now:
                end = now
            if end > start:
                windows["forums"].append((start, end))
        if (
            len(windows["gdelt"]) >= max_gdelt_windows
            and len(windows["youtube"]) >= max_youtube_windows
            and (not include_forums or len(windows["forums"]) >= 1)
        ):
            break

    # Determine YouTube keywords subset under quota
    yt_keywords_all = [kw for kw in base_config.keywords if kw.strip()]
    if not yt_keywords_all:
        chosen_keywords: List[str] = []
    else:
        # Estimate cost per keyword: search.list(100) + videos.list(1) = 101 units
        per_kw_cost = 101
        # Rough budget = available units // per_kw_cost
        avail = max(0, state.youtube.available() // per_kw_cost)
        limit = min(max_youtube_keywords, avail)
        if limit <= 0:
            chosen_keywords = []
        else:
            # Round-robin from cursor for fairness across keywords
            start_idx = state.youtube_kw_cursor % len(yt_keywords_all)
            ordered = yt_keywords_all[start_idx:] + yt_keywords_all[:start_idx]
            chosen_keywords = ordered[:limit]
            state.youtube_kw_cursor = (start_idx + len(chosen_keywords)) % len(
                yt_keywords_all
            )
            # Consume quota upfront
            state.youtube.consume(len(chosen_keywords) * per_kw_cost)

    return RoundPlan(
        windows=windows,
        youtube_keywords=chosen_keywords,
        include_forums=include_forums,
        max_fetch=round_max_fetch,
    )
