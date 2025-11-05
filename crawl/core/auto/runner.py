from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from ..config import CrawlerConfig, TimeWindow
from ..pipeline import UnifiedPipeline
from ..storage.index import DocumentIndex
from .scheduler import plan_round
from .state import AutoState
import logging

logger = logging.getLogger(__name__)


def _clone_with_timewindow(
    config: CrawlerConfig, start: datetime, end: Optional[datetime]
) -> CrawlerConfig:
    # dataclasses.replace supports nested replace for top-level only; manually rebuild
    return CrawlerConfig(
        keywords=list(config.keywords),
        lang=list(config.lang),
        time_window=TimeWindow(start_date=start, end_date=end),
        output=config.output,
        runtime=config.runtime,
        limits=config.limits,
        quality=config.quality,
        gdelt=config.gdelt,
        forums=config.forums,
    )


class AutoCrawler:
    def __init__(
        self,
        base_config: CrawlerConfig,
        state_path: Optional[Path] = None,
    ) -> None:
        self.base_config = base_config
        self.output_dir = base_config.output.root
        self.state_path = state_path or (self.output_dir / "_auto_state.json")
        self.state = AutoState.load(self.state_path)
        # Bootstrap index once; pipeline will refresh on store
        self.index = DocumentIndex(self.output_dir)

    def _observer(self, state: AutoState):
        def _fn(document, candidate):  # type: ignore[no-untyped-def]
            state.record_stored(document, candidate)

        return _fn

    def run_round(
        self,
        *,
        months_back: int,
        monthly_target_per_source: int,
        round_max_fetch: Optional[int] = None,
        max_gdelt_windows: int = 1,
        max_youtube_windows: int = 1,
        max_youtube_keywords: int = 2,
        include_forums: bool = True,
        max_forums_windows: int = 1,
    ) -> Dict[str, int]:
        # Possibly adjust environment to reduce YouTube comment API usage
        os.environ.setdefault("YOUTUBE_COMMENTS_PAGES", "0")

        # Step 0: decay cooldowns
        self.state.tick_cooldowns()

        plan = plan_round(
            self.base_config,
            self.state,
            months_back=months_back,
            monthly_target_per_source=monthly_target_per_source,
            round_max_fetch=round_max_fetch,
            max_gdelt_windows=max_gdelt_windows,
            max_youtube_windows=max_youtube_windows,
            max_forums_windows=(max_forums_windows if include_forums else 0),
            max_youtube_keywords=max_youtube_keywords,
            include_forums=include_forums,
        )

        totals: Dict[str, int] = {"stored": 0, "fetched": 0, "discovered": 0}

        # Log plan summary for visibility
        def _fmt_windows(arr):
            return [f"{s.isoformat()}→{e.isoformat()}" for s, e in arr]

        logger.info(
            "Auto plan: gdelt=%s youtube=%s yt_keywords=%s forums=%s max_fetch=%s",
            _fmt_windows(plan.windows.get("gdelt", [])),
            _fmt_windows(plan.windows.get("youtube", [])),
            plan.youtube_keywords,
            plan.include_forums,
            plan.max_fetch,
        )

        # For each planned source, run pipeline with an overridden window
        # GDELT windows
        for start, end in plan.windows.get("gdelt", []):
            cfg = _clone_with_timewindow(self.base_config, start, end)
            pipe = UnifiedPipeline(
                cfg,
                include_sources={"gdelt"},
                max_fetch=plan.max_fetch,
                store_observer=self._observer(self.state),
            )
            stats = pipe.run()
            totals["stored"] += stats.stored
            totals["fetched"] += stats.fetched
            totals["discovered"] += sum(stats.discovered.values())
            # cooldown decision for this bucket
            bucket = f"{start.year:04d}-{start.month:02d}"
            self.state.apply_cooldown(
                bucket,
                "gdelt",
                stored=stats.stored,
                fetched=stats.fetched,
                duplicates_skipped=stats.duplicates_skipped,
            )

        # YouTube windows (with keyword subset)
        for start, end in plan.windows.get("youtube", []):
            cfg = _clone_with_timewindow(self.base_config, start, end)
            keywords_filter = {"youtube": plan.youtube_keywords}
            pipe = UnifiedPipeline(
                cfg,
                include_sources={"youtube"},
                max_fetch=plan.max_fetch,
                store_observer=self._observer(self.state),
                source_keyword_filter=keywords_filter,
            )
            stats = pipe.run()
            totals["stored"] += stats.stored
            totals["fetched"] += stats.fetched
            totals["discovered"] += sum(stats.discovered.values())
            bucket = f"{start.year:04d}-{start.month:02d}"
            self.state.apply_cooldown(
                bucket,
                "youtube",
                stored=stats.stored,
                fetched=stats.fetched,
                duplicates_skipped=stats.duplicates_skipped,
            )

        # Forums windows
        for start, end in plan.windows.get("forums", []):
            # Build board cursor map from state
            cursors = dict(self.state.forum_cursors)
            pipe = UnifiedPipeline(
                self.base_config,
                include_sources={"forums"},
                max_fetch=plan.max_fetch,
                store_observer=self._observer(self.state),
                forums_time_window=(start, end),
                forums_until_date=start,
                forums_board_cursors=cursors,
            )
            stats = pipe.run()
            totals["stored"] += stats.stored
            totals["fetched"] += stats.fetched
            totals["discovered"] += sum(stats.discovered.values())
            # advance cursors based on pages visited
            for board_url, last_page in pipe.last_forums_pages.items():
                self.state.forum_cursors[board_url] = int(last_page) + 1
            bucket = f"{start.year:04d}-{start.month:02d}"
            self.state.apply_cooldown(
                bucket,
                "forums",
                stored=stats.stored,
                fetched=stats.fetched,
                duplicates_skipped=stats.duplicates_skipped,
            )

        # Advance rotation cursor to rotate buckets next round
        self.state.bucket_cursor = (self.state.bucket_cursor + 1) % 120  # keep bounded
        # Persist state
        self.state.save(self.state_path)
        return totals
