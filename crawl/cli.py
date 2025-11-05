from __future__ import annotations

import argparse
import json
import time
import logging
from dataclasses import asdict
from pathlib import Path

from .core.config import load_config
from .core.pipeline import UnifiedPipeline
from .core.auto.runner import AutoCrawler


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Unified crawler pipeline runner.")
    # Backward-compatible pipeline options (no subcommand)
    parser.add_argument(
        "--params",
        type=Path,
        help="Optional path to params.yaml override.",
    )
    parser.add_argument(
        "--only",
        nargs="+",
        choices=["forums", "youtube", "gdelt"],
        help=(
            "Run only the selected sources. Choices: forums, youtube, gdelt. "
            "If omitted, runs all (subject to config)."
        ),
    )
    parser.add_argument(
        "--forums-sites",
        nargs="+",
        metavar="SITE",
        help=(
            "Within forums, crawl only these site keys (e.g., dcinside mlbpark). "
            "Defaults to all enabled forum sites in params.yaml."
        ),
    )
    parser.add_argument(
        "--max-fetch",
        type=int,
        help=(
            "Maximum number of fetch attempts to perform in this run. "
            "Useful to quickly sample a small batch."
        ),
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level.",
    )
    sub = parser.add_subparsers(dest="command")

    # Autocrawl subcommand
    ac = sub.add_parser("autocrawl", help="Run or inspect the auto-crawler")
    ac.add_argument(
        "action", choices=["run", "status", "plan", "reset"], nargs="?", default="run"
    )
    ac.add_argument("--rounds", type=int, default=1, help="Number of rounds to run")
    ac.add_argument("--sleep-sec", type=float, default=0.0, help="Sleep between rounds")
    ac.add_argument("--months-back", type=int, help="Months to consider for deficits")
    ac.add_argument(
        "--monthly-target",
        type=int,
        help="Target stored docs per source per month",
    )
    ac.add_argument("--include-forums", action="store_true", help="Include forums")
    ac.add_argument(
        "--exclude-forums", action="store_true", help="Exclude forums (override config)"
    )
    ac.add_argument("--max-fetch", type=int, help="Round fetch cap (override config)")
    ac.add_argument("--max-gdelt-windows", type=int, help="Max GDELT windows per round")
    ac.add_argument(
        "--max-youtube-windows", type=int, help="Max YouTube windows per round"
    )
    ac.add_argument(
        "--max-youtube-keywords",
        type=int,
        help="Max YouTube keywords per round (quota-aware)",
    )
    ac.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not fetch; only show plan (for plan/status)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))

    config = load_config(params_path=args.params) if args.params else load_config()

    if args.command == "autocrawl":
        # Configure YouTube quota from config.autocrawl if available
        runner = AutoCrawler(config)
        if args.action == "status":
            state = runner.state
            payload = {
                "stored_by_source": state.stored_by_source,
                "counts": state.counts,
                "youtube_quota": {
                    "daily_quota": state.youtube.daily_quota,
                    "reserve_quota": state.youtube.reserve_quota,
                    "used_today": state.youtube.used_today,
                    "period_start_utc": state.youtube.period_start_utc,
                    "available": state.youtube.available(),
                },
                "last_updated": state.last_updated,
            }
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return 0

        if args.action == "reset":
            # Reset state to a fresh instance and save
            from .core.auto.state import AutoState  # type: ignore

            runner.state = AutoState()
            # carry over youtube quota defaults from config if present
            if config.autocrawl and config.autocrawl.youtube:
                runner.state.youtube.daily_quota = config.autocrawl.youtube.daily_quota
                runner.state.youtube.reserve_quota = (
                    config.autocrawl.youtube.reserve_quota
                )
            runner.state.save(runner.state_path)
            print(
                json.dumps(
                    {"reset": True, "path": str(runner.state_path)},
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 0

        if args.action == "plan":
            from .core.auto.scheduler import plan_round, compute_deficits  # type: ignore

            acfg = config.autocrawl
            months_back = args.months_back or (acfg.months_back if acfg else 12)
            monthly_target = args.monthly_target or (
                acfg.monthly_target_per_source if acfg else 60
            )
            include_forums = (
                args.include_forums or (acfg.include_forums if acfg else True)
            ) and not args.exclude_forums
            round_max_fetch = (
                args.max_fetch
                if args.max_fetch is not None
                else (acfg.round.max_fetch if acfg and acfg.round else None)
            )
            max_gdelt_windows = (
                args.max_gdelt_windows
                if args.max_gdelt_windows is not None
                else (acfg.round.max_gdelt_windows if acfg and acfg.round else 1)
            )
            max_youtube_windows = (
                args.max_youtube_windows
                if args.max_youtube_windows is not None
                else (acfg.round.max_youtube_windows if acfg and acfg.round else 1)
            )
            max_youtube_keywords = (
                args.max_youtube_keywords
                if args.max_youtube_keywords is not None
                else (acfg.round.max_youtube_keywords if acfg and acfg.round else 2)
            )

            # Apply YouTube quota config to state
            if acfg and acfg.youtube:
                runner.state.youtube.daily_quota = acfg.youtube.daily_quota
                runner.state.youtube.reserve_quota = acfg.youtube.reserve_quota

            # Get diagnostics and plan
            recent_buckets, deficits = compute_deficits(
                config,
                runner.state,
                months_back=months_back,
                monthly_target_per_source=monthly_target,
            )
            plan = plan_round(
                config,
                runner.state,
                months_back=months_back,
                monthly_target_per_source=monthly_target,
                round_max_fetch=round_max_fetch,
                max_gdelt_windows=max_gdelt_windows,
                max_youtube_windows=max_youtube_windows,
                max_youtube_keywords=max_youtube_keywords,
                include_forums=include_forums,
            )
            payload = {
                "recent_buckets": recent_buckets,
                "deficits": deficits,
                "plan": {
                    "windows": {
                        k: [
                            (s.isoformat(), (e.isoformat() if e else None))
                            for s, e in v
                        ]
                        for k, v in plan.windows.items()
                    },
                    "youtube_keywords": plan.youtube_keywords,
                    "include_forums": plan.include_forums,
                    "max_fetch": plan.max_fetch,
                },
            }
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return 0

        # action == run
        acfg = config.autocrawl
        months_back = args.months_back or (acfg.months_back if acfg else 12)
        monthly_target = args.monthly_target or (
            acfg.monthly_target_per_source if acfg else 60
        )
        include_forums = (
            args.include_forums or (acfg.include_forums if acfg else True)
        ) and not args.exclude_forums
        round_max_fetch = (
            args.max_fetch
            if args.max_fetch is not None
            else (acfg.round.max_fetch if acfg and acfg.round else None)
        )
        max_gdelt_windows = (
            args.max_gdelt_windows
            if args.max_gdelt_windows is not None
            else (acfg.round.max_gdelt_windows if acfg and acfg.round else 1)
        )
        max_youtube_windows = (
            args.max_youtube_windows
            if args.max_youtube_windows is not None
            else (acfg.round.max_youtube_windows if acfg and acfg.round else 1)
        )
        max_youtube_keywords = (
            args.max_youtube_keywords
            if args.max_youtube_keywords is not None
            else (acfg.round.max_youtube_keywords if acfg and acfg.round else 2)
        )

        # Apply YouTube quota config to state
        if acfg and acfg.youtube:
            runner.state.youtube.daily_quota = acfg.youtube.daily_quota
            runner.state.youtube.reserve_quota = acfg.youtube.reserve_quota

        rounds = int(getattr(args, "rounds", 1) or 1)
        sleep_sec = float(getattr(args, "sleep_sec", 0.0) or 0.0)
        results: list[dict] = []
        for i in range(rounds):
            stats = runner.run_round(
                months_back=months_back,
                monthly_target_per_source=monthly_target,
                round_max_fetch=round_max_fetch,
                max_gdelt_windows=max_gdelt_windows,
                max_youtube_windows=max_youtube_windows,
                max_youtube_keywords=max_youtube_keywords,
                include_forums=include_forums,
            )
            results.append({"round": i + 1, **stats})
            if i < rounds - 1 and sleep_sec > 0:
                time.sleep(sleep_sec)
        print(json.dumps({"results": results}, ensure_ascii=False, indent=2))
        return 0

    # Default: single pipeline run (backward compatible)
    include: set[str] | None = set(args.only) if args.only else None
    forum_filter: set[str] | None = (
        set(args.forums_sites) if args.forums_sites else None
    )
    pipeline = UnifiedPipeline(
        config,
        include_sources=include,
        forum_sites_filter=forum_filter,
        max_fetch=args.max_fetch,
    )
    stats = pipeline.run()
    print(json.dumps(asdict(stats), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
