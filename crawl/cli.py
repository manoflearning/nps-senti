from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict
from pathlib import Path

from .core.config import load_config
from .core.pipeline import UnifiedPipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Unified crawler pipeline runner.")
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
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))

    config = load_config(params_path=args.params) if args.params else load_config()

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
