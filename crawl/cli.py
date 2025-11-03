from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict
from pathlib import Path

from .core.config import CrawlerConfig, load_config
from .core.pipeline import UnifiedPipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Unified crawler pipeline runner.")
    parser.add_argument(
        "--params",
        type=Path,
        help="Optional path to params.yaml override.",
    )
    parser.add_argument(
        "--no-gdelt",
        action="store_true",
        help="Disable GDELT discovery for this run.",
    )
    parser.add_argument(
        "--max-fetch",
        type=int,
        help="Override max_fetch_per_run for this invocation.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level.",
    )
    return parser


def apply_overrides(config: CrawlerConfig, args: argparse.Namespace) -> None:
    if args.max_fetch is not None and args.max_fetch > 0:
        config.limits = type(config.limits)(
            max_candidates_per_source=config.limits.max_candidates_per_source,
            max_fetch_per_run=args.max_fetch,
            request_timeout_sec=config.limits.request_timeout_sec,
        )
    if getattr(args, "no_gdelt", False):
        # Respect missing attr for older callers
        if hasattr(config, "gdelt") and hasattr(config.gdelt, "enabled"):
            config.gdelt.enabled = False


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))

    config = load_config(params_path=args.params) if args.params else load_config()
    apply_overrides(config, args)

    pipeline = UnifiedPipeline(config)
    stats = pipeline.run()
    print(json.dumps(asdict(stats), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
