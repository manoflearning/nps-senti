from __future__ import annotations

import argparse
import os

from nps_senti_core import Config, ensure_data_dirs, init_schema
from nps_senti_crawl import run as run_crawl
from nps_senti_preprocess import run as run_preprocess
from nps_senti_ml import run_featurize, run_infer, run_train
from nps_senti_topics import run as run_topics
from nps_senti_analytics import run as run_analytics
from nps_senti_viz import run as run_viz

from . import __version__


def _cmd_init(args: argparse.Namespace) -> int:
    cfg = Config.from_env(data_dir=args.data_dir)
    created = ensure_data_dirs(cfg)
    if not args.no_db:
        init_schema(cfg.db_path)
    if args.verbose:
        for p in created:
            print(p)
        print(cfg.db_path)
    return 0


def _cmd_info(args: argparse.Namespace) -> int:
    cfg = Config.from_env(data_dir=args.data_dir)
    print(f"data_dir={cfg.data_dir}")
    print(f"raw_dir={cfg.raw_dir}")
    print(f"processed_dir={cfg.processed_dir}")
    print(f"artifacts_dir={cfg.artifacts_dir}")
    print(f"reports_dir={cfg.reports_dir}")
    print(f"db_path={cfg.db_path}")
    return 0


def _cmd_version(_: argparse.Namespace) -> int:
    print(__version__)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="nps-senti")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init", help="Initialize data folders and DB")
    p_init.add_argument("--data-dir", default=os.getenv("NPS_DATA_DIR"))
    p_init.add_argument("--no-db", action="store_true")
    p_init.add_argument("-v", "--verbose", action="store_true")
    p_init.set_defaults(func=_cmd_init)

    p_info = sub.add_parser("info", help="Print resolved paths")
    p_info.add_argument("--data-dir", default=os.getenv("NPS_DATA_DIR"))
    p_info.set_defaults(func=_cmd_info)

    p_ver = sub.add_parser("version", help="Print version")
    p_ver.set_defaults(func=_cmd_version)

    p_crawl = sub.add_parser("crawl", help="Crawl sources")
    p_crawl.add_argument("--data-dir", default=os.getenv("NPS_DATA_DIR"))
    p_crawl.set_defaults(func=lambda a: run_crawl(Config.from_env(data_dir=a.data_dir)))

    p_pre = sub.add_parser("preprocess", help="Preprocess raw data")
    p_pre.add_argument("--data-dir", default=os.getenv("NPS_DATA_DIR"))
    p_pre.set_defaults(
        func=lambda a: run_preprocess(Config.from_env(data_dir=a.data_dir))
    )

    p_feat = sub.add_parser("featurize", help="Build features")
    p_feat.add_argument("--data-dir", default=os.getenv("NPS_DATA_DIR"))
    p_feat.set_defaults(
        func=lambda a: run_featurize(Config.from_env(data_dir=a.data_dir))
    )

    p_train = sub.add_parser("train", help="Train baseline model")
    p_train.add_argument("--data-dir", default=os.getenv("NPS_DATA_DIR"))
    p_train.set_defaults(func=lambda a: run_train(Config.from_env(data_dir=a.data_dir)))

    p_infer = sub.add_parser("infer", help="Batch inference")
    p_infer.add_argument("--data-dir", default=os.getenv("NPS_DATA_DIR"))
    p_infer.set_defaults(func=lambda a: run_infer(Config.from_env(data_dir=a.data_dir)))

    p_topics = sub.add_parser("topics", help="Topic modeling pipeline")
    p_topics.add_argument("--data-dir", default=os.getenv("NPS_DATA_DIR"))
    p_topics.set_defaults(
        func=lambda a: run_topics(Config.from_env(data_dir=a.data_dir))
    )

    p_analytics = sub.add_parser("analytics", help="Analytics and trends")
    p_analytics.add_argument("--data-dir", default=os.getenv("NPS_DATA_DIR"))
    p_analytics.set_defaults(
        func=lambda a: run_analytics(Config.from_env(data_dir=a.data_dir))
    )

    p_viz = sub.add_parser("viz", help="Build dashboard")
    p_viz.add_argument("--data-dir", default=os.getenv("NPS_DATA_DIR"))
    p_viz.set_defaults(func=lambda a: run_viz(Config.from_env(data_dir=a.data_dir)))

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = args.func(args)
    return 0 if result is None else int(result)


if __name__ == "__main__":
    raise SystemExit(main())
