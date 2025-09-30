# Sentiment Analysis of the National Pension Service

This repository now follows a monorepo layout: shared building blocks live in dedicated packages under `packages/`, while runnable surfaces (CLIs, services, notebooks) sit under `apps/`. Each stage of the pipeline is isolated in its own package so teams can iterate independently while still sharing a common configuration contract.

## Monorepo layout
- `pyproject.toml` – top-level project that ships the `nps-senti` CLI; depends on local workspace packages.
- `apps/cli/src/nps_senti/` – CLI entrypoint and thin orchestration layer. The console script `nps-senti` resolves to `nps_senti.cli:main`.
- `packages/core/src/nps_senti_core/` – configuration, logging, paths, I/O, and SQLite helpers shared by every stage.
- `packages/crawl/src/nps_senti_crawl/` – synthetic data crawler for bootstrapping the pipeline contract. Add source modules under `nps_senti_crawl/sources/` and decorate their fetch routines with `@register("source-name")` to have them included automatically (see `sources/naver.py` and `sources/dcinside.py` for skeletons).
- `packages/preprocess/src/nps_senti_preprocess/` – cleaning & deduplication helpers that prepare model-ready text.
- `packages/ml/src/nps_senti_ml/` – placeholders for feature extraction, training, inference, and export routines.
- `packages/topics/src/nps_senti_topics/` – placeholders for keyword extraction and topic grouping.
- `packages/analytics/src/nps_senti_analytics/` – placeholders for trend, event, and correlation analytics.
- `packages/viz/src/nps_senti_viz/` – placeholders for chart composition and static dashboard rendering.
- `tests/` – cross-package integration and wiring checks.
- `uv.lock` – workspace lockfile produced by `uv`.

### Package breakdown
#### `nps_senti_core`
- `config.py` – defines the `Config` contract (data locations, DB path resolution) plus helpers to bootstrap directories.
- `paths.py` – canonical filenames for each stage’s artifacts.
- `db.py` – minimal SQLite schema bootstrapper for local artifacts.
- `io.py` – JSONL read/write helpers with filesystem safety rails.
- `log.py` – shared logging configuration.

#### Stage packages
Each stage publishes a `run(cfg: Config)` entrypoint exposed from its top-level package:
- `nps_senti_crawl` – seeds raw feedback data for experimentation.
- `nps_senti_preprocess` – normalizes and deduplicates crawled feedback.
- `nps_senti_ml` – builds lexical features, trains a heuristic model, performs inference, and emits an ONNX placeholder.
- `nps_senti_topics` – extracts keywords and groups them into simple topics.
- `nps_senti_analytics` – aggregates sentiment counts, flags events, and calculates correlation metrics.
- `nps_senti_viz` – composes chart-ready JSON and renders a static HTML dashboard.

### CLI workflow
Each CLI subcommand is wired but currently raises `NotImplementedError`; fill in the stage packages before running end-to-end.
```
uv run nps-senti init --data-dir ./data-sandbox
uv run nps-senti crawl --data-dir ./data-sandbox
uv run nps-senti preprocess --data-dir ./data-sandbox
uv run nps-senti featurize --data-dir ./data-sandbox
uv run nps-senti train --data-dir ./data-sandbox
uv run nps-senti infer --data-dir ./data-sandbox
uv run nps-senti topics --data-dir ./data-sandbox
uv run nps-senti analytics --data-dir ./data-sandbox
uv run nps-senti viz --data-dir ./data-sandbox
```

`Config` defaults to `./data` when `--data-dir` is omitted (or uses `NPS_DATA_DIR`).

### Development
- Install toolchain: `uv sync --group dev`
- Run tests: `uv run pytest`
- Lint/type-check: `uv run ruff check` and `uv run pyright`
- Regenerate the lockfile when dependencies change: `uv lock`

The workspace is declared in the root `pyproject.toml`; `uv` automatically links all stage packages during local development.
