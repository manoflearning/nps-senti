# AGENTS.md

## Setup commands
- Install deps: `uv sync --group dev`
- Start dev server: *(not needed; run CLI stages instead with `uv run nps-senti <stage>`)*
- Run tests: `uv run pytest`

## Code style
- Favor the standard library and keep third-party dependencies at zero unless a clear, measurable win is documented.
- Keep modules and functions small, intention-revealing, and free of hidden side effects; choose data structures for clarity over cleverness.
- Work from first principles: understand the data contract and pipeline flow before changing code, and challenge assumptions with quick experiments.
- Treat refactoring and legacy cleanup as core work; simplify or delete dead paths when you touch a module rather than layering on fixes.
- Write comments/docstrings only when they capture non-obvious reasoning or guard invariants.

## Workflow
- Bootstrap the workspace with `uv run nps-senti init`; use `--data-dir` to point at an isolated data sandbox when experimenting.
- Run pipeline stages explicitly (`crawl`, `preprocess`, `featurize`, `train`, `infer`, `topics`, `analytics`, `viz`) and verify their outputs stay within the `Config` contract.
- Before adding new code paths, audit adjacent modules for simplification opportunities; prefer pruning complexity to extending it.
- Keep runtime layers thin—push heavy lifting into pure helpers that are easy to test and reason about.
- Reflect schema or crawler changes in `docs/` (e.g., `docs/data-schema.md`, `docs/crawl-overview.md`) while you work so the documentation stays current.

## Testing & QA
- Back each behavioral change or bug fix with a focused test under `tests/`; lean on realistic fixtures and avoid brittle mocks.
- Run `uv run pytest` plus `uv run ruff check` and `uv run pyright` before sharing work; treat lint/type failures as signals to simplify.
- When touching IO/DB layers, add smoke checks that confirm the schema and file layout stay compatible with downstream stages.

## Decision log expectations
- Capture notable architectural or dependency decisions in PR descriptions (or a short note in README) so future contributors see why trade-offs were made.
- If you must introduce a dependency or extra configuration, document the gain, the exit strategy, and the plan to keep the blast radius contained.
