CI:
- uv run ruff check
- uv run ruff format
- uv run pyright
- uv run python -m pytest -v
- uv run python -m crawl.cli --only forums
