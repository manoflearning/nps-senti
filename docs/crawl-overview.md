# Crawl Pipeline Overview

## Scope
- Source: National Pension Service press releases (`https://www.nps.or.kr/jsppage/news/pressrelease/`).
- Output: normalized records appended to `data/raw/press_releases.jsonl`.

## Flow
1. Load existing JSONL, normalize legacy rows, and build a global `seen` set keyed by `item_id`.
2. Enumerate sources via `nps_senti.crawl.run.get_sources()` (currently: `PressReleaseSource`).
3. For each source, call `iter_items(seen)`:
   - Iterate listing pages in descending chronological order.
   - Skip detail fetch when `item_id` already exists in `seen`.
   - Stop pagination once an entire page contains only known IDs.
   - Fetch new detail pages, parse text/attachments, and yield `RawItem` objects.
4. Merge new records with existing ones, sort by `(published_at, item_id)`, and rewrite JSONL.

## Resilience & Politics
- HTTP errors are logged and skipped; the pipeline keeps other items flowing.
- Parsing uses `html.parser` from the standard library to avoid third-party dependencies.
- Attachments are resolved to absolute URLs and deduplicated per item.
- Design favours extensibility: new sources implement `BaseSource` and are added to the registry.

## Operational Notes
- Re-running the stage is idempotent: duplicates are suppressed before detail fetches.
- `PressReleaseSource` uses a conservative user agent (`nps-senti-crawler/0.1`).
- For sandboxed experiments, pass `--data-dir` to `nps-senti crawl` to isolate outputs.
