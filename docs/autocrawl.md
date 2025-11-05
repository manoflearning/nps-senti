Auto Crawler
============

Overview
- Dynamic, stateful auto-crawler that balances recency and backfill for GDELT, YouTube, and forums.
- Aims for uniform monthly coverage per source and respects small YouTube API quotas.

Usage
- Status: `uv run python -m crawl.cli autocrawl status`
- Run N rounds: `uv run python -m crawl.cli autocrawl run --rounds 3 --sleep-sec 5`
- Override knobs per run:
  - `--months-back 12` `--monthly-target 60`
  - `--max-fetch 300` `--max-gdelt-windows 2` `--max-youtube-windows 1` `--max-youtube-keywords 2`
  - `--exclude-forums` or `--include-forums`

Config (`crawl/config/params.yaml`)
```
autocrawl:
  enabled: true
  months_back: 12
  monthly_target_per_source: 60
  include_forums: true
  round:
    max_fetch: 300
    max_gdelt_windows: 2
    max_youtube_windows: 1
    max_youtube_keywords: 2
  youtube:
    daily_quota: 1000
    reserve_quota: 200
```

YouTube Quota
- Uses a conservative estimator: ~101 units per keyword (search 100 + videos 1).
- Quota state is tracked in `data_crawl/_auto_state.json` and resets daily (UTC).
- Comments fetching is disabled for autocrawl runs by default (`YOUTUBE_COMMENTS_PAGES=0`).

State
- Stored at `data_crawl/_auto_state.json` with per-month per-source counts and YouTube quota bookkeeping.
- Index remains `_index.json` for duplicates.

Notes
- Forums discovery does not page by time window; it favors recent posts based on configured boards and `max_pages`.
- GDELT/YouTube windows are chosen monthly by deficit with slight recency bias.
