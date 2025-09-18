from __future__ import annotations

import json
from pathlib import Path

import pytest

from nps_senti.core.config import Config
from nps_senti.crawl.run import run as crawl_run
from nps_senti.crawl.sources.korea_policy_rss import KoreaPolicyRSSSource


pytestmark = pytest.mark.network


def test_korea_policy_rss_end_to_end(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    cfg = Config(data_dir=tmp_path)
    cfg.raw_dir.mkdir(parents=True, exist_ok=True)

    # Limit items to keep test fast and avoid hammering the site.
    src = KoreaPolicyRSSSource(max_items=3)
    monkeypatch.setattr("nps_senti.crawl.run.get_sources", lambda: [src], raising=True)

    crawl_run(cfg)

    raw_path = cfg.raw_dir / "press_releases.jsonl"
    assert raw_path.exists()
    lines = raw_path.read_text(encoding="utf-8").strip().splitlines()
    assert 1 <= len(lines) <= 3

    for line in lines:
        rec = json.loads(line)
        # Minimal contract checks
        assert rec["source"] == "korea_policy_rss"
        assert isinstance(rec["item_id"], str) and rec["item_id"]
        assert rec["url"].startswith("http")
        assert isinstance(rec["title"], str) and rec["title"]
        assert isinstance(rec["content"], str) and rec["content"]
        assert rec["published_at"].endswith("Z")
        assert rec["fetched_at"].endswith("Z")
