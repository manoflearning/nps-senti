from pathlib import Path
from datetime import datetime

from crawl.core.pipeline import UnifiedPipeline
from crawl.core.config import load_config, OutputConfig
from crawl.core.models import Candidate, FetchResult


def test_pipeline_runs_with_forum_candidates(tmp_path, monkeypatch):
    # Load base config and redirect output to tmp
    config = load_config()
    config.output = OutputConfig(root=Path(tmp_path))
    config.limits.max_candidates_per_source = 10
    # Relax quality for testing
    config.quality.min_keyword_hits = 0

    pipeline = UnifiedPipeline(config)

    # Stub discover to return 2 forum candidates
    c1 = Candidate(
        url="https://example.com/1",
        source="dcinside",
        discovered_via={"type": "forum", "site": "dcinside"},
        title="국민연금 관련 글",
    )
    c2 = Candidate(
        url="https://example.com/2",
        source="bobaedream",
        discovered_via={"type": "forum", "site": "bobaedream"},
        title="국민연금 기사",
    )

    monkeypatch.setattr(
        pipeline,
        "discover",
        lambda: {"dcinside": [c1], "bobaedream": [c2]},
    )

    # Stub fetcher to return minimal HTML containing the keyword so quality passes
    def fake_fetch(_candidate: Candidate) -> FetchResult:
        return FetchResult(
            url=_candidate.url,
            fetched_from="live",
            status_code=200,
            html="<html><head><title>국민연금</title></head><body>국민연금 본문 내용</body></html>",
            snapshot_url=_candidate.url,
            encoding="utf-8",
            fetched_at=datetime.utcnow(),
        )

    monkeypatch.setattr(pipeline.fetcher, "fetch", fake_fetch)

    stats = pipeline.run()
    assert stats.stored >= 1
    # Per-source output files exist for forums
    dc_file = Path(tmp_path) / "forum_dcinside.jsonl"
    bd_file = Path(tmp_path) / "forum_bobaedream.jsonl"
    assert dc_file.exists() and dc_file.stat().st_size > 0
    assert bd_file.exists() and bd_file.stat().st_size > 0
