import os
from types import SimpleNamespace

from crawl.core.auto.runner import AutoCrawler
from crawl.core.config import load_config


def test_autocrawl_does_not_disable_youtube_comments(monkeypatch, tmp_path):
    # Start with no override in the environment
    monkeypatch.delenv("YOUTUBE_COMMENTS_PAGES", raising=False)

    # Avoid network/pipeline work by stubbing the planner
    dummy_plan = SimpleNamespace(
        windows={"gdelt": [], "youtube": [], "forums": []},
        youtube_keywords=[],
        include_forums=False,
        max_fetch=None,
    )
    monkeypatch.setattr(
        "crawl.core.auto.runner.plan_round", lambda *args, **kwargs: dummy_plan
    )

    config = load_config()
    # Keep test artifacts isolated
    config.output.root = tmp_path

    runner = AutoCrawler(config, state_path=tmp_path / "_auto_state_test.json")
    runner.run_round(
        months_back=12,
        monthly_target_per_source=60,
        round_max_fetch=None,
        max_gdelt_windows=0,
        max_youtube_windows=0,
        max_forums_windows=0,
        max_youtube_keywords=0,
        include_forums=False,
    )

    assert "YOUTUBE_COMMENTS_PAGES" not in os.environ
