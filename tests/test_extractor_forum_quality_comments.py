from datetime import datetime, timezone

from crawl.core.config import QualityConfig
from crawl.core.extract.extractor import Extractor
from crawl.core.models import Candidate, FetchResult


def test_forum_quality_uses_comments_when_body_empty():
    extractor = Extractor(
        keywords=["국민연금"],
        allowed_languages=["ko"],
        quality_config=QualityConfig(min_keyword_hits=1),
    )

    # Avoid touching network/comment fetch
    extractor._augment_forum = lambda candidate, extraction, fetch_result: extraction  # type: ignore[method-assign]
    extractor._run_trafilatura = lambda html, url: None  # type: ignore[method-assign]

    cand = Candidate(
        url="https://example.com/square/1",
        source="theqoo",
        discovered_via={"type": "forum"},
        title="제목 없음",
        extra={"forum": {"comments": [{"text": "국민연금 최고"}]}},
    )
    fetch = FetchResult(
        url=cand.url,
        snapshot_url=cand.url,
        html="",
        status_code=200,
        fetched_from="live",
        fetched_at=datetime.now(timezone.utc),
        encoding="utf-8",
    )

    doc, meta = extractor.build_document(cand, fetch, run_id="t")
    assert doc is not None, meta
    assert meta is not None
    keyword_hits = meta.get("keyword_hits", 0)
    assert isinstance(keyword_hits, (int, float))
    assert keyword_hits >= 1
