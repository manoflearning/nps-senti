from datetime import datetime

from crawl.core.extract.extractor import Extractor
from crawl.core.models import Candidate
from crawl.core.config import QualityConfig
from crawl.core.models import FetchResult


def test_youtube_augmentation_without_api_key(monkeypatch):
    # Ensure no API key in env
    monkeypatch.delenv("YOUTUBE_API_KEY", raising=False)

    extractor = Extractor(
        keywords=["국민연금"],
        allowed_languages=["ko"],
        quality_config=QualityConfig(
            min_score=0.0,
            min_keyword_coverage=0.0,
            min_characters=1,
            min_keyword_hits=0,
        ),
    )

    candidate = Candidate(
        url="https://www.youtube.com/watch?v=abc123",
        source="youtube",
        discovered_via={"type": "youtube"},
        title="영상 제목",
        extra={
            "youtube": {
                "id": "abc123",
                "snippet": {
                    "title": "영상 제목",
                    "description": "국민연금 설명 텍스트",
                },
                "statistics": {"viewCount": 10},
            }
        },
    )
    fetch_result = FetchResult(
        url=candidate.url,
        fetched_from="live",
        status_code=200,
        html="",
        snapshot_url=candidate.url,
        encoding="utf-8",
        fetched_at=datetime.utcnow(),
    )

    doc, quality = extractor.build_document(candidate, fetch_result, run_id="test")
    assert doc is not None
    assert "국민연금" in doc.text
    assert doc.title == "영상 제목"
