from datetime import datetime, timezone

from crawl.core.config import QualityConfig
from crawl.core.extract.extractor import ExtractionResult, Extractor
from crawl.core.models import Candidate, FetchResult


def _build_fetch_result(html: str = "") -> FetchResult:
    return FetchResult(
        url="https://www.example.com/view?code=freeb&No=1",
        fetched_from="live",
        status_code=200,
        html=html,
        snapshot_url="https://www.example.com/view?code=freeb&No=1",
        encoding="utf-8",
        fetched_at=datetime.now(timezone.utc),
    )


def test_forum_published_at_from_text_two_digit_year():
    extractor = Extractor(
        keywords=["국민연금"],
        allowed_languages=["ko"],
        quality_config=QualityConfig(min_keyword_hits=0),
    )

    def fake_trafilatura(_html: str, _url: str) -> ExtractionResult:
        return ExtractionResult(
            text="수정 25.11.16 09:56 | 내용",
            title="제목",
            authors=[],
            published_at=None,
        )

    extractor._run_trafilatura = fake_trafilatura  # type: ignore[method-assign]
    extractor._augment_forum = (
        lambda candidate, extraction, fetch_result: extraction  # type: ignore[method-assign]
    )

    cand = Candidate(
        url="https://www.example.com/view?code=freeb&No=1",
        source="bobaedream",
        discovered_via={"type": "forum"},
        title="원제",
    )
    doc, _ = extractor.build_document(cand, _build_fetch_result(), run_id="t")
    assert doc is not None
    assert doc.published_at and doc.published_at.startswith("2025-11-16T09:56:00")


def test_forum_published_at_falls_back_to_comments():
    extractor = Extractor(
        keywords=["국민연금"],
        allowed_languages=["ko"],
        quality_config=QualityConfig(min_keyword_hits=0),
    )

    def fake_trafilatura(_html: str, _url: str) -> ExtractionResult:
        return ExtractionResult(
            text="본문",
            title="제목",
            authors=[],
            published_at=None,
        )

    def fake_augment(candidate, extraction, fetch_result):  # type: ignore[no-untyped-def]
        forum = candidate.extra.setdefault("forum", {})
        forum["comments"] = [
            {"publishedAt": "25.11.16 17:24", "text": "c1"},
            {"publishedAt": "25.11.17 01:00", "text": "c2"},
        ]
        return extraction

    extractor._run_trafilatura = fake_trafilatura  # type: ignore[method-assign]
    extractor._augment_forum = fake_augment  # type: ignore[method-assign]

    cand = Candidate(
        url="https://www.example.com/view?code=freeb&No=1",
        source="bobaedream",
        discovered_via={"type": "forum"},
        title="원제",
    )
    doc, _ = extractor.build_document(cand, _build_fetch_result(), run_id="t")
    assert doc is not None
    assert doc.published_at and doc.published_at.startswith("2025-11-17T01:00:00")
