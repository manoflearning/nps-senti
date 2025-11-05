from crawl.core.extract.extractor import Extractor
from crawl.core.models import Candidate, FetchResult
from crawl.core.config import QualityConfig
from datetime import datetime, timezone


def _build_fetch_result(html: str) -> FetchResult:
    return FetchResult(
        url="https://www.example.com/view?code=freeb&No=1",
        fetched_from="live",
        status_code=200,
        html=html,
        snapshot_url="https://www.example.com/view?code=freeb&No=1",
        encoding="utf-8",
        fetched_at=datetime.now(timezone.utc),
    )


def test_extractor_uses_head_title_when_json_missing():
    # Simulate trafilatura returning plain text with no title by crafting minimal HTML
    html = """
    <html><head>
      <meta property="og:title" content="정상 제목" />
    </head>
    <body><p>본문</p></body></html>
    """
    extractor = Extractor(
        keywords=["국민연금"],
        allowed_languages=["ko"],
        quality_config=QualityConfig(min_keyword_hits=0),
    )
    cand = Candidate(
        url="https://www.example.com/view?code=freeb&No=1",
        source="bobaedream",
        discovered_via={"type": "forum"},
        title="깨진 제목",  # should be ignored by fallback
    )
    doc, quality = extractor.build_document(cand, _build_fetch_result(html), run_id="t")
    assert doc is not None
    assert doc.title == "정상 제목"
