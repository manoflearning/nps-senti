from bs4 import BeautifulSoup

from crawl.core.config import QualityConfig
from crawl.core.extract.extractor import Extractor


def _build_extractor() -> Extractor:
    return Extractor(
        keywords=["국민연금"],
        allowed_languages=["ko"],
        quality_config=QualityConfig(min_keyword_hits=0),
    )


def test_theqoo_image_only_body_extracts_img_alt():
    html = """
    <html><body>
      <div class="xe_content">
        <p><img alt="이미지본문" src="https://example.com/a.jpg" /></p>
      </div>
    </body></html>
    """
    extractor = _build_extractor()
    soup = BeautifulSoup(html, "html.parser")
    text = extractor._extract_forum_body_text("theqoo", soup, html)
    assert text == "이미지본문"
