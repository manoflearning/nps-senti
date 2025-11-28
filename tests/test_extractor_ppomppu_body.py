from bs4 import BeautifulSoup

from crawl.core.config import QualityConfig
from crawl.core.extract.extractor import Extractor


def _build_extractor() -> Extractor:
    return Extractor(
        keywords=["국민연금"],
        allowed_languages=["ko"],
        quality_config=QualityConfig(min_keyword_hits=0),
    )


def test_ppomppu_body_td_board_contents():
    html = """
    <html><body>
      <table><tr><td class="board-contents" align="left" valign=top class=han>
        <p>첫줄</p><p>&nbsp;</p><p>둘째</p>
      </td></tr></table>
    </body></html>
    """
    extractor = _build_extractor()
    soup = BeautifulSoup(html, "html.parser")
    text = extractor._extract_forum_body_text("ppomppu", soup, html)
    assert text == "첫줄 둘째"
