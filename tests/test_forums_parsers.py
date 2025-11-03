from crawl.core.discovery.forums import ForumsDiscoverer, ForumSiteConfig


class DummyResp:
    def __init__(self, text: str, status_code: int = 200):
        self.text = text
        self.status_code = status_code


class DummySession:
    def __init__(self, html_by_url: dict[str, str]):
        self._html_by_url = html_by_url

    def get(self, url, timeout=None):  # noqa: ARG002
        # Return the same fixture for any page param variant of base URL
        base = url.split("?")[0]
        for key in self._html_by_url:
            if base.startswith(key) or url.startswith(key):
                return DummyResp(self._html_by_url[key])
        return DummyResp("", status_code=404)


def build_forums(session, site: str, html: str):
    cfg = {
        site: ForumSiteConfig(
            enabled=True,
            boards=["https://example.com/"],
            max_pages=1,
            per_board_limit=10,
        )
    }
    discoverer = ForumsDiscoverer(
        session=session, request_timeout=5, user_agent="test-agent", sites_config=cfg
    )
    # allow all robots
    discoverer.robots.allowed = lambda _url: True  # type: ignore[attr-defined]
    # Inject fixture mapping for base url
    session._html_by_url = {"https://example.com/": html}
    return discoverer


def test_dcinside_parser_extracts_meta():
    html = """
    <table><tbody>
      <tr>
        <td class="gall_tit"><a href="/mgallery/board/view/?id=%EA%B5%AD%EB%AF%BC%EC%97%B0%EA%B8%88&no=123">제목A</a></td>
        <td class="gall_writer">홍길동</td>
        <td class="gall_date" title="2025-11-03 16:20:10">방금</td>
      </tr>
    </tbody></table>
    """
    session = DummySession({})
    d = build_forums(session, "dcinside", html)
    per_site = d.discover()
    items = per_site["dcinside"]
    assert len(items) == 1
    c = items[0]
    assert "dcinside" == c.source
    assert c.title == "제목A"
    assert c.extra.get("forum", {}).get("site") == "dcinside"
    # timestamp parsed from title attr
    assert c.timestamp is not None


def test_bobaedream_parser_basic():
    html = """
    <table><tbody>
      <tr>
        <td class="tit"><a href="/board/bbs_view?code=freeb&No=1">보배 제목</a></td>
        <td class="author">작가</td>
        <td class="date">2025.11.01 12:34</td>
      </tr>
    </tbody></table>
    """
    session = DummySession({})
    d = build_forums(session, "bobaedream", html)
    items = d.discover()["bobaedream"]
    assert items and items[0].title == "보배 제목"
    assert items[0].timestamp is not None


def test_bobaedream_parser_view_pattern():
    # Current production listing links look like /view?code=freeb&No=...
    html = """
    <table><tbody>
      <tr>
        <td class="tit"><a class="bsubject" href="/view?code=freeb&No=123">현행 링크 제목</a></td>
        <td class="author">작성자</td>
        <td class="date">2025-11-03 10:20</td>
      </tr>
    </tbody></table>
    """
    session = DummySession({})
    d = build_forums(session, "bobaedream", html)
    items = d.discover()["bobaedream"]
    assert items and items[0].title == "현행 링크 제목"
    assert items[0].timestamp is not None


def test_fmkorea_parser_basic():
    html = """
    <table><tbody>
      <tr>
        <td class="title"><a href="/123456">에펨 제목</a></td>
        <td class="author">닉네임</td>
        <td class="time">2025-11-03 14:00</td>
      </tr>
    </tbody></table>
    """
    session = DummySession({})
    d = build_forums(session, "fmkorea", html)
    items = d.discover()["fmkorea"]
    assert items and items[0].title == "에펨 제목"
    assert items[0].timestamp is not None


def test_mlbpark_parser_basic():
    html = """
    <table><tbody>
      <tr>
        <td class="t_left"><a href="/mp/b.php?b=bullpen&m=view&idx=42">불펜 제목</a></td>
        <td class="nikcon">닉</td>
        <td class="date">2025-11-02</td>
      </tr>
    </tbody></table>
    """
    session = DummySession({})
    d = build_forums(session, "mlbpark", html)
    items = d.discover()["mlbpark"]
    assert items and items[0].title == "불펜 제목"
    assert items[0].timestamp is not None


def test_theqoo_parser_basic():
    html = """
    <table><tbody>
      <tr>
        <td class="title"><a href="/square/1234">더쿠 제목</a></td>
        <td class="nik">닉</td>
        <td class="time">2025/11/02 13:00</td>
      </tr>
    </tbody></table>
    """
    session = DummySession({})
    d = build_forums(session, "theqoo", html)
    items = d.discover()["theqoo"]
    assert items and items[0].title == "더쿠 제목"
    assert items[0].timestamp is not None


def test_ppomppu_parser_basic():
    html = """
    <table><tbody>
      <tr class="list0">
        <td class="subject"><a href="/zboard/view.php?id=freeboard&no=7">뽐뿌 제목</a></td>
        <td class="name">닉</td>
        <td class="date">2025-11-02 13:00</td>
      </tr>
    </tbody></table>
    """
    session = DummySession({})
    d = build_forums(session, "ppomppu", html)
    items = d.discover()["ppomppu"]
    assert items and items[0].title == "뽐뿌 제목"
    assert items[0].timestamp is not None


def test_pagination_and_limit():
    html = """
    <table><tbody>
      <tr>
        <td class="gall_tit"><a href="/mgallery/board/view/?id=x&no=1">A</a></td>
        <td class="gall_writer">a</td>
        <td class="gall_date">2025-11-03</td>
      </tr>
      <tr>
        <td class="gall_tit"><a href="/mgallery/board/view/?id=x&no=2">B</a></td>
        <td class="gall_writer">b</td>
        <td class="gall_date">2025-11-03</td>
      </tr>
    </tbody></table>
    """
    session = DummySession({})
    cfg = {
        "dcinside": ForumSiteConfig(
            enabled=True,
            boards=["https://example.com/"],
            max_pages=3,
            per_board_limit=3,
            pause_between_requests=0,
        )
    }
    d = ForumsDiscoverer(
        session=session, request_timeout=5, user_agent="ua", sites_config=cfg
    )
    d.robots.allowed = lambda _url: True  # type: ignore[attr-defined]
    session._html_by_url = {"https://example.com/": html}
    res = d.discover()["dcinside"]
    # 동일 글이 페이지마다 반복되어도 중복 없이 2건만 수집되어야 함
    assert len(res) == 2
