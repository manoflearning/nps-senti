from crawl.core.fetch.fetcher import Fetcher
from crawl.core.models import Candidate
import requests


class DummyResp:
    def __init__(self, content: bytes):
        self.status_code = 200
        self.content = content
        self.headers = {"Content-Type": "text/html; charset=utf-8"}

    def raise_for_status(self):
        return None


def test_fetcher_respects_robots_override_flag(monkeypatch):
    session = requests.Session()

    # Patch session.get to return dummy response without network
    def _fake_get(url, headers=None, timeout=None):  # noqa: ARG002
        return DummyResp(b"<html><title>ok</title><body>text</body></html>")

    session.get = _fake_get  # type: ignore[method-assign]
    f = Fetcher(session=session, timeout=3)
    # Simulate robots disallow by forcing allowed() -> False
    f.robots.allowed = lambda _url: False  # type: ignore[attr-defined]
    cand = Candidate(
        url="https://mlbpark.donga.com/mp/b.php?b=bullpen&m=view&idx=1",
        source="mlbpark",
        discovered_via={"type": "forum"},
        extra={"robots_override": True},
    )
    res = f.fetch(cand)
    assert res is not None
    assert res.html and "text" in res.html
