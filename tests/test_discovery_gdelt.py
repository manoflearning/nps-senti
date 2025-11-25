from datetime import datetime, timezone
from typing import Any, Protocol

from crawl.core.discovery.gdelt import GdeltDiscoverer, GdeltConfig
from crawl.core.models import Candidate


class DummyResp:
    status_code = 200

    def __init__(self, payload: Any):
        self._payload = payload

    def json(self):
        return self._payload

    def raise_for_status(self) -> None:
        return None


class _RequestsSessionProto(Protocol):
    def get(self, url: str, params=None, timeout=None): ...


class DummySession:
    def __init__(self, payload: Any):
        self.payload = payload

    def get(self, url: str, params=None, timeout=None):  # type: ignore[override]  # noqa: ARG002
        return DummyResp(self.payload)


def test_gdelt_seendate_with_time_is_parsed():
    payload = {
        "articles": [
            {
                "url": "https://example.com/a",
                "title": "t1",
                "seendate": "20251123T143000Z",
            }
        ]
    }
    session = DummySession(payload)
    gd = GdeltDiscoverer(
        session=session,  # type: ignore[arg-type]
        keywords=["test"],
        languages=["ko"],
        start_date=datetime(2025, 11, 20, tzinfo=timezone.utc),
        end_date=datetime(2025, 11, 25, tzinfo=timezone.utc),
        request_timeout=5,
        config=GdeltConfig(chunk_days=1, max_attempts=1, max_concurrency=1),
    )
    candidates = gd.discover()
    assert candidates
    cand: Candidate = candidates[0]
    assert cand.timestamp is not None
    assert cand.timestamp.tzinfo is not None
    assert cand.timestamp.year == 2025
    assert cand.timestamp.hour == 14 and cand.timestamp.minute == 30
