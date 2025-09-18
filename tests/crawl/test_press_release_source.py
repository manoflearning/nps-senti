from __future__ import annotations

import io
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator
from unittest import mock

import pytest

from nps_senti.core.config import Config
from nps_senti.crawl.models import RawItem
from nps_senti.crawl.run import run as crawl_run
from nps_senti.crawl.sources.base import BaseSource
from nps_senti.crawl.sources.press_release import ListingEntry, PressReleaseSource


FIXTURES = Path(__file__).parent.parent / "fixtures" / "crawl"


def load_fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def fake_response(text: str) -> io.BytesIO:
    buffer = io.BytesIO(text.encode("utf-8"))
    buffer.getcode = lambda: 200  # type: ignore[attr-defined]
    buffer.__enter__ = lambda self=buffer: self  # type: ignore[attr-defined]
    buffer.__exit__ = lambda self, exc_type, exc, tb: False  # type: ignore[attr-defined]
    return buffer


class CountingSource(BaseSource):
    def __init__(self, items: Iterator[RawItem]):
        self._items = list(items)
        self.last_seen_snapshot: list[str] = []

    @property
    def source_id(self) -> str:
        return "stub_source"

    def iter_items(self, seen_ids: set[str]) -> Iterator[RawItem]:
        self.last_seen_snapshot = sorted(seen_ids)
        yield from self._items


def test_parse_listing_extracts_entries():
    html = load_fixture("press_listing_page1.html")
    source = PressReleaseSource()

    entries = source.parse_listing(html)

    assert isinstance(entries, list)
    assert [type(e) for e in entries] == [ListingEntry, ListingEntry]
    assert entries[0].item_id == "202311"
    assert entries[0].title == "국민연금공단, 새로운 제도 발표"
    assert entries[0].published_at == datetime(2023, 11, 2, tzinfo=timezone.utc)
    assert entries[1].item_id == "202310"
    assert entries[1].url.endswith("seq=202310")


def test_parse_detail_extracts_content_and_attachments():
    html = load_fixture("press_detail_202311.html")
    source = PressReleaseSource()

    detail = source.parse_detail(html)

    assert "새로운 서비스를 도입한다" in detail.content
    assert detail.attachments == [
        "https://www.nps.or.kr/files/press/202311/policy.pdf"
    ]
    assert detail.raw_html.startswith("<html>")


def test_iter_items_skips_seen_and_stops_when_page_has_only_seen(monkeypatch: pytest.MonkeyPatch):
    page1 = load_fixture("press_listing_page1.html")
    page2 = load_fixture("press_listing_page2.html")
    detail = load_fixture("press_detail_202311.html")

    responses: dict[str, str] = {
        "https://www.nps.or.kr/jsppage/news/pressrelease/list.jsp?page=1": page1,
        "https://www.nps.or.kr/jsppage/news/pressrelease/list.jsp?page=2": page2,
        "https://www.nps.or.kr/jsppage/news/pressrelease/view.jsp?seq=202311": detail,
    }

    def _fake_urlopen(request, *args, **kwargs):  # type: ignore[override]
        if hasattr(request, "full_url"):
            url = request.full_url
        else:
            url = request
        try:
            return fake_response(responses[url])
        except KeyError:  # pragma: no cover - unexpected URL should fail test
            raise AssertionError(f"Unexpected URL requested: {url}")

    with mock.patch("urllib.request.urlopen", _fake_urlopen):
        source = PressReleaseSource()
        seen_ids = {"202310", "202309"}

        items = list(source.iter_items(seen_ids))

    assert len(items) == 1
    item = items[0]
    assert item.item_id == "202311"
    assert item.source == "nps_press_release"
    assert item.attachments == [
        "https://www.nps.or.kr/files/press/202311/policy.pdf"
    ]


def test_run_merges_existing_and_new_records(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    cfg = Config(data_dir=tmp_path)
    cfg.raw_dir.mkdir(parents=True, exist_ok=True)

    raw_path = cfg.raw_dir / "press_releases.jsonl"
    existing_record = {
        "source": "nps_press_release",
        "item_id": "202310",
        "url": "https://www.nps.or.kr/jsppage/news/pressrelease/view.jsp?seq=202310",
        "title": "국민연금공단, 기존 소식",
        "content": "기존 내용",
        "published_at": "2023-10-28T00:00:00+00:00",
        "fetched_at": "2023-10-29T10:00:00Z",
    }
    raw_path.write_text(json.dumps(existing_record, ensure_ascii=False) + "\n", encoding="utf-8")

    new_item = RawItem(
        source="stub_source",
        item_id="new-1",
        url="https://example.com/new-1",
        title="새 소식",
        content="새로운 소식 본문",
        published_at=datetime(2024, 1, 2, tzinfo=timezone.utc),
        attachments=["https://example.com/new-1.pdf"],
        raw_html="<html></html>",
    )

    stub_source = CountingSource(iter([new_item]))

    def fake_get_sources():
        return [stub_source]

    monkeypatch.setattr("nps_senti.crawl.run.get_sources", fake_get_sources)

    crawl_run(cfg)

    written = raw_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(written) == 2

    records = [json.loads(line) for line in written]
    sort_keys = [record["item_id"] for record in records]
    assert sort_keys == ["202310", "new-1"]

    existing = next(r for r in records if r["item_id"] == "202310")
    assert existing["attachments"] == []
    assert existing.get("raw_html", "") == ""

    new = next(r for r in records if r["item_id"] == "new-1")
    assert new["attachments"] == ["https://example.com/new-1.pdf"]
    assert new["fetched_at"].endswith("Z")
    assert stub_source.last_seen_snapshot == ["202310"]
