from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

__all__ = ["RawItem", "ensure_utc", "utc_now"]


@dataclass(frozen=True, slots=True)
class RawItem:
    """Canonical representation of a crawled record before serialization."""

    source: str
    item_id: str
    url: str
    title: str
    content: str
    published_at: datetime
    attachments: list[str]
    raw_html: str

    def to_record(self, *, fetched_at: datetime | None = None) -> dict[str, Any]:
        fetched = ensure_utc(fetched_at or utc_now())
        return {
            "source": self.source,
            "item_id": self.item_id,
            "url": self.url,
            "title": self.title,
            "content": self.content,
            "published_at": ensure_utc(self.published_at)
            .isoformat()
            .replace("+00:00", "Z"),
            "fetched_at": fetched.isoformat().replace("+00:00", "Z"),
            "attachments": list(self.attachments),
            "raw_html": self.raw_html,
        }


def ensure_utc(value: datetime) -> datetime:
    """Return a timezone-aware datetime in UTC."""

    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
