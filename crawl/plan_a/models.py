from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional


@dataclass(slots=True)
class Candidate:
    url: str
    source: str
    discovered_via: Dict[str, object]
    snapshot_url: Optional[str] = None
    timestamp: Optional[datetime] = None
    title: Optional[str] = None
    extra: Dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class FetchResult:
    url: str
    fetched_from: str
    status_code: int
    html: Optional[str]
    snapshot_url: Optional[str]
    encoding: Optional[str]
    fetched_at: datetime


@dataclass(slots=True)
class Document:
    id: str
    source: str
    url: str
    snapshot_url: Optional[str]
    title: Optional[str]
    text: str
    lang: str
    published_at: Optional[str]
    authors: List[str]
    discovered_via: Dict[str, object]
    quality: Dict[str, object]
    dup: Dict[str, object]
    crawl: Dict[str, object]
    extra: Dict[str, object] = field(default_factory=dict)
