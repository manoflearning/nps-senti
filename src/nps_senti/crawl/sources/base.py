from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable, Iterator

from ..models import RawItem


class BaseSource(ABC):
    """Common interface for crawl sources."""

    @property
    @abstractmethod
    def source_id(self) -> str:
        """Stable identifier used in persisted records."""

    @abstractmethod
    def iter_items(self, seen_ids: set[str]) -> Iterator[RawItem]:
        """Yield new RawItems, optionally using `seen_ids` to skip duplicates."""

    def __iter__(self) -> Iterable[RawItem]:  # pragma: no cover - convenience
        return self.iter_items(set())
