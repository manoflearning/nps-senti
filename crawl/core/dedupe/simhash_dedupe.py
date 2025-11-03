from __future__ import annotations

from dataclasses import dataclass

from simhash import Simhash

from ..models import Document


@dataclass(slots=True)
class DedupeConfig:
    threshold: int = 10


class SimhashDeduper:
    def __init__(self, config: DedupeConfig | None = None) -> None:
        self.config = config or DedupeConfig()
        self._groups: dict[str, Simhash] = {}

    def add(self, document: Document) -> bool:
        simhash_value = Simhash(document.text)
        for group_id, group_hash in self._groups.items():
            if simhash_value.distance(group_hash) <= self.config.threshold:
                document.dup["simhash"] = format(simhash_value.value, "016x")
                document.dup["group"] = group_id
                return True
        group_id = document.id
        self._groups[group_id] = simhash_value
        document.dup["simhash"] = format(simhash_value.value, "016x")
        document.dup["group"] = group_id
        return False
