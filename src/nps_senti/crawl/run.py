from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from ..core.config import Config
from .models import RawItem, utc_now
from .sources.base import BaseSource
from .sources.press_release import PressReleaseSource

LOGGER = logging.getLogger(__name__)
_RECORD_FILE = "press_releases.jsonl"


def get_sources() -> list[BaseSource]:
    return [PressReleaseSource()]


def run(cfg: Config) -> None:
    path = cfg.raw_dir / _RECORD_FILE
    path.parent.mkdir(parents=True, exist_ok=True)

    existing_records, seen_ids = _load_existing(path)
    LOGGER.info("loaded %s existing records", len(existing_records))

    records_map = {record["item_id"]: record for record in existing_records}

    for source in get_sources():
        LOGGER.info("running crawler for %s", source.source_id)
        for item in source.iter_items(seen_ids):
            record = _serialize_item(item)
            records_map[item.item_id] = record
            seen_ids.add(item.item_id)
            LOGGER.debug("captured %s:%s", item.source, item.item_id)

    ordered = sorted(
        records_map.values(),
        key=lambda r: (r.get("published_at", ""), r["item_id"]),
    )

    with path.open("w", encoding="utf-8") as fh:
        for record in ordered:
            fh.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    LOGGER.info("wrote %s records to %s", len(ordered), path)


def _load_existing(path: Path) -> tuple[list[dict[str, Any]], set[str]]:
    if not path.exists():
        return [], set()

    records: list[dict[str, Any]] = []
    seen: set[str] = set()

    with path.open("r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError as exc:
                LOGGER.warning("skip malformed line %s: %s", line_no, exc)
                continue
            normalized = _normalize_record(data)
            records.append(normalized)
            seen.add(normalized["item_id"])

    return records, seen


def _normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    data = dict(record)
    data.setdefault("attachments", [])
    if not isinstance(data["attachments"], list):
        data["attachments"] = []
    data.setdefault("raw_html", "")
    data.setdefault("source", "nps_press_release")
    data.setdefault("content", "")
    data.setdefault("title", "")
    data.setdefault("url", "")
    data.setdefault("published_at", "")
    data.setdefault("fetched_at", utc_now().isoformat().replace("+00:00", "Z"))
    return data


def _serialize_item(item: RawItem) -> dict[str, Any]:
    return item.to_record()
