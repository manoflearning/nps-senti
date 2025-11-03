from __future__ import annotations

import json
from pathlib import Path
from typing import Set


class DocumentIndex:
    def __init__(self, output_dir: Path) -> None:
        # Unified index lives alongside output JSONL
        self.path = output_dir / "_index.json"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.ids: Set[str] = set()
        if self.path.exists():
            try:
                data = json.loads(self.path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    ids = data.get("ids", [])
                else:
                    ids = data
                self.ids.update(str(entry) for entry in ids)
            except (json.JSONDecodeError, OSError):
                # Corrupt or unreadable index; start fresh
                self.ids = set()
            self._dirty = False
        else:
            self._dirty = False
            # bootstrap from existing JSONL files if index is new
            for jsonl_file in self.path.parent.glob("*.jsonl"):
                try:
                    with jsonl_file.open("r", encoding="utf-8") as fh:
                        for line in fh:
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                record = json.loads(line)
                            except json.JSONDecodeError:
                                continue
                            doc_id = record.get("id")
                            if isinstance(doc_id, str):
                                self.ids.add(doc_id)
                except OSError:
                    continue
            if self.ids:
                self._dirty = True

    def contains(self, doc_id: str) -> bool:
        return doc_id in self.ids

    def add(self, doc_id: str) -> None:
        if doc_id not in self.ids:
            self.ids.add(doc_id)
            self._dirty = True

    def flush(self) -> None:
        if not self._dirty:
            return
        payload = {"ids": sorted(self.ids)}
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        self._dirty = False
