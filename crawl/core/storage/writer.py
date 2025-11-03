from __future__ import annotations

import json
from pathlib import Path

from ..models import Document


class MultiSourceJsonlWriter:
    """Write documents into separate JSONL files per source.

    Rules:
    - Forums go to `forum_{source}.jsonl` (e.g., forum_dcinside.jsonl)
    - Other sources go to `{source}.jsonl` (e.g., gdelt.jsonl, youtube.jsonl)
    """

    def __init__(self, output_root: Path) -> None:
        self.output_root = output_root
        self.output_dir = output_root
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _file_path_for(self, document: Document) -> Path:
        discovered_type = None
        if isinstance(document.discovered_via, dict):
            discovered_type = document.discovered_via.get("type")
        source = document.source or "unknown"
        if discovered_type == "forum":
            file_name = f"forum_{source}.jsonl"
        else:
            file_name = f"{source}.jsonl"
        return self.output_dir / file_name

    def append(self, document: Document) -> None:
        file_path = self._file_path_for(document)
        with file_path.open("a", encoding="utf-8") as fh:
            record = {
                "id": document.id,
                "source": document.source,
                "url": document.url,
                "snapshot_url": document.snapshot_url,
                "title": document.title,
                "text": document.text,
                "lang": document.lang,
                "published_at": document.published_at,
                "authors": document.authors,
                "discovered_via": document.discovered_via,
                "quality": document.quality,
                "dup": document.dup,
                "crawl": document.crawl,
                "extra": document.extra,
            }
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")


class JsonlWriter:
    def __init__(
        self,
        output_root: Path,
        run_id: str,
        file_name: str | None = None,
    ) -> None:
        self.output_root = output_root
        self.run_id = run_id
        self.output_dir = output_root
        self.output_dir.mkdir(parents=True, exist_ok=True)
        if file_name:
            if not file_name.endswith(".jsonl"):
                file_name = f"{file_name}.jsonl"
            self.file_path = self.output_dir / file_name
        else:
            self.file_path = self.output_dir / f"{run_id}.jsonl"

    def append(self, document: Document) -> None:
        with self.file_path.open("a", encoding="utf-8") as fh:
            record = {
                "id": document.id,
                "source": document.source,
                "url": document.url,
                "snapshot_url": document.snapshot_url,
                "title": document.title,
                "text": document.text,
                "lang": document.lang,
                "published_at": document.published_at,
                "authors": document.authors,
                "discovered_via": document.discovered_via,
                "quality": document.quality,
                "dup": document.dup,
                "crawl": document.crawl,
                "extra": document.extra,
            }
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
