from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Iterator, Any


def write_jsonl(path: str | Path, items: Iterable[Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")


def read_jsonl(path: str | Path) -> Iterator[Any]:
    p = Path(path)
    if not p.exists():
        return iter(())
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)
