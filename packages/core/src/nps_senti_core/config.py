from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Config:
    data_dir: Path

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def processed_dir(self) -> Path:
        return self.data_dir / "processed"

    @property
    def artifacts_dir(self) -> Path:
        return self.data_dir / "artifacts"

    @property
    def reports_dir(self) -> Path:
        return self.data_dir.parent / "reports"

    @property
    def db_path(self) -> Path:
        return self.data_dir / "nps.db"

    @classmethod
    def from_env(cls, *, data_dir: str | os.PathLike[str] | None = None) -> "Config":
        base = Path(data_dir) if data_dir else Path(os.getenv("NPS_DATA_DIR", "./data"))
        return cls(data_dir=base.resolve())


def ensure_data_dirs(cfg: Config) -> list[Path]:
    paths = [
        cfg.data_dir,
        cfg.raw_dir,
        cfg.processed_dir,
        cfg.artifacts_dir,
        cfg.reports_dir,
    ]
    created: list[Path] = []
    for p in paths:
        if not p.exists():
            p.mkdir(parents=True, exist_ok=True)
            created.append(p)
    return created
