from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Optional

from ..models import Candidate, Document


def _month_bucket(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return f"{dt.year:04d}-{dt.month:02d}"


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


@dataclass(slots=True)
class YouTubeQuota:
    daily_quota: int = 1000
    reserve_quota: int = 200
    used_today: int = 0
    period_start_utc: str = ""  # YYYY-MM-DD

    def _today_key(self) -> str:
        return date.today().isoformat()

    def _ensure_day(self) -> None:
        today = self._today_key()
        if self.period_start_utc != today:
            self.period_start_utc = today
            self.used_today = 0

    def available(self) -> int:
        self._ensure_day()
        return max(0, self.daily_quota - self.reserve_quota - self.used_today)

    def can_consume(self, units: int) -> bool:
        return self.available() >= max(0, units)

    def consume(self, units: int) -> None:
        self._ensure_day()
        self.used_today += max(0, units)


@dataclass(slots=True)
class AutoState:
    version: int = 1
    # counts["YYYY-MM"]["source"] = stored_count
    counts: Dict[str, Dict[str, int]] = field(default_factory=dict)
    # total per source, cumulative
    stored_by_source: Dict[str, int] = field(default_factory=dict)
    youtube: YouTubeQuota = field(default_factory=YouTubeQuota)
    youtube_kw_cursor: int = 0
    last_updated: str = ""

    @classmethod
    def load(cls, path: Path) -> "AutoState":
        if not path.exists():
            return cls()
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return cls()
        state = cls()
        state.version = int(data.get("version", 1))
        state.counts = {
            str(k): {str(sk): int(vv) for sk, vv in v.items()}
            for k, v in data.get("counts", {}).items()
        }
        state.stored_by_source = {
            str(k): int(v) for k, v in data.get("stored_by_source", {}).items()
        }
        yt = data.get("youtube", {})
        state.youtube = YouTubeQuota(
            daily_quota=int(yt.get("daily_quota", 1000)),
            reserve_quota=int(yt.get("reserve_quota", 200)),
            used_today=int(yt.get("used_today", 0)),
            period_start_utc=str(yt.get("period_start_utc", "")),
        )
        state.youtube_kw_cursor = int(data.get("youtube_kw_cursor", 0))
        state.last_updated = str(data.get("last_updated", ""))
        return state

    def save(self, path: Path) -> None:
        payload = {
            "version": self.version,
            "counts": self.counts,
            "stored_by_source": self.stored_by_source,
            "youtube": {
                "daily_quota": self.youtube.daily_quota,
                "reserve_quota": self.youtube.reserve_quota,
                "used_today": self.youtube.used_today,
                "period_start_utc": self.youtube.period_start_utc,
            },
            "youtube_kw_cursor": self.youtube_kw_cursor,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def record_stored(self, document: Document, candidate: Candidate) -> None:
        # Prefer published_at, fallback to candidate timestamp, else now
        dt = (
            _parse_iso(document.published_at)
            or candidate.timestamp
            or datetime.now(timezone.utc)
        )
        bucket = _month_bucket(dt)
        per_src = self.counts.setdefault(bucket, {})
        per_src[candidate.source] = int(per_src.get(candidate.source, 0)) + 1
        self.stored_by_source[candidate.source] = (
            int(self.stored_by_source.get(candidate.source, 0)) + 1
        )
        self.last_updated = datetime.now(timezone.utc).isoformat()
