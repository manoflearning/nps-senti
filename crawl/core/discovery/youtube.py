from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Optional

import requests

from ..models import Candidate

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class YouTubeConfig:
    max_results_per_keyword: int = 25


class YouTubeDiscoverer:
    SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
    VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"

    def __init__(
        self,
        api_key: Optional[str],
        keywords: Iterable[str],
        start_date: datetime,
        end_date: Optional[datetime],
        config: YouTubeConfig | None = None,
    ) -> None:
        self.api_key = api_key
        self.keywords = [kw for kw in keywords if kw.strip()]
        self.start_date = start_date
        self.end_date = end_date
        self.config = config or YouTubeConfig()

    def discover(self) -> List[Candidate]:
        if not self.api_key:
            logger.info("Skipping YouTube discoverer because API key is missing.")
            return []

        candidates: List[Candidate] = []
        published_after = self.start_date.isoformat().replace("+00:00", "Z")
        published_before = None
        if self.end_date:
            published_before = self.end_date.isoformat().replace("+00:00", "Z")

        for keyword in self.keywords:
            params = {
                "key": self.api_key,
                "part": "snippet",
                "type": "video",
                "order": "date",
                "q": keyword,
                "maxResults": str(self.config.max_results_per_keyword),
                "publishedAfter": published_after,
            }
            if published_before:
                params["publishedBefore"] = published_before

            try:
                response = requests.get(self.SEARCH_URL, params=params, timeout=30)
                response.raise_for_status()
            except requests.RequestException as exc:
                logger.warning("YouTube search request failed: %s", exc)
                continue
            items = response.json().get("items", [])
            video_ids = [
                item["id"]["videoId"]
                for item in items
                if "id" in item and "videoId" in item["id"]
            ]
            if not video_ids:
                continue
            details_params = {
                "key": self.api_key,
                "part": "snippet,contentDetails,statistics",
                "id": ",".join(video_ids),
            }
            try:
                details_resp = requests.get(
                    self.VIDEOS_URL, params=details_params, timeout=30
                )
                details_resp.raise_for_status()
            except requests.RequestException as exc:
                logger.warning("YouTube video details failed: %s", exc)
                continue
            details = {
                item["id"]: item
                for item in details_resp.json().get("items", [])
                if "id" in item
            }
            for item in items:
                vid = item["id"].get("videoId")
                if not vid:
                    continue
                snippet = details.get(vid, {}).get("snippet", item.get("snippet", {}))
                published_at = snippet.get("publishedAt")
                timestamp = None
                if published_at:
                    try:
                        timestamp = datetime.fromisoformat(
                            published_at.replace("Z", "+00:00")
                        )
                    except ValueError:
                        timestamp = None
                candidates.append(
                    Candidate(
                        url=f"https://www.youtube.com/watch?v={vid}",
                        source="youtube",
                        discovered_via={
                            "type": "youtube",
                            "keyword": keyword,
                        },
                        snapshot_url=None,
                        timestamp=timestamp,
                        title=snippet.get("title"),
                        extra={"youtube": details.get(vid, {})},
                    )
                )
        logger.info("YouTube discovered %d candidates", len(candidates))
        return candidates
