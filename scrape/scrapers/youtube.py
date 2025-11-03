from typing import Dict, List, Optional, cast
from scrape.base_scraper import BaseScraper
import datetime as dt
import requests
import os
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()


class YoutubeScraper(BaseScraper):
    SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
    COMMENTS_URL = "https://www.googleapis.com/youtube/v3/commentThreads"
    VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("YOUTUBE_API_KEY")
        if not self.api_key:
            raise RuntimeError(
                "YOUTUBE_API_KEY not found in environment, and api_key argument is empty."
            )
        self._last_search_params: Optional[dict] = None
        self.comment_tz: str = "Asia/Seoul"

    @staticmethod
    def _to_rfc3339_from_local(date: dt.date, tz_name: str, end: bool = False) -> str:
        tz = ZoneInfo(tz_name)
        local_dt = dt.datetime.combine(
            date + dt.timedelta(days=1 if end else 0),
            dt.time(0, 0, 0),
            tzinfo=tz,
        )
        utc_dt = local_dt.astimezone(dt.timezone.utc)
        return utc_dt.isoformat().replace("+00:00", "Z")

    @staticmethod
    def _to_local_dt_from_rfc3339(ts: str, tz_name: str) -> dt.datetime:
        utc = dt.datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(
            dt.timezone.utc
        )
        return utc.astimezone(ZoneInfo(tz_name))

    def _build_request_params(
        self, keyword: str, start_date: dt.date, end_date: dt.date
    ) -> dict:
        return {
            "part": "snippet",
            "q": keyword,
            "type": "video",
            "order": "date",
            "maxResults": 50,
            "publishedAfter": self._to_rfc3339_from_local(
                start_date, self.comment_tz, end=False
            ),
            "publishedBefore": self._to_rfc3339_from_local(
                end_date, self.comment_tz, end=True
            ),
        }

    def _make_request(self, params: dict) -> requests.Response:
        params = dict(params)
        params["key"] = self.api_key
        params["fields"] = "items(id/videoId),nextPageToken,pageInfo"
        self._last_search_params = dict(params)
        return requests.get(self.SEARCH_URL, params=params, timeout=20)

    def _parse_response(self, response: requests.Response) -> List[Dict]:
        try:
            data = response.json()
        except ValueError:
            return []

        video_ids: List[str] = []
        for it in data.get("items") or []:
            vid = (it.get("id") or {}).get("videoId")
            if vid:
                video_ids.append(vid)

        page_token = data.get("nextPageToken")
        if self._last_search_params is None:
            self._last_search_params = {}

        while page_token:
            next_params = dict(self._last_search_params)
            next_params["pageToken"] = page_token
            resp = requests.get(self.SEARCH_URL, params=next_params, timeout=20)
            if not resp.ok:
                break
            d2 = resp.json()
            for it in d2.get("items") or []:
                vid = (it.get("id") or {}).get("videoId")
                if vid:
                    video_ids.append(vid)
            page_token = d2.get("nextPageToken")

        start_date: Optional[dt.date] = getattr(self, "start_date", None)
        end_date: Optional[dt.date] = getattr(self, "end_date", None)
        use_comment_date_filter = bool(start_date and end_date)

        titles_map = self._get_video_titles(video_ids)

        all_comments: List[Dict] = []
        for vid in video_ids:
            all_comments.extend(
                self._fetch_comments_for_video(
                    video_id=vid,
                    video_title=titles_map.get(vid, ""),
                    include_replies=True,
                    text_format="plainText",
                    max_pages=None,
                    comment_start_date=start_date if use_comment_date_filter else None,
                    comment_end_date=end_date if use_comment_date_filter else None,
                    comment_tz=self.comment_tz,
                )
            )

        return all_comments

    def _get_video_titles(self, video_ids: List[str]) -> Dict[str, str]:
        titles: Dict[str, str] = {}
        if not video_ids:
            return titles

        for i in range(0, len(video_ids), 50):
            chunk = video_ids[i : i + 50]
            params = {
                "part": "snippet",
                "id": ",".join(chunk),
                "key": self.api_key,
                "fields": "items(id,snippet/title)",
                "maxResults": 50,
            }
            resp = requests.get(self.VIDEOS_URL, params=params, timeout=20)
            if not resp.ok:
                continue
            data = resp.json()
            for item in data.get("items") or []:
                vid = item.get("id")
                title = ((item.get("snippet") or {}).get("title")) or ""
                if vid:
                    titles[vid] = title
        return titles

    def _fetch_comments_for_video(
        self,
        video_id: str,
        video_title: str = "",
        include_replies: bool = True,
        text_format: str = "plainText",
        max_pages: Optional[int] = None,
        comment_start_date: Optional[dt.date] = None,
        comment_end_date: Optional[dt.date] = None,
        comment_tz: str = "Asia/Seoul",
    ) -> List[Dict]:
        params = {
            "part": "snippet,replies" if include_replies else "snippet",
            "videoId": video_id,
            "maxResults": 100,
            "textFormat": text_format,
            "order": "time",
            "key": self.api_key,
        }

        use_date_filter = (comment_start_date is not None) and (
            comment_end_date is not None
        )

        tz = ZoneInfo(comment_tz)
        if use_date_filter:
            start_d = cast(dt.date, comment_start_date)
            end_d = cast(dt.date, comment_end_date)
            start_local = dt.datetime.combine(start_d, dt.time(0, 0, 0), tzinfo=tz)
            end_local_excl = dt.datetime.combine(
                end_d + dt.timedelta(days=1), dt.time(0, 0, 0), tzinfo=tz
            )
        else:
            start_local = dt.datetime.min.replace(tzinfo=tz)
            end_local_excl = dt.datetime.max.replace(tzinfo=tz)

        def _pass_date(snippet: dict) -> bool:
            if not use_date_filter:
                return True
            ts = snippet.get("publishedAt")
            if not ts:
                return True
            local_dt = self._to_local_dt_from_rfc3339(ts, comment_tz)
            return start_local <= local_dt < end_local_excl

        def _fmt(snippet: dict) -> str:
            ts = snippet.get("publishedAt")
            if not ts:
                return ""
            return self._to_local_dt_from_rfc3339(ts, comment_tz).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

        results: List[Dict] = []
        page_token = None
        pages = 0
        hard_stop = False

        while True:
            if page_token:
                params["pageToken"] = page_token
            resp = requests.get(self.COMMENTS_URL, params=params, timeout=30)
            if resp.status_code != 200:
                break
            data = resp.json()

            for item in data.get("items", []):
                top = (item.get("snippet") or {}).get("topLevelComment") or {}
                top_sn = top.get("snippet") or {}

                if not _pass_date(top_sn):
                    hard_stop = True
                else:
                    results.append(
                        {
                            "source": "youtube",
                            "video_id": video_id,
                            "video_title": video_title,
                            "text": top_sn.get("textDisplay"),
                            "published_at": _fmt(top_sn),
                        }
                    )

                    if (
                        include_replies
                        and item.get("snippet", {}).get("totalReplyCount", 0) > 0
                    ):
                        for r in (item.get("replies") or {}).get("comments", []) or []:
                            rs = r.get("snippet") or {}
                            if not _pass_date(rs):
                                continue
                            results.append(
                                {
                                    "source": "youtube",
                                    "video_id": video_id,
                                    "video_title": video_title,
                                    "text": rs.get("textDisplay"),
                                    "published_at": _fmt(rs),
                                }
                            )

            if hard_stop:
                break

            page_token = data.get("nextPageToken")
            pages += 1
            if not page_token:
                break
            if max_pages and pages >= max_pages:
                break

        return results
