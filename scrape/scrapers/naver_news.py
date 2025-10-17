from scrape.base_scraper import BaseScraper
import datetime as dt
import requests
import os


class NaverNewsScraper(BaseScraper):
    def _build_request_params(
        self, keyword: str, start_date: dt.date, end_date: dt.date
    ) -> dict:
        params = {"query": keyword, "display": 100, "start": 1, "sort": "date"}
        return params

    def _make_request(self, params: dict) -> requests.Response:
        client_id = os.getenv("NAVER_NEWS_CLIENT_ID")
        client_secret = os.getenv("NAVER_NEWS_CLIENT_SECRET")

        headers = {}

        if client_id:
            headers["X-Naver-Client-Id"] = client_id
        if client_secret:
            headers["X-Naver-Client-Secret"] = client_secret

        return requests.get(
            "https://openapi.naver.com/v1/search/news.json",
            params=params,
            headers=headers,
        )

    def _parse_response(self, response: requests.Response) -> list[dict]:
        try:
            data = response.json()
        except ValueError:
            return []

        items = data.get("items") if isinstance(data, dict) else None
        return items if isinstance(items, list) else []
