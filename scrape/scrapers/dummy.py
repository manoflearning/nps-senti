import datetime as dt
import requests
from scrape.base_scraper import BaseScraper
from typing import override


class DummyScraper(BaseScraper):
    def _build_request_params(
        self, keyword: str, start_date: dt.date, end_date: dt.date
    ) -> dict:
        return super()._build_request_params(keyword, start_date, end_date)

    def _make_request(self, params: dict) -> requests.Response:
        return super()._make_request(params)

    def _parse_response(self, response: requests.Response) -> list[dict]:
        return super()._parse_response(response)

    @override
    def scrape(
        self, keyword: str, start_date: dt.date, end_date: dt.date
    ) -> list[dict]:
        return [
            {
                "title": "What is the answer to life, universe, and everything?",
                "link": "https://youtu.be/0lngjO5bV2Y?si=CV6tEfBG-Pw8rXjB",
                "date": "2358-13-21T00:00:00",
                "content": "The answer is 42.",
            },
            {
                "title": "What is the answer to life, universe, and everything?",
                "link": "https://youtu.be/0lngjO5bV2Y?si=CV6tEfBG-Pw8rXjB",
                "date": "2358-13-21T00:00:00",
                "content": "The answer is 42.",
            },
            {
                "title": "What is the answer to life, universe, and everything?",
                "link": "https://youtu.be/0lngjO5bV2Y?si=CV6tEfBG-Pw8rXjB",
                "date": "2358-13-21T00:00:00",
                "content": "The answer is 42.",
            },
        ]
