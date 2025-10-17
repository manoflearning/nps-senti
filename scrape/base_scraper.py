from abc import ABC, abstractmethod
import datetime as dt
import requests


class BaseScraper(ABC):
    @abstractmethod
    def _build_request_params(
        self, keyword: str, start_date: dt.date, end_date: dt.date
    ) -> dict:
        """Build request parameters for the target API.

        Args:
            keyword: Search term to query.
            start_date: Inclusive start date for results.
            end_date: Inclusive end date for results.

        Returns:
            A dict of querystring parameters for the HTTP request.
        """
        pass

    @abstractmethod
    def _make_request(self, params: dict) -> requests.Response:
        """Send the HTTP request and return the raw response.

        Args:
            params: Query parameters to include in the request.

        Returns:
            The `requests.Response` object from the server.
        """
        pass

    @abstractmethod
    def _parse_response(self, response: requests.Response) -> list[dict]:
        """Parse the HTTP response into structured items.

        Args:
            response: A successful HTTP response to parse.

        Returns:
            A list of dicts, each representing one scraped item.
        """
        pass

    def scrape(
        self, keyword: str, start_date: dt.date, end_date: dt.date
    ) -> list[dict]:
        """Run the full scraping workflow and return items.

        Steps:
            1) Build request params, 2) request, 3) validate, 4) parse.

        Returns:
            A list of parsed item dicts.
        """
        params = self._build_request_params(keyword, start_date, end_date)
        response = self._make_request(params=params)
        response.raise_for_status()  # Pass only 2xx response
        items = self._parse_response(response=response)
        # TODO: filter by date range
        return items
