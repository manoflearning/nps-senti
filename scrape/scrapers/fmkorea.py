import datetime as dt
import logging
import re
import time
import urllib.parse
from pathlib import Path
from typing import Any, Dict, Optional, Set

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from scrape.base_scraper import BaseScraper


class FmkoreaScraper(BaseScraper):
    """Collect FMKorea posts and their comments using Selenium."""

    SEARCH_BASE_URL = "https://www.fmkorea.com/search.php"

    def __init__(
        self,
        board: Optional[str] = None,
        headless: bool = True,
        max_posts: int = 50,
        max_pages: int = 5,
    ) -> None:
        super().__init__()
        self.board = board or "best"
        self.headless = headless
        self.max_posts = max_posts
        self.max_pages = max_pages
        self.logger = logging.getLogger(self.__class__.__name__)
        self.page_wait_seconds = 10
        self._dumped_article_ids: Set[str] = set()

    def _build_request_params(
        self, keyword: str, start_date: dt.date, end_date: dt.date
    ) -> Dict[str, Any]:
        # REST pipeline is not used for Selenium-based scraper
        return {}

    def _make_request(self, params: Dict[str, Any]):
        raise NotImplementedError("FmkoreaScraper operates via Selenium only")

    def _parse_response(self, response):
        raise NotImplementedError("FmkoreaScraper operates via Selenium only")

    def scrape(
        self, keyword: str, start_date: dt.date, end_date: dt.date
    ) -> list[dict[str, Any]]:
        driver = self._build_driver()

        try:
            return self._collect_posts(driver, keyword, start_date, end_date)
        finally:
            driver.quit()

    def _build_driver(self) -> webdriver.Chrome:
        options = Options()
        if self.headless:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--window-size=1280,720")
        return webdriver.Chrome(options=options)

    def _collect_posts(
        self,
        driver: webdriver.Chrome,
        keyword: str,
        start_date: dt.date,
        end_date: dt.date,
    ) -> list[dict[str, Any]]:
        collected: list[dict[str, Any]] = []
        seen_links: set[str] = set()

        for page in range(1, self.max_pages + 1):
            search_url = self._build_search_url(keyword, page=page)
            self.logger.info("Fetching list page: %s", search_url)
            driver.get(search_url)
            self._wait_for_list_page(driver)

            rows = self._find_list_rows(driver)
            if not rows:
                self.logger.warning("No list rows found on %s; dumping page source", search_url)
                self._dump_page_source(driver, f"list_page_{page}")
                break

            for row in rows:
                try:
                    post_meta = self._extract_list_row(row)
                except NoSuchElementException:
                    continue

                if post_meta is None:
                    continue

                post_dt = post_meta["datetime"]
                if post_dt.date() > end_date:
                    continue

                if post_dt.date() < start_date:
                    self.logger.info("Encountered posts older than start date; stopping")
                    return collected

                link = post_meta["link"]
                if link in seen_links:
                    continue

                post_detail = self._scrape_article(driver, link)
                post_detail.update(
                    {
                        "title": post_meta["title"],
                        "link": link,
                        "search_keyword": keyword,
                        "date": post_dt.isoformat(),
                    }
                )
                collected.append(post_detail)
                seen_links.add(link)

                if len(collected) >= self.max_posts:
                    return collected

            time.sleep(1.5)

        return collected

    def _wait_for_list_page(self, driver: webdriver.Chrome) -> None:
        candidate_selectors = [
            "table.bd_lst tbody tr:not(.notice)",
            "table.board tbody tr:not(.notice)",
            "div.bd_lst_wrp div.list div",
            "div.fm_best_widget li.li",
            "div.fmkorea_best_widget li.li",
        ]

        for selector in candidate_selectors:
            try:
                WebDriverWait(driver, self.page_wait_seconds).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))
                )
                return
            except TimeoutException:
                continue

        self.logger.debug("Timed out waiting for list selectors on %s", driver.current_url)

    def _dump_page_source(self, driver: webdriver.Chrome, label: str) -> None:
        debug_dir = Path("results") / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        target = debug_dir / f"fmkorea_{label}_{timestamp}.html"
        target.write_text(driver.page_source, encoding="utf-8")
        self.logger.info("Saved debug page source to %s", target)

    def _build_search_url(self, keyword: str, page: int) -> str:
        params = {
            "mid": self.board,
            "search_target": "title_content",
            "search_keyword": keyword,
            "page": page,
        }
        query = urllib.parse.urlencode(params, quote_via=urllib.parse.quote_plus)
        return f"{self.SEARCH_BASE_URL}?{query}"

    def _find_list_rows(self, driver: webdriver.Chrome):
        selectors = [
            "table.bd_lst tbody tr:not(.notice)",
            "table.board tbody tr:not(.notice)",
            "div.bd_lst_wrp div.list-item",
            "div.fm_best_widget li.li",
            "div.fmkorea_best_widget li.li",
            "ul.fmkorea_best_list li.li",
        ]
        for selector in selectors:
            rows = driver.find_elements(By.CSS_SELECTOR, selector)
            if rows:
                return rows
        return []

    def _extract_list_row(self, row) -> Optional[Dict[str, Any]]:
        classes = row.get_attribute("class") or ""
        tag_name = (row.tag_name or "").lower()

        if "notice" in classes or "empty" in classes:
            return None

        if tag_name == "li" or "li_best" in classes or "fmkorea_best" in classes:
            return self._extract_card_row(row)

        title_elem = self._first_present(
            row,
            [
                ("td.title a", True),
                ("td.bd_td p.title a", True),
                ("div.title a", True),
            ],
        )
        if title_elem is None:
            return None

        title = title_elem.text.strip()
        link = title_elem.get_attribute("href")
        if not link:
            return None

        date_elem = self._first_present(
            row,
            [
                ("td.time", False),
                ("td.date", False),
                ("div.time", False),
            ],
        )
        date_text = date_elem.text.strip() if date_elem else ""
        parsed_dt = self._parse_datetime(date_text)
        if parsed_dt is None:
            parsed_dt = dt.datetime.now()

        return {"title": title, "link": link, "datetime": parsed_dt}

    def _extract_card_row(self, row) -> Optional[Dict[str, Any]]:
        try:
            anchor_candidates = row.find_elements(By.CSS_SELECTOR, "h3.title a")
        except NoSuchElementException:
            anchor_candidates = []

        anchor = anchor_candidates[0] if anchor_candidates else None
        if anchor is None:
            return None

        ellipsis = anchor.find_elements(By.CSS_SELECTOR, ".ellipsis-target")
        if ellipsis:
            title = ellipsis[0].text.strip()
        else:
            title = anchor.text.strip()
        link = anchor.get_attribute("href")
        if not link:
            return None

        regdate_elem = row.find_elements(By.CSS_SELECTOR, "span.regdate")
        regdate = regdate_elem[0] if regdate_elem else None
        date_text = regdate.text.strip() if regdate else ""
        parsed_dt = self._parse_datetime(date_text)

        if parsed_dt is None and regdate is not None:
            inner_html = regdate.get_attribute("innerHTML") or ""
            comment_match = re.search(r"\d{4}\.\d{2}\.\d{2}", inner_html)
            if comment_match:
                parsed_dt = self._parse_datetime(comment_match.group(0))

        if parsed_dt is None:
            parsed_dt = dt.datetime.now()

        return {"title": title, "link": link, "datetime": parsed_dt}

    def _scrape_article(self, driver: webdriver.Chrome, link: str) -> Dict[str, Any]:
        parent_handle = driver.current_window_handle
        driver.switch_to.new_window("tab")
        try:
            driver.get(link)
            time.sleep(2.0)

            doc_id = self._extract_document_id(link)
            if doc_id and doc_id not in self._dumped_article_ids:
                self._dump_page_source(driver, f"article_{doc_id}")
                self._dumped_article_ids.add(doc_id)

            content = self._extract_article_body(driver)
            content = self._clean_article_text(content, link)
            self._wait_for_comments(driver)
            comments = self._extract_comments(driver)

            return {
                "content": content,
                "comments": comments,
                "comment_count": len(comments),
            }
        finally:
            driver.close()
            driver.switch_to.window(parent_handle)

    def _clean_article_text(self, text: str, link: str) -> str:
        link = link or ""
        filtered = []
        for raw_line in text.splitlines():
            stripped = raw_line.strip()
            if not stripped:
                if filtered and filtered[-1] != "":
                    filtered.append("")
                continue
            if stripped.startswith(link) or stripped.startswith(link.rstrip('/')):
                continue
            if stripped.endswith(" 복사") and stripped[:-3].startswith("https://www.fmkorea.com"):
                continue
            if stripped == "복사":
                continue
            filtered.append(stripped)

        cleaned = []
        prev_blank = False
        for line in filtered:
            if not line:
                if not prev_blank:
                    cleaned.append("")
                prev_blank = True
            else:
                cleaned.append(line)
                prev_blank = False

        return "\n".join(cleaned).strip()

    def _extract_article_body(self, driver: webdriver.Chrome) -> str:
        selectors = [
            "div.rd__content",
            "div.rd_hd + div.rd_body",
            "div.read_body div.xe_content",
            "article[data-role='content']",
        ]
        for selector in selectors:
            elems = driver.find_elements(By.CSS_SELECTOR, selector)
            for elem in elems:
                text = elem.text.strip()
                if text:
                    return text
        return ""

    def _wait_for_comments(self, driver: webdriver.Chrome) -> None:
        selectors = [
            "div.fdb_lst_wrp",
            "div[id$='_comment']",
            "div.comment-box",
            "#comment",
        ]
        for selector in selectors:
            try:
                WebDriverWait(driver, self.page_wait_seconds).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))
                )
                return
            except TimeoutException:
                continue
        self.logger.debug("No comment container found on %s", driver.current_url)

    def _extract_comments(self, driver: webdriver.Chrome) -> list[str]:
        comment_texts: list[str] = []
        seen: set[str] = set()

        comment_selectors = [
            "div.fdb_lst_wrp div.comment-content .xe_content",
            "div.fdb_lst_wrp .comment-content-text",
            "div[id$='_comment'] .comment-content .xe_content",
            "div.comment-box .comment-content",
        ]

        for selector in comment_selectors:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for element in elements:
                text = element.text.strip()
                if text and text not in seen:
                    seen.add(text)
                    comment_texts.append(text)
            if comment_texts:
                break

        if not comment_texts:
            fallback_elements = driver.find_elements(
                By.CSS_SELECTOR, "div.fdb_lst_wrp .xe_content"
            )
            for element in fallback_elements:
                text = element.text.strip()
                if text and text not in seen and text:
                    seen.add(text)
                    comment_texts.append(text)

        return comment_texts

    def _parse_datetime(self, value: str) -> Optional[dt.datetime]:
        value = value.strip()
        if not value:
            return None

        relative_dt = self._parse_relative_datetime(value)
        if relative_dt is not None:
            return relative_dt

        patterns = [
            "%Y.%m.%d %H:%M",
            "%Y-%m-%d %H:%M",
            "%Y.%m.%d",
            "%Y-%m-%d",
            "%m.%d %H:%M",
            "%m-%d %H:%M",
        ]

        for pattern in patterns:
            try:
                parsed = dt.datetime.strptime(value, pattern)
                if "%Y" not in pattern:
                    parsed = parsed.replace(year=dt.datetime.now().year)
                return parsed
            except ValueError:
                continue

        if value.count(":") == 1 and value.replace(":", "").isdigit():
            hour, minute = value.split(":")
            try:
                return dt.datetime.combine(
                    dt.date.today(), dt.time(int(hour), int(minute))
                )
            except ValueError:
                return None
        return None

    def _parse_relative_datetime(self, text: str) -> Optional[dt.datetime]:
        normalized = text.strip()
        if not normalized:
            return None

        now = dt.datetime.now()

        lower = normalized.lower()
        if lower in {"방금", "지금", "방금 전"}:
            return now
        if lower == "어제":
            return now - dt.timedelta(days=1)
        if lower == "그제" or lower == "엊그제":
            return now - dt.timedelta(days=2)

        match = re.match(r"(\d+)\s*(분|시간|일)\s*전", normalized)
        if match:
            amount = int(match.group(1))
            unit = match.group(2)
            if unit == "분":
                return now - dt.timedelta(minutes=amount)
            if unit == "시간":
                return now - dt.timedelta(hours=amount)
            if unit == "일":
                return now - dt.timedelta(days=amount)

        return None


    def _extract_document_id(self, link: str) -> Optional[str]:
        if not link:
            return None

        parsed = urllib.parse.urlparse(link)
        query = urllib.parse.parse_qs(parsed.query)
        ids = query.get("document_srl")
        if ids:
            return ids[0]

        parts = [p for p in parsed.path.split("/") if p]
        for idx, part in enumerate(parts):
            if part == "best" and idx + 1 < len(parts):
                candidate = parts[idx + 1]
                if candidate.isdigit():
                    return candidate
        return None

    def _first_present(self, root, selector_flags: list[tuple[str, bool]]):
        for selector, required in selector_flags:
            elements = root.find_elements(By.CSS_SELECTOR, selector)
            if elements:
                return elements[0]
            if required:
                break
        return None
