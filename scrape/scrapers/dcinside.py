# scrape/scrapers/dcinside.py
from __future__ import annotations

import os
import time
import re
import hashlib
import datetime as dt
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup

from scrape.base_scraper import BaseScraper

# 디시 검색 결과 페이지 (페이지네이션)
SEARCH_URL_TPL = "https://search.dcinside.com/post/p/{page}"

# 식별 가능한 UA 권장 (약관/robots 배려)
DEFAULT_UA = (
    "NPS-SentiBot/1.0 (+contact: you@yourdomain) "
    "respect-robots/1.0 purpose=research"
)

def _clean(s: Optional[str]) -> Optional[str]:
    if not s:
        return s
    return " ".join(s.split())

def _hash(*parts: Optional[str]) -> str:
    s = "|".join((p or "") for p in parts)
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def _parse_dt(text: Optional[str]) -> Optional[dt.datetime]:
    """
    디시 검색 결과에 흔한 날짜 포맷 예:
      - '2025.10.18 13:45'
      - '2025.10.18'
    """
    if not text:
        return None
    t = text.strip()
    for fmt in ("%Y.%m.%d %H:%M", "%Y.%m.%d"):
        try:
            return dt.datetime.strptime(t, fmt)
        except Exception:
            pass
    return None


class DcinsideScraper(BaseScraper):
    """
    디시인사이드 검색 결과 기반 수집기.
    - 공개 검색 페이지에서 목록만 수집(기본).
    - 상세 본문/댓글은 기본 비활성(약관/robots 확인 후 옵션으로 확장).
    - BaseScraper의 3개 추상메서드를 구현하되, 페이징을 위해 scrape()를 오버라이드.
    """

    source = "dcinside"

    # ---- A) 추상 메서드 구현: "단일 페이지" 요청 단위 ----
    def _build_request_params(self, keyword: str, start_date: dt.date, end_date: dt.date) -> dict:
        # 페이지 번호는 scrape()에서 __page로 주입
        return {
            "keyword": keyword,
            "search_pos": "0",
            "s_type": "search_subject_memo",  # 제목+본문
        }

    def _make_request(self, params: dict) -> requests.Response:
        page = params.pop("__page")
        url = SEARCH_URL_TPL.format(page=page)
        headers = {"User-Agent": os.getenv("SCRAPER_USER_AGENT", DEFAULT_UA)}
        # 디시 검색은 GET + 쿼리스트링
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        return resp

    def _parse_response(self, response: requests.Response) -> List[dict]:
        soup = BeautifulSoup(response.text, "lxml")
        rows: List[Dict] = []

        # 검색 결과 아이템 컨테이너(페이지 개편에 대비해 넓게)
        containers = soup.select(
            "div.box_result, ul.gall_detail li, div.wrap_result, div.result, div.sch_result li"
        )
        if not containers:
            containers = soup.select("a")  # 백업

        for box in containers:
            a = box.select_one("a")
            if not a or not a.get("href"):
                if box.name == "a" and box.get("href"):
                    a = box
                else:
                    continue

            href = a.get("href")
            title = _clean(a.get_text())

            # 메타 블록에서 날짜 후보 추출
            date_text = None
            meta = box.select_one(".desc, .wrap_txt, .txt, .etc_box, .etc, .gall_info")
            if meta:
                for s in meta.stripped_strings:
                    if _parse_dt(s):
                        date_text = s
                        break
            posted_dt = _parse_dt(date_text)
            posted_iso = posted_dt.isoformat() if posted_dt else None

            # 갤러리명 후보
            gallery = None
            g = box.select_one(".gall_name, .gall, .name, .gall_tit")
            if g:
                gallery = _clean(g.get_text())
            else:
                if meta:
                    mt = _clean(meta.get_text(" ", strip=True))
                    if mt:
                        gallery = _clean(mt.split("·")[0])

            hid = _hash(title, href, posted_iso)

            # 최소 스키마
            rows.append(
                {
                    "source": self.source,
                    "title": title,
                    "link": href,
                    "gallery": gallery,
                    "posted_at": posted_iso,  # 사이트 표기 기준(로컬) ISO
                    "hash_id": hid,
                }
            )
        return rows

    # ---- B) 다중 페이지 + 날짜필터 + 중복제거 ----
    def scrape(self, keyword: str, start_date: dt.date, end_date: dt.date) -> List[dict]:
        delay = float(os.getenv("DCINSIDE_DELAY_SEC", "1.0"))
        max_pages = int(os.getenv("DCINSIDE_MAX_PAGES", "10"))
        fetch_detail = os.getenv("DCINSIDE_FETCH_DETAIL", "false").lower() == "true"

        results: List[dict] = []
        seen = set()

        base_params = self._build_request_params(keyword, start_date, end_date)

        for page in range(1, max_pages + 1):
            params = dict(base_params)
            params["__page"] = page

            resp = self._make_request(params=params)
            resp.raise_for_status()
            items = self._parse_response(resp)

            if not items:
                break

            kept = 0
            for it in items:
                # 날짜 필터
                p = it.get("posted_at")
                if p:
                    try:
                        d = dt.datetime.fromisoformat(p).date()
                        if (start_date and d < start_date) or (end_date and d > end_date):
                            continue
                    except Exception:
                        pass

                hid = it.get("hash_id")
                if hid in seen:
                    continue
                seen.add(hid)

                # (옵션) 상세 본문 수집 — 기본 OFF (약관/robots 확인 후만 사용)
                if fetch_detail:
                    try:
                        it.update(self._fetch_detail(it.get("link")))
                    except Exception:
                        pass

                it["keyword"] = keyword
                it["collected_at"] = dt.datetime.now().astimezone().isoformat()
                results.append(it)
                kept += 1

            time.sleep(delay)
            if kept == 0:
                break

        return results

    # ---- C) 상세 본문(옵션; 기본 OFF) ----
    def _fetch_detail(self, url: Optional[str]) -> Dict[str, Optional[str]]:
        if not url:
            return {}
        headers = {"User-Agent": os.getenv("SCRAPER_USER_AGENT", DEFAULT_UA)}
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        content = soup.select_one(".write_div, .thum-txtin, #content, .re_txt")
        text = _clean(content.get_text(" ", strip=True)) if content else None
        return {"content": text}
