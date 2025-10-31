# scrape/main.py
import argparse
import logging
import datetime as dt
from pathlib import Path
import json
from dotenv import load_dotenv, find_dotenv
import os
import re
from typing import Iterable

# .env 로드 (.env 값이 OS 값보다 우선)
load_dotenv(find_dotenv(), override=True)

# 로깅
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(name)s: %(message)s",
    force=True,
)
logger = logging.getLogger(__name__)

# 확실한 스크레이퍼만 먼저 등록
from scrape.scrapers import dummy, naver_news

SCRAPER_MAP = {
    "dummy": dummy.DummyScraper,
    "naver-news": naver_news.NaverNewsScraper,
}

# dcinside는 있으면만 등록 (없어도 dummy/naver는 정상 동작)
try:
    import scrape.scrapers.dcinside as _dc
    if hasattr(_dc, "DcinsideScraper"):
        SCRAPER_MAP["dcinside"] = _dc.DcinsideScraper
    else:
        logger.warning("dcinside module found, but DcinsideScraper is missing; skipping.")
except Exception as e:
    logger.warning(f"dcinside not available: {e}")

def date_type(s: str) -> dt.date:
    try:
        return dt.date.fromisoformat(s)
    except ValueError:
        raise argparse.ArgumentTypeError("Should be YYYY-MM-DD format")

def append_jsonl(path: Path, records: Iterable[dict]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with path.open("a", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
            written += 1
    return written

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--keyword", required=True, help="Scrape websites by keyword")
    parser.add_argument("--start-date", type=date_type, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=date_type, required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--site", choices=list(SCRAPER_MAP.keys()), required=True, help="Target site")

    args = parser.parse_args()
    logger.info(f"Args: {args}")

    scraper_cls = SCRAPER_MAP[args.site]
    scraper = scraper_cls()

    # BaseScraper는 list 반환이 기본이지만, 제너레이터도 대응 가능
    data_iter = scraper.scrape(args.keyword, args.start_date, args.end_date)

    data_dir = os.getenv("DATA_DIR") or "data_raw"
    safe_keyword = re.sub(r'[\\/:*?"<>|]+', "_", args.keyword)
    out_path = Path(data_dir) / f"{args.site}_{safe_keyword}.jsonl"

    written = append_jsonl(out_path, data_iter)
    logger.info(f"Stored {written} records → {out_path}")

if __name__ == "__main__":
    logger.info("Start")
    main()
