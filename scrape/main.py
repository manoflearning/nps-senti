import argparse
from scrape.scrapers import dummy, naver_news
import logging
import datetime as dt
from pathlib import Path
import json
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(name)s: %(message)s",
    force=True,
)
logger = logging.getLogger(__name__)

SCRAPER_MAP = {
    "dummy": dummy.DummyScraper,
    "naver-news": naver_news.NaverNewsScraper,
}


def date_type(s: str) -> dt.date:
    try:
        return dt.date.fromisoformat(s)
    except ValueError:
        raise argparse.ArgumentTypeError("Should be YYYY-MM-DD format")


def append_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument("--keyword", required=True, help="Scrape websites by keyword")
    parser.add_argument(
        "--start-date", type=date_type, required=True, help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date", type=date_type, required=True, help="End date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--site", choices=list(SCRAPER_MAP.keys()), required=True, help="Target site"
    )

    args = parser.parse_args()
    logger.info(f"Args: {args}")

    scraper = SCRAPER_MAP[args.site]()

    # TODO: implement worker class, avoid using Scraper directly
    data = scraper.scrape(args.keyword, args.start_date, args.end_date)

    logger.info(f"Data: {data}")

    data_dir = os.getenv("DATA_DIR")

    if isinstance(data_dir, str):
        append_jsonl(Path(f"{data_dir}/{args.site}_{args.keyword}.jsonl"), data)
        logger.info(f"Data stored at {data_dir}/{args.site}_{args.keyword}.jsonl")
    else:
        logger.warning("Set DATA_DIR in .env file")


if __name__ == "__main__":
    logger.info("Start")

    main()
