"""
File for development of the RAWG fetcher - gets raw data from the RAWG API
"""

import json
import os
import requests
import datetime
import time

from dotenv import load_dotenv
from pathlib import Path
from src.utils.logger import setup_logger

# region ------------ Load env variables ------------
load_dotenv()
# endregion

# region ------------ Logger setup ------------
logger = setup_logger(__name__)
# endregion


# region ------------ API access config ------------
API_KEY: str = os.getenv("RAWG_API_KEY")
# BASE_URL_GAMES: str = f"https://api.rawg.io/api/games?key={API_KEY}&page_size=40"
BASE_URL_GAMES: str = f"https://api.rawg.io/api/games"
headers: dict = {"accept": "application/json"}

today_date: str = datetime.datetime.now().strftime("%Y-%m-%d")
all_results: list[dict] = []
page = 1
params: dict = {
    "key": API_KEY,
    "page_size": 40,
    "dates": f"2025-09-01,{today_date}",
    "page": page,
}
# endregion


# region ------------ Get project path ------------
PROJECT_ROOT: Path = Path(__file__).parent.parent.parent.parent
DATA_LOCAL: Path = PROJECT_ROOT / "data_local" / "raw" / "rawg" / "games"
DATA_LOCAL.mkdir(parents=True, exist_ok=True)
# endregion


if __name__ == "__main__":
    start_time = time.time()
    logger.info(
        "STARTED FETCHER ROUTINE at %s",
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )

    while True:
        response: requests.Response = requests.get(
            BASE_URL_GAMES,
            headers=headers,
            params=params,
        )

        logger.info("Getting data from page: %s", page)

        try:
            data: dict = response.json()
            logger.info("Successfully fetched %s games", len(data.get("results", [])))
        except ValueError as e:
            data: dict = {}
            logger.error("Failed to fetch data, response: %s", e)
            logger.error("Response text (first 200 chars): %s", response.text[:200])

        count: int | None = data.get("count")
        if count is not None:
            logger.info("Total games for this query: %s", count)
        else:
            logger.warning("Count not found in response, skipping...")

        results: list[dict] = data.get("results", [])
        all_results.extend(results)

        if not data.get("next"):
            break
        page += 1
        params["page"] = page

        logger.info("Page finished. Querying next page...")
        if count is not None:
            logger.info("Remaining games: %s", data["count"] - len(all_results))

        time.sleep(0.5)

    end_time = time.time()
    elapsed_time = end_time - start_time

    time_now: datetime.datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename: str = DATA_LOCAL / f"rawg_games_response_{time_now}.json"

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)

    logger.info("Process finished!")
    logger.info(
        "Elapsed time: seconds - %.2f  / minutes - %.2f / hours - %.2f",
        elapsed_time,
        elapsed_time / 60,
        elapsed_time / 3600,
    )
    logger.info("Number of pages: %s", page)
    logger.info("Collected %s games between %s!", len(all_results), params["dates"])
    logger.info("Saved RAWG response to %s", filename)
    logger.info(
        "ENDED FETCHER ROUTINE at %s",
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
