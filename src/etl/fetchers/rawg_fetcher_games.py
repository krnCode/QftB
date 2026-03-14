"""
File for development of the RAWG fetcher - gets raw game data from the RAWG API
"""

import json
import os

# import requests
import datetime
import time
import asyncio
import aiohttp
import math

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
BASE_URL_GAMES: str = f"https://api.rawg.io/api/games"
HEADERS: dict = {"accept": "application/json"}
PAGE_SIZE: int = 40

today_date: str = datetime.datetime.now().strftime("%Y-%m-%d")
# all_results: list[dict] = []
# page = 1
base_params: dict = {
    "key": API_KEY,
    "page_size": PAGE_SIZE,
    "dates": f"2025-09-01,{today_date}",  # start date in 09/2025 to not expend available requests
}
# endregion


# region ------------ Get project path ------------
PROJECT_ROOT: Path = Path(__file__).parent.parent.parent.parent
DATA_LOCAL: Path = PROJECT_ROOT / "data_local" / "raw" / "rawg" / "games"
DATA_LOCAL.mkdir(parents=True, exist_ok=True)
# endregion


async def fetch_page(
    session: aiohttp.ClientSession,
    page: int,
    semaphore: asyncio.Semaphore,
    max_retries: int = 3,
) -> list[dict]:
    """
    Fetches specific page with concurrency limit, retries, and exponential backoff.
    """
    params: dict = base_params.copy()
    params["page"] = page

    async with semaphore:
        for attempt in range(max_retries):
            try:
                async with session.get(
                    BASE_URL_GAMES, headers=HEADERS, params=params
                ) as response:
                    response.raise_for_status()
                    data: dict = await response.json()
                    logger.info("Successfully fetched page %s", page)
                    return data.get("results", [])

            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
                logger.warning(
                    "Attempt %s/%s failes for page %s: %s",
                    attempt + 1,
                    max_retries,
                    page,
                    e,
                )
                if attempt == max_retries - 1:
                    logger.error(
                        "----- MAX RETRIES REACHED - FINAL FAILURE FOR PAGE %s -----",
                        page,
                    )
                    return []
                sleep_time = 2 ** (attempt + 1)
                logger.info("Retrying page %s in %s seconds...", page, sleep_time)
                await asyncio.sleep(sleep_time)


async def main():
    start_time = time.time()
    logger.info(
        "----- STARTED FETCHER ROUTINE AT %s -----",
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    timeout = aiohttp.ClientTimeout(total=30)
    semaphore = asyncio.Semaphore(5)

    all_results: list = []

    async with aiohttp.ClientSession(timeout=timeout) as session:
        logger.info("Fetching first page to determine total count...")

        try:
            params_p1 = base_params.copy()
            params_p1["page"] = 1
            async with session.get(
                BASE_URL_GAMES, headers=HEADERS, params=params_p1
            ) as response:
                response.raise_for_status()
                first_page_data: dict = await response.json()

                total_games: int = first_page_data.get("count")
                first_page_results: list = first_page_data.get("results", [])
                all_results.extend(first_page_results)

                logger.info("Total games for this query: %s", total_games)

        except Exception as e:
            logger.error("Failed to fetch inital page. Aborting process. Error: %s", e)
            return

        if total_games > PAGE_SIZE:
            total_pages: int = math.ceil(total_games / PAGE_SIZE)
            logger.info("Total pages to fetch: %s", total_pages)

            tasks: list = []
            for page in range(2, total_pages + 1):
                tasks.append(fetch_page(session, page, semaphore))

            page_results = await asyncio.gather(*tasks)

            for result_list in page_results:
                all_results.extend(result_list)

        else:
            logger.info("Only one page of results found.")

    # region ------------ Save data to file ------------
    end_time = time.time()
    elapsed_time = end_time - start_time

    time_now: str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename: str = DATA_LOCAL / f"rawg_games_response_{time_now}.json"

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)

    logger.info("Process finished!")
    logger.info("Fetched %s games between %s!", len(all_results), base_params["dates"])
    logger.info("Saved RAWG response to %s", filename)
    logger.info(
        "Elapsed time: seconds - %.2f  / minutes - %.2f / hours - %.2f",
        elapsed_time,
        elapsed_time / 60,
        elapsed_time / 3600,
    )
    logger.info(
        "----- ENDED FETCHER ROUTINE at %s -----",
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )


if __name__ == "__main__":
    asyncio.run(main())
