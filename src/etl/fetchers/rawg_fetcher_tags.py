"""
File for development of the RAWG tags fetcher - gets raw tags data from the RAWG API
asynchronously.

Saves continuously to a .jsonl (JSON Lines) file to prevent data loss and quota waste on
failures.
"""

import os
import datetime
import time
import json
import asyncio
import aiohttp
import math

import polars as pl

from pathlib import Path
from dotenv import load_dotenv
from src.utils.supabase_tools import (
    query_all_data_rawg_tags,
    init_connection,
    query_existing_tags_ids,
)
from src.utils.logger import setup_logger


# region ------------ Load env variables ------------
load_dotenv()
# endregion


# region ------------ Logger setup ------------
logger = setup_logger(__name__)
# endregion


# region ------------ Connect to supabase ------------
supabase = init_connection()
# endregion


# region ------------ API access config ------------
API_KEY: str = os.getenv("RAWG_GAME_DETAILS_API_KEY")
BASE_URL_TAGS: str = f"https://api.rawg.io/api/tags"
HEADERS: dict = {"accept": "application/json"}
PAGE_SIZE: int = 100
BASE_PARAMS: dict = {
    "key": API_KEY,
    "page_size": PAGE_SIZE,
}
# endregion


# region ------------ Get project path ------------
PROJECT_ROOT: Path = Path(__file__).parent.parent.parent.parent
DATA_LOCAL: Path = PROJECT_ROOT / "data_local" / "raw" / "rawg" / "tags"
DATA_LOCAL.mkdir(parents=True, exist_ok=True)
# endregion


async def fetch_tags_page(
    session: aiohttp.ClientSession,
    page: int,
    semaphore: asyncio.Semaphore,
    # tag_id: int,
    max_retries: int = 3,
) -> list[dict] | None:
    """
    Fetches pages with tags data from the RAWG API asynchronously.

    Args:
        session (aiohttp.ClientSession): The aiohttp session object.
        page (int): The page number to fetch.
        semaphore (asyncio.Semaphore): The semaphore object to limit concurrency.
        tag_id (int): The tag id to fetch data for.
        tag_id (int): The tag id to fetch.
        max_retries (int, optional): The maximum number of retries. Defaults to 3.
    """
    # url: str = f"https://api.rawg.io/api/tags/{tag_id}"
    params: dict = BASE_PARAMS.copy()
    params["page"] = page

    async with semaphore:
        for attempt in range(max_retries):
            try:
                async with session.get(
                    BASE_URL_TAGS, headers=HEADERS, params=params
                ) as response:
                    response.raise_for_status()
                    data: dict = await response.json()
                    logger.info("Successfully fetched tags page %s", page)
                    return data.get("results", [])

            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
                logger.warning(
                    "Attempt %s/%s failed for tags page %s: %s",
                    attempt + 1,
                    max_retries,
                    page,
                    e,
                )
                if attempt == max_retries - 1:
                    logger.error(
                        "----- MAX RETRIES REACHED - FINAL FAILURE FOR TAGS PAGE %s -----",
                        page,
                    )
                    return []
                sleep_time = 2 ** (attempt + 1)
                logger.info("Retrying tags page %s in %s seconds...", page, sleep_time)
                await asyncio.sleep(sleep_time)


async def main():
    """
    Main function to run the tags fetcher.
    """
    start_time = time.time()
    logger.info(
        "----- STARTED TAGS FETCHER ROUTINE AT %s -----",
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    timeout = aiohttp.ClientTimeout(total=30)
    semaphore = asyncio.Semaphore(5)

    all_results: list = []

    async with aiohttp.ClientSession(timeout=timeout) as session:
        logger.info("Fetching first tags page to determine total count...")

        try:
            params_p1 = BASE_PARAMS.copy()
            params_p1["page"] = 1
            async with session.get(
                BASE_URL_TAGS, headers=HEADERS, params=params_p1
            ) as response:
                response.raise_for_status()
                first_page_data: dict = await response.json()

                total_tags: int = first_page_data.get("count")
                first_page_results: list = first_page_data.get("results", [])
                all_results.extend(first_page_results)

                logger.info("Total tags for this query: %s", total_tags)

        except Exception as e:
            logger.error(
                "Failed to fetch inital tags page. Aborting process. Error: %s", e
            )
            return

        if total_tags > PAGE_SIZE:
            total_pages: int = math.ceil(total_tags / PAGE_SIZE)
            logger.info("Total pages to fetch: %s", total_pages)

            tasks: list = []
            for page in range(2, total_pages + 1):
                tasks.append(fetch_tags_page(session, page, semaphore))

            page_results = await asyncio.gather(*tasks)

            for result_list in page_results:
                all_results.extend(result_list)

        else:
            logger.info("Only one page of results found.")

    # region ------------ Save data to file ------------
    end_time = time.time()
    elapsed_time = end_time - start_time

    time_now: str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename: str = DATA_LOCAL / f"rawg_tags_response_{time_now}.json"

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)

    logger.info("Process finished!")
    logger.info(
        "Fetched %s tags!",
        len(all_results),
    )
    logger.info("Saved RAWG response to %s", filename)
    logger.info(
        "Elapsed time: seconds - %.2f  / minutes - %.2f / hours - %.2f",
        elapsed_time,
        elapsed_time / 60,
        elapsed_time / 3600,
    )
    logger.info(
        "----- ENDED TAGS FETCHER ROUTINE at %s -----",
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )


if __name__ == "__main__":
    asyncio.run(main())
