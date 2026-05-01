"""
File for development of the RAWG tag details fetcher - gets raw tag details data from
the RAWG API asynchronously.

Saves continuously to a .jsonl (JSON Lines) file to prevent data loss and quota waste on
failures.
"""

import os
import datetime
import time
import json
import asyncio
import aiohttp
import polars as pl

from pathlib import Path
from dotenv import load_dotenv
from src.utils.supabase_tools import (
    query_all_data_rawg_tags,
    init_connection,
    query_existing_tag_details_ids,
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
HEADERS: dict = {"accept": "application/json"}
BASE_PARAMS: dict = {"key": API_KEY}
# endregion


# region ------------ Get project path ------------
PROJECT_ROOT: Path = Path(__file__).parent.parent.parent.parent
DATA_LOCAL: Path = PROJECT_ROOT / "data_local" / "raw" / "rawg" / "tag_details"
DATA_LOCAL.mkdir(parents=True, exist_ok=True)
# endregion


async def fetch_game_details(
    session: aiohttp.ClientSession,
    tag_id: int,
    semaphore: asyncio.Semaphore,
    max_retries: int = 3,
) -> dict | None:
    """
    Fetches tag details data from the RAWG API asynchronously.

    Args:
        session (aiohttp.ClientSession): The aiohttp session object.
        game_id (int): The tag ID to fetch data for.
        semaphore (asyncio.Semaphore): The semaphore object to limit concurrency.
        max_retries (int, optional): The maximum number of retries. Defaults to 3.
    """
    url: str = f"https://api.rawg.io/api/tags/{tag_id}"

    async with semaphore:
        for attempt in range(max_retries):
            try:
                async with session.get(
                    url, headers=HEADERS, params=BASE_PARAMS
                ) as response:
                    response.raise_for_status()
                    data: dict = await response.json()
                    return data

            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
                if attempt == max_retries - 1:
                    logger.error("----- FINAL FAILURE FOR GAME %s -----", tag_id)
                    return None

                sleep_time = 2 ** (attempt + 1)
                await asyncio.sleep(sleep_time)
                logger.error(
                    "Attempt %s failed for game %s, retrying in %s seconds...",
                    attempt + 1,
                    tag_id,
                    sleep_time,
                )


async def main():
    """
    Main function to run the fetcher.
    """
    start_time = time.time()
    logger.info(
        "----- STARTED FETCHER ROUTINE AT %s -----",
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )

    # region ------------ Get ID for all games ------------
    logger.info("Querying Supabase for tag IDs...")
    all_tags: list[dict] = query_all_data_rawg_tags()

    if not all_tags:
        logger.warning("No tags found in Supabase, exiting...")
        return

    # region ------------ Create a Delta Load for fetching tag details ------------
    logger.info("Querying Supabase for existing tag IDs with tag details...")
    existing_tag_ids: list[int] = query_existing_tag_details_ids()

    df_all: pl.DataFrame = pl.DataFrame(data=all_tags, infer_schema_length=None)
    all_tag_ids: list[int] = [int(x) for x in df_all["tag_id"].to_list()]

    existing_set = set([int(x) for x in existing_tag_ids])
    logger.info("Total tags in rawg_tags: %s", len(all_tag_ids))
    logger.info(
        "Total tags with tag details in rawg_tag_details: %s", len(existing_set)
    )

    tag_ids: list[int] = [
        tag_id for tag_id in all_tag_ids if tag_id not in existing_set
    ]
    # endregion

    # region ------------ Limit requests for testing ------------
    # SECURITY LOCK: Limit the number of requests for testing
    # Remove or adjust when running in production
    # LIMIT_REQUESTS: int = 5  # TEST: Use this in testing
    LIMIT_REQUESTS: int = len(tag_ids)  # PROD: Use this in production
    tag_ids = tag_ids[:LIMIT_REQUESTS]
    # endregion

    total_tags: int = len(tag_ids)
    logger.info("Total tags to fetch in this run: %s", total_tags)
    # endregion

    if total_tags == 0:
        logger.info(
            "----- ALL TAGS ARE UP TO DATE! No new details to fecth. Exiting... -----"
        )

        return

    # region ------------ Async fetching and continuous saving ------------
    timeout = aiohttp.ClientTimeout(total=15)
    semaphore = asyncio.Semaphore(10)

    time_now: str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename: Path = DATA_LOCAL / f"rawg_tag_details_response_{time_now}.jsonl"

    tags_fetched: int = 0

    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks: list[asyncio.Task] = [
            fetch_game_details(session, tag_id, semaphore) for tag_id in tag_ids
        ]
        logger.info("Fetching data and saving continuously to %s...", filename)

        with open(filename, "a", encoding="utf-8") as f:
            for coro in asyncio.as_completed(tasks):
                result = await coro

                if result is not None:
                    f.write(json.dumps(result, ensure_ascii=False) + "\n")
                    f.flush()

                    tags_fetched += 1

                    if tags_fetched % 10 == 0 or tags_fetched == total_tags:
                        logger.info(
                            "Progress: tags fetched and saved: %s/%s",
                            tags_fetched,
                            total_tags,
                        )

    end_time = time.time()
    elapsed_time = end_time - start_time

    logger.info("Process finished!")
    logger.info("Total tags successfully fetched: %s", tags_fetched)
    logger.info("Saved consolidated JSON Lines response to %s", filename)
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
    # endregion


if __name__ == "__main__":
    asyncio.run(main())
