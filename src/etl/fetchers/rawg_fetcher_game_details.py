"""
File for development of the RAWG game details fetcher - gets raw game details data from
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
from src.utils.supabase_tools import query_all_data_rawg_games_cleaned, init_connection
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
DATA_LOCAL: Path = PROJECT_ROOT / "data_local" / "raw" / "rawg" / "game_details"
DATA_LOCAL.mkdir(parents=True, exist_ok=True)
# endregion


async def fetch_game_details(
    session: aiohttp.ClientSession,
    game_id: int,
    semaphore: asyncio.Semaphore,
    max_retries: int = 3,
) -> dict | None:
    """
    Fetches game details data from the RAWG API asynchronously.

    Args:
        session (aiohttp.ClientSession): The aiohttp session object.
        game_id (int): The game ID to fetch data for.
        semaphore (asyncio.Semaphore): The semaphore object to limit concurrency.
        max_retries (int, optional): The maximum number of retries. Defaults to 3.
    """
    url: str = f"https://api.rawg.io/api/games/{game_id}"

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
                    logger.error("----- FINAL FAILURE FOR GAME %s -----", game_id)
                    return None

                sleep_time = 2 ** (attempt + 1)
                await asyncio.sleep(sleep_time)
                logger.error(
                    "Attempt %s failed for game %s, retrying in %s seconds...",
                    attempt + 1,
                    game_id,
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
    logger.info("Querying Supabase for game IDs...")
    all_games: list[dict] = query_all_data_rawg_games_cleaned()

    if not all_games:
        logger.warning("No games found in Supabase, exiting...")
        return

    df: pl.DataFrame = pl.DataFrame(data=all_games)
    game_ids: list[int] = df["game_id"].to_list()

    # region ------------ Limit requests for testing ------------
    # SECURITY LOCK: Limit the number of requests for testing
    # Remove or adjust when running in production
    LIMIT_REQUESTS: int = 50  # TEST: Use this in testing
    # LIMIT_REQUESTS: int = len(game_ids)  # PROD: Use this in production
    game_ids = game_ids[:LIMIT_REQUESTS]
    # endregion

    total_games: int = len(game_ids)
    logger.info("Total games to fetch in this run: %s", total_games)
    # endregion

    # region ------------ Async fetching and continuous saving ------------
    timeout = aiohttp.ClientTimeout(total=15)
    semaphore = asyncio.Semaphore(10)

    time_now: str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename: str = DATA_LOCAL / f"rawg_game_details_response_{time_now}.jsonl"

    games_fetched: int = 0

    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks: list[asyncio.Task] = [
            fetch_game_details(session, game_id, semaphore) for game_id in game_ids
        ]
        logger.info("Fetching data and saving continuously to %s...", filename)

        with open(filename, "a", encoding="utf-8") as f:
            for coro in asyncio.as_completed(tasks):
                result = await coro

                if result is not None:
                    f.write(json.dumps(result, ensure_ascii=False) + "\n")
                    f.flush()

                    games_fetched += 1

                    if games_fetched % 10 == 0 or games_fetched == total_games:
                        logger.info(
                            "Progress: %s/%s fames fetched and saved...",
                            games_fetched,
                            total_games,
                        )

    end_time = time.time()
    elapsed_time = end_time - start_time

    logger.info("Process finished!")
    logger.info("Total games successfully fetched: %s", games_fetched)
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
