"""
File for development of the RAWG fetcher - gets raw game details data from the RAWG API
"""

# TODO: Decide to fetch for all games or just a subset
# Since this endpoint does not support batch requests, need to think about requests
# usage.


import os
import datetime
import requests
import json
import time
import random
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


# region ------------ Get project path ------------
PROJECT_ROOT: Path = Path(__file__).parent.parent.parent.parent
DATA_LOCAL: Path = PROJECT_ROOT / "data_local" / "raw" / "rawg" / "game_details"
DATA_LOCAL.mkdir(parents=True, exist_ok=True)
# endregion


# region ------------ Get ID for all games ------------
all_games: list[dict] = query_all_data_rawg_games_cleaned()
df: pl.DataFrame = pl.DataFrame(data=all_games)
df = df.select("game_id")
# game_id = df.item(0, 0)
game_id = df["game_id"].to_list()
total_games: int = len(game_id)
games_fetched: int = 0
# endregion


# region ------------ API access config ------------
API_KEY: str = os.getenv("RAWG_GAME_DETAILS_API_KEY")

# game_id: list[str] = []
# BASE_URL_GAME_DETAILS: str = f"https://api.rawg.io/api/games/"
headers: dict = {"accept": "application/json"}
params: dict = {"key": API_KEY}

# endregion

if __name__ == "__main__":
    start_time = time.time()
    logger.info(
        "----- STARTED FETCHER ROUTINE AT %s -----",
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )

    for id in game_id:
        BASE_URL_GAME_DETAILS: str = f"https://api.rawg.io/api/games/{id}"

        print(f"Getting data from game id: {id}")

        try:
            response = requests.get(
                BASE_URL_GAME_DETAILS, headers=headers, params=params, timeout=10
            )
            response.raise_for_status()

            data: dict = response.json()

            logger.info("Successfully fetched data for game id: %s", id)

            filename: str = DATA_LOCAL / f"rawg_game_details_response_id_{id}.json"
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            games_fetched += 1
            logger.info("Total games fetched: %s", games_fetched)
            logger.info("Remaining games: %s", total_games - games_fetched)
            time.sleep(random.uniform(0.5, 1.5))

        except Exception as e:
            logger.error("Failed to fetch data for game id: %s", id)
            logger.error("*** FAILED TO FETCH DATA, RESPONSE: %s ***", e)
            logger.error("Response text (first 200 chars): %s", response.text[:200])

            time.sleep(5)
            continue

    end_time = time.time()
    elapsed_time = end_time - start_time

    logger.info("Process finished!")
    logger.info("Total games fetched: %s", games_fetched)
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
