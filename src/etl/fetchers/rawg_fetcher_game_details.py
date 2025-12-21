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
import polars as pl

from pathlib import Path
from dotenv import load_dotenv
from src.utils.supabase_tools import query_all_data_rawg_games_cleaned, init_connection


# region ------------ Load env variables ------------
load_dotenv()
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
game_id = df["game_id"].to_list()[:5]

# endregion


# region ------------ API access config ------------
API_KEY: str = os.getenv("RAWG_API_KEY")

# game_id: list[str] = []
# BASE_URL_GAME_DETAILS: str = f"https://api.rawg.io/api/games/"
headers: dict = {"accept": "application/json"}
params: dict = {"key": API_KEY}

all_results: list[dict] = []
# endregion


if __name__ == "__main__":
    for id in game_id:
        BASE_URL_GAME_DETAILS: str = f"https://api.rawg.io/api/games/{id}"
        response: requests.Response = requests.get(
            BASE_URL_GAME_DETAILS,
            headers=headers,
            params=params,
        )

        print(f"Getting data from game id: {id}")

        try:
            data: dict = response.json()
            print(f"Successfully fetched data for game id: {id}")
            print(f"{response}")
        except ValueError as e:
            data: dict = {}
            print(f"*** FAILED TO FETCH DATA, RESPONSE: {response} ***")
            print(f"Response text (first 200 chars): {response.text[:200]}")

        all_results.append(data)

    time_now: datetime.datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename: str = DATA_LOCAL / f"rawg_game_details_response_{time_now}.json"

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)
