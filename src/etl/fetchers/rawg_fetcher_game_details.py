"""
File for development of the RAWG fetcher - gets raw game details data from the RAWG API
"""

# TODO: Decide to fetch for all games or just a subset
# Since this endpoint does not support batch requests, need to think about requests
# usage.


import os
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
game_id = df.item(0, 0)
print(game_id)
print(type(game_id))
# endregion


# region ------------ API access config ------------
API_KEY: str = os.getenv("RAWG_API_KEY")

# game_id: list[str] = []
BASE_URL_GAME_DETAILS: str = f"https://api.rawg.io/api/games/{game_id}"
headers: dict = {"accept": "application/json"}

params: dict = {"key": API_KEY, "id": game_id}

if __name__ == "__main__":
    response: requests.Response = requests.get(
        BASE_URL_GAME_DETAILS,
        headers=headers,
        params=params,
    )

    print(BASE_URL_GAME_DETAILS)
    print(response)
