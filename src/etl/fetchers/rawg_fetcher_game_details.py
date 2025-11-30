"""
File for development of the RAWG fetcher - gets raw game details data from the RAWG API
"""

# TODO: Decide to fetch for all games or just a subset
# Since this endpoint does not support batch requests, need to think about requests
# usage.


import os

from dotenv import load_dotenv

# regio ------------ Load env variables ------------
load_dotenv()
# endregion

# region ------------ API access config ------------
API_KEY: str = os.getenv("RAWG_API_KEY")

game_id: list[str] = []
BASE_URL_GAME_DETAILS: str = f"https://api.rawg.io/api/games/{game_id}"
