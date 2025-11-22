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

# region ------------ Load env variables ------------
load_dotenv()
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
print(PROJECT_ROOT)
# endregion


if __name__ == "__main__":
    start_time = time.time()
    while True:
        response: requests.Response = requests.get(
            BASE_URL_GAMES,
            headers=headers,
            params=params,
        )

        print(f"Getting data from page: {page}")

        try:
            data: json = response.json()
        except ValueError as e:
            print(f"Response was not valid JSON, now returning empty dict... {e}")
            print(response.text[:200])
            data: dict = {}

        count: int = data.get("count")
        if count is not None:
            print("Total games for this query: ", count)
        else:
            print("Count not found in response, skipping...")

        results: list[dict] = data.get("results", [])
        all_results.extend(results)

        if not data.get("next"):
            break
        page += 1
        params["page"] = page

        print("Page finished. Querying next page...")
        print("Remaining games: ", data["count"] - len(all_results))

        time.sleep(0.5)

    end_time = time.time()
    elapsed_time = end_time - start_time

    time_now: datetime.datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename: str = DATA_LOCAL / f"rawg_games_response_{time_now}.json"

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)

    print("Process finished!")
    print(f"Elapsed time: {elapsed_time} seconds")
    print(f"Elapsed time: {elapsed_time / 60} minutes")
    print(f"Elapsed time: {elapsed_time / 3600} hours")
    print(f"Number of pages: {page}")
    print(f"Collected {len(all_results)} games between {params['dates']}!")
    print(f"Saved RAWG response to {filename}")
