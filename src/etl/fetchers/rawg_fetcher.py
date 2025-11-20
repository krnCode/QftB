"""
File for development of the RAWG fetcher - gets raw data from the RAWG API
"""

import json
import os
import requests
import datetime

from dotenv import load_dotenv
from pathlib import Path

# region ------------ Load environment variables ------------
load_dotenv()
api_key: str = os.getenv("RAWG_API_KEY")
# endregion


# region ------------ API access config ------------
main_url: str = f"https://api.rawg.io/api/games?key={api_key}&page_size=40"

headers: dict = {"accept": "application/json"}
# endregion


# region ------------ Get project path ------------
PROJECT_ROOT: Path = Path(__file__).parent.parent.parent.parent
DATA_LOCAL: Path = PROJECT_ROOT / "data_local" / "raw" / "rawg"
DATA_LOCAL.mkdir(parents=True, exist_ok=True)
# endregion


if __name__ == "__main__":
    response: requests.Response = requests.get(main_url, headers=headers)
    data: json = response.json()

    filename: str = (
        DATA_LOCAL
        / f"rawg_response_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
    )

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Saved RAWG response to {filename}")
    print(response.status_code)
