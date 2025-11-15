"""
File for development of the RAWG fetcher - gets raw data from the RAWG API
"""

import os
import requests


from dotenv import load_dotenv

# region ------------ Load environment variables ------------
load_dotenv()
api_key: str = os.getenv("RAWG_API_KEY")
# endregion


# region ------------ API access config ------------
main_url: str = f"https://api.rawg.io/api/games?key={api_key}"

headers: dict = {"accept": "application/json"}
# endregion


if __name__ == "__main__":
    response: requests.Response = requests.get(test_url, headers=headers)

    print(response.status_code)
    print(response.json())
