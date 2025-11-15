"""
File for development of the RAWG cleaner - cleans raw json data
"""

import datetime
import json
import os
import polars as pl

from pathlib import Path
from src.models.schema import SCHEMA


# region ------------ Get root path ------------
PROJECT_ROOT: Path = Path(__file__).parent.parent.parent.parent
DATA_LOCAL: Path = PROJECT_ROOT / "data_local" / "raw"
# endregion


# region ------------ Get recent data ------------
folder: Path = DATA_LOCAL

# Get all json files in the folder and sort them by most recent
files: list[Path] = [
    os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".json")
]

files.sort(key=os.path.getmtime, reverse=True)
latest_file: Path = files[0]
# endregion


# region ------------ Extract data from json ------------
data: json = json.load(open(latest_file, "r", encoding="utf-8"))
data: dict = data["results"]

game_ids = [item["id"] for item in data]
slug = [item["slug"] for item in data]
name = [item["name"] for item in data]
released = [item["released"] for item in data]
rating = [item["rating"] for item in data]
ratings_count = [item["ratings_count"] for item in data]

# Nested itens in the json file
platforms = [[p["platform"]["name"] for p in item["platforms"]] for item in data]
genres = [[g["name"] for g in item["genres"]] for item in data]
# endregion


# region ------------ Get date of the json file ------------
latest_file_timestamp: datetime.datetime = datetime.datetime.fromtimestamp(
    os.path.getmtime(latest_file)
)
# endregion


# region ------------ Create dataframe ------------
df: pl.DataFrame = pl.DataFrame(
    data={
        "game_id": game_ids,
        "slug": slug,
        "name": name,
        "released": released,
        "rating": rating,
        "ratings_count": ratings_count,
        "platforms": platforms,
        "genres": genres,
        "updated_at": latest_file_timestamp,
    },
    schema=SCHEMA,
)
# endregion

if __name__ == "__main__":
    print(f"Latest file: {latest_file}")
    print(f"Date of the json file: {latest_file_timestamp}")

    print(df.shape)
    print(df.columns)
    print(df.dtypes)
    print(df.head())
    print(df.tail())
