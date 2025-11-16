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
DATA_LOCAL_RAW: Path = PROJECT_ROOT / "data_local" / "raw"
DATA_LOCAL_TEMP: Path = PROJECT_ROOT / "data_local" / "temp"

# Folder to read json files to clean
folder_raw: Path = DATA_LOCAL_RAW

# Folder to save the cleaned parquet file
folder_temp: Path = DATA_LOCAL_TEMP
# endregion


# region ------------ Get recent data ------------
# Get all json files in the folder and sort them by most recent
files: list[Path] = [
    os.path.join(folder_raw, f) for f in os.listdir(folder_raw) if f.endswith(".json")
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

# region ------------ Save dataframe as parquet ------------
filename: str = (
    folder_temp
    / f"rawg_cleaned_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.parquet"
)

df.write_parquet(filename)
# endregion

if __name__ == "__main__":
    print(f"Latest file: {latest_file}")
    print(f"Date of the json file: {latest_file_timestamp}")

    df_cleaned: pl.DataFrame = pl.read_parquet(filename)
    print(df_cleaned)
