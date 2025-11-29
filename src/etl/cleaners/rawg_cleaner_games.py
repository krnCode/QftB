"""
File for development of the RAWG cleaner - cleans raw json data
"""

import datetime
import time
import json
import os
import polars as pl

from src.utils.supabase_client import supabase
from src.utils.supabase_tools import upload_file, update_table
from pathlib import Path
from src.models.schema import GAME_SCHEMA
from src.utils.logger import setup_logger


# region ------------ Get root path ------------
PROJECT_ROOT: Path = Path(__file__).parent.parent.parent.parent
DATA_LOCAL_RAW: Path = PROJECT_ROOT / "data_local" / "raw" / "rawg" / "games"
DATA_LOCAL_TEMP: Path = PROJECT_ROOT / "data_local" / "temp" / "rawg" / "games"

# Folder to read json files to clean
folder_raw: Path = DATA_LOCAL_RAW

# Folder to save the cleaned parquet file
folder_temp: Path = DATA_LOCAL_TEMP
# endregion


# region ------------ Logger setup ------------
logger = setup_logger(__name__)
# endregion

# region ------------ Get recent data ------------
start_time = time.time()
logger.info(
    "----- STARTED CLEANER ROUTINE AT %s -----",
    datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
)

# Get all json files in the folder and sort them by most recent
files: list[Path] = [
    os.path.join(folder_raw, f) for f in os.listdir(folder_raw) if f.endswith(".json")
]

files.sort(key=os.path.getmtime, reverse=True)
latest_file: Path = files[0]
logger.info("Latest file found: %s", latest_file)
# endregion


# region ------------ Extract data from json ------------
data: json = json.load(open(latest_file, "r", encoding="utf-8"))
# data: dict = data["results"]

game_ids = [item["id"] for item in data]
slug = [item["slug"] for item in data]
name = [item["name"] for item in data]
released = [item["released"] for item in data]
rating = [item["rating"] for item in data]
ratings_count = [item["ratings_count"] for item in data]
platforms = []
genres = []

# Nested itens in the json file
for item in data:
    get_platforms_found = item.get("platforms") or []
    platforms.append([p["platform"]["name"] for p in get_platforms_found])

    get_genres_found = item.get("genres") or []
    genres.append([g["name"] for g in get_genres_found])

logger.info(
    "Finished getting data from json file at %s",
    datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
)
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
    schema=GAME_SCHEMA,
)
# endregion

# region ------------ Save dataframe as parquet ------------
time_now: datetime.datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
filename: str = folder_temp / f"rawg_games_cleaned_{time_now}.parquet"

df.write_parquet(filename)
# endregion

if __name__ == "__main__":
    logger.info(
        "Finished creating dataframe at %s",
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )

    df_cleaned: pl.DataFrame = pl.read_parquet(filename)

    upload_file(local_path=filename, filename=filename, bucket="rawg-data")
    update_table(table_name="rawg_games_cleaned", data_to_update=df_cleaned)

    end_time = time.time()
    elapsed_time = end_time - start_time
    logger.info(
        "Created parquet file %s at %s",
        filename,
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    logger.info("DF size: %s", df_cleaned.shape)
    logger.info(
        "Elapsed time: seconds - %.2f  / minutes - %.2f / hours - %.2f",
        elapsed_time,
        elapsed_time / 60,
        elapsed_time / 3600,
    )
    logger.info(
        "----- ENDED CLEANER ROUTINE AT %s -----",
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
