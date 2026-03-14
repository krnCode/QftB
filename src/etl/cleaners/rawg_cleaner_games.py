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


# region ------------ Logger setup ------------
logger = setup_logger(__name__)
# endregion

# region ------------ Get root path ------------
PROJECT_ROOT: Path = Path(__file__).parent.parent.parent.parent
DATA_LOCAL_RAW: Path = PROJECT_ROOT / "data_local" / "raw" / "rawg" / "games"
DATA_LOCAL_TEMP: Path = PROJECT_ROOT / "data_local" / "temp" / "rawg" / "games"

DATA_LOCAL_RAW.mkdir(parents=True, exist_ok=True)
DATA_LOCAL_TEMP.mkdir(parents=True, exist_ok=True)
# endregion


def main():
    start_time = time.time()
    logger.info(
        "----- STARTED CLEANER ROUTINE AT %s -----",
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )

    # region ------------ Get recent data ------------
    # Get all json files in the folder and sort them by most recent
    files: list[Path] = [
        os.path.join(DATA_LOCAL_RAW, f)
        for f in os.listdir(DATA_LOCAL_RAW)
        if f.endswith(".json")
    ]

    if not files:
        logger.error("No JSON files found in %s. Aborting cleaner.", DATA_LOCAL_RAW)
        return

    files.sort(key=os.path.getmtime, reverse=True)
    latest_file: Path = files[0]
    logger.info("Latest file found: %s", latest_file)
    # endregion

    # region ------------ Extract data and create DataFrame ------------
    logger.info("Reading JSON and creating DataFrame with Polars...")

    latest_file_timestamp: datetime.datetime = datetime.datetime.fromtimestamp(
        os.path.getmtime(latest_file)
    )

    df_raw = pl.read_json(latest_file)
    df: pl.DataFrame = df_raw.select(
        pl.col("id").alias("game_id"),
        pl.col("slug"),
        pl.col("name"),
        pl.col("released"),
        pl.col("rating"),
        pl.col("ratings_count"),
        pl.col("platforms")
        .fill_null([])
        .list.eval(pl.element().struct.field("platform").struct.field("name"))
        .alias("platforms"),
        pl.col("genres")
        .fill_null([])
        .list.eval(pl.element().struct.field("name"))
        .alias("genres"),
    ).with_columns(pl.lit(latest_file_timestamp).alias("updated_at"))

    df = df.cast(GAME_SCHEMA)

    logger.info(
        "Finished reading JSON and creating DataFrame at %s. Shape: %s",
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        df.shape,
    )

    # endregion

    # region ------------ Save dataframe as parquet ------------
    time_now: datetime.datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename: Path = DATA_LOCAL_TEMP / f"rawg_games_cleaned_{time_now}.parquet"

    df.write_parquet(filename)
    logger.info("Created parquet file %s at %s", filename, time_now)
    # endregion

    # region ------------ Upload to Supabase ------------
    logger.info("Starting upload to Supabase...")
    df_cleaned: pl.DataFrame = pl.read_parquet(filename)

    upload_file(local_path=str(filename), filename=filename.name, bucket="rawg-data")
    update_table(table_name="rawg_games_cleaned", data_to_update=df_cleaned)

    end_time = time.time()
    elapsed_time = end_time - start_time

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


# endregion

if __name__ == "__main__":
    main()
