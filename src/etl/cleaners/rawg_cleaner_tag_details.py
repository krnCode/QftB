"""
File for development of the RAWG cleaner (tag details) - cleans raw tag details data
"""

import datetime
import time
import os
import polars as pl

from pathlib import Path
from src.models.schema import TAG_DETAILS_SCHEMA
from src.utils.logger import setup_logger
from src.utils.supabase_client import supabase
from src.utils.supabase_tools import upload_file, update_table

# region ------------ Logger setup ------------
logger = setup_logger(__name__)
# endregion

# region ------------ Get root path ------------
PROJECT_ROOT: Path = Path(__file__).parent.parent.parent.parent
DATA_LOCAL_RAW: Path = PROJECT_ROOT / "data_local" / "raw" / "rawg" / "tag_details"
DATA_LOCAL_TEMP: Path = PROJECT_ROOT / "data_local" / "temp" / "rawg" / "tag_details"

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
        if f.endswith(".jsonl")
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

    df_raw = pl.read_ndjson(
        latest_file,
        infer_schema_length=None,
    )

    # Turn off formatter for this block for readability
    # fmt: off
    df: pl.DataFrame = df_raw.select(
        pl.col("id")
            .alias("tag_id"),
        pl.col("name"),
        pl.col("slug"),
        pl.col("games_count"),
        pl.col("image_background"),
        pl.col("description"),
    ).with_columns(pl.lit(latest_file_timestamp).alias("updated_at"))
    # fmt: on

    df = df.cast(TAG_DETAILS_SCHEMA)

    logger.info(
        "Finished reading JSON and creating DataFrame at %s. Shape: %s",
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        df.shape,
    )
    # endregion

    # region ------------ Save dataframe as parquet ------------
    time_now: datetime.datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename: Path = DATA_LOCAL_TEMP / f"rawg_tag_details_cleaned_{time_now}.parquet"
    df.write_parquet(filename)
    logger.info("Created parquet file %s at %s", filename, time_now)
    # endregion

    # region ------------ Upload to Supabase ------------
    logger.info("Starting upload to Supabase...")
    df_cleaned: pl.DataFrame = pl.read_parquet(filename)

    upload_file(
        local_path=str(filename),
        filename=filename,
        bucket="rawg-data",
        folder="tag_details",
    )
    update_table(
        table_name="rawg_tag_details",
        data_to_update=df_cleaned,
        on_conflict="tag_id",
    )

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


if __name__ == "__main__":
    main()
