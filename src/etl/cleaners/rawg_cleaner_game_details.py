"""
File for development of the RAWG cleaner (game details) - cleans raw game details data
"""

import datetime
import time
import os
import polars as pl

from pathlib import Path
from src.models.schema import GAME_DETAILS_SCHEMA
from src.utils.logger import setup_logger
from src.utils.supabase_client import supabase
from src.utils.supabase_tools import upload_file, update_table

# region ------------ Logger setup ------------
logger = setup_logger(__name__)
# endregion

# region ------------ Get root path ------------
PROJECT_ROOT: Path = Path(__file__).parent.parent.parent.parent
DATA_LOCAL_RAW: Path = PROJECT_ROOT / "data_local" / "raw" / "rawg" / "game_details"
DATA_LOCAL_TEMP: Path = PROJECT_ROOT / "data_local" / "temp" / "rawg" / "game_details"

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

    # region ------------ Override schema types ------------
    # For itens that have a struct, override the schema to resolve possible null values
    schema_override: dict[str, pl.PolarsDataType] = {
        "esrb_rating": pl.Struct({"id": pl.Int64}),
        "ratings": pl.List(pl.Struct({"id": pl.Int64})),
        "genres": pl.List(pl.Struct({"id": pl.Int64})),
        "tags": pl.List(pl.Struct({"id": pl.Int64})),
        "developers": pl.List(pl.Struct({"id": pl.Int64})),
        "publishers": pl.List(pl.Struct({"id": pl.Int64})),
        "platforms": pl.List(pl.Struct({"platform": pl.Struct({"id": pl.Int64})})),
    }
    # endregion

    df_raw = pl.read_ndjson(
        latest_file, infer_schema_length=None, schema_overrides=schema_override
    )

    # Turn off formatter for this block for readability
    # fmt: off
    df: pl.DataFrame = df_raw.select(
        pl.col("id")
            .alias("game_id"),
        pl.col("description_raw"),
        pl.col("tba"),
        pl.col("platforms")
            .fill_null([]).list.eval(pl.element().struct.field("platform")
            .struct.field("id"))
            .alias("platform_id"),
        pl.col("updated")
            .alias("updated_on_rawg"),
        pl.col("background_image"),
        pl.col("background_image_additional"),
        pl.col("reviews_count"),
        pl.col("reviews_text_count"),
        pl.col("ratings_count"),
        pl.col("rating").alias("rating_overall"),
        pl.col("ratings")
            .fill_null([]).list.eval(pl.element().struct.field("id"))
            .alias("rating_id"),
        pl.col("esrb_rating").struct.field("id")
            .alias("esrb_rating_id"),
        pl.col("genres")
            .fill_null([]).list.eval(pl.element().struct.field("id"))
            .alias("genre_id"),
        pl.col("tags")
            .fill_null([]).list.eval(pl.element().struct.field("id"))
            .alias("tag_id"),
        pl.col("achievements_count"),
        pl.col("reddit_logo"),
        pl.col("reddit_name"),
        pl.col("reddit_description"),
        pl.col("reddit_url"),
        pl.col("developers")
            .fill_null([]).list.eval(pl.element().struct.field("id"))
            .alias("developer_id"),
        pl.col("publishers")
            .fill_null([]).list.eval(pl.element().struct.field("id"))
            .alias("publisher_id"),
    ).with_columns(pl.lit(latest_file_timestamp).alias("updated_at"))
    # fmt: on

    df = df.cast(GAME_DETAILS_SCHEMA)

    logger.info(
        "Finished reading JSON and creating DataFrame at %s. Shape: %s",
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        df.shape,
    )
    # endregion

    # region ------------ Save dataframe as parquet ------------
    time_now: datetime.datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename: Path = DATA_LOCAL_TEMP / f"rawg_game_details_cleaned_{time_now}.parquet"
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
        folder="game_details",
    )
    update_table(table_name="rawg_game_details", data_to_update=df_cleaned)

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
