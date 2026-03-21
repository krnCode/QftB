"""
File for development of the RAWG cleaner - cleans raw json data
"""

import datetime
import time
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

    # region ------------ Override schema types ------------
    # For itens that have a struct, override the schema to resolve possible null values
    schema_override = {
        "ratings": pl.List(
            pl.Struct(
                {
                    "id": pl.Int64,
                    "title": pl.Utf8,
                    "count": pl.Int64,
                    "percent": pl.Float64,
                }
            )
        ),
        "added_by_status": pl.Struct(
            {
                "yet": pl.Int64,
                "owned": pl.Int64,
                "beaten": pl.Int64,
                "toplay": pl.Int64,
                "dropped": pl.Int64,
                "playing": pl.Int64,
            }
        ),
        # Adicionado name e slug para o Polars não reclamar de "extra fields"
        "esrb_rating": pl.Struct(
            {
                "id": pl.Int64,
                "name": pl.Utf8,
                "slug": pl.Utf8,
                "name_en": pl.Utf8,
                "name_ru": pl.Utf8,
            }
        ),
        "platforms": pl.List(
            pl.Struct(
                {
                    "platform": pl.Struct(
                        {"id": pl.Int64, "name": pl.Utf8, "slug": pl.Utf8}
                    ),
                    "released_at": pl.Utf8,  # Pode vir em alguns jogos
                    "requirements_en": pl.Struct(
                        {"minimum": pl.Utf8, "recommended": pl.Utf8}
                    ),
                    "requirements_ru": pl.Struct(
                        {"minimum": pl.Utf8, "recommended": pl.Utf8}
                    ),
                    "requirements": pl.Struct(
                        {"minimum": pl.Utf8, "recommended": pl.Utf8}
                    ),
                }
            )
        ),
        "genres": pl.List(
            pl.Struct({"id": pl.Int64, "name": pl.Utf8, "slug": pl.Utf8})
        ),
        "stores": pl.List(
            pl.Struct(
                {
                    "id": pl.Int64,
                    "url": pl.Utf8,
                    "store": pl.Struct(
                        {"id": pl.Int64, "name": pl.Utf8, "slug": pl.Utf8}
                    ),
                }
            )
        ),
    }
    # endregion

    df_raw = pl.read_json(
        latest_file, infer_schema_length=None, schema_overrides=schema_override
    )

    # Turn off formatter for this block for readability
    # fmt: off
    df: pl.DataFrame = df_raw.select(
        pl.col("id").alias("game_id"),
        pl.col("slug"),
        pl.col("name"),
        pl.col("released"),
        pl.col("rating"),
        pl.col("ratings_count"),
        pl.col("rating_top"),
        pl.col("ratings")
            .fill_null([])
            .list.eval(pl.element().struct.field("id"))
            .alias("rating_classification_id"),
        pl.col("ratings")
            .fill_null([])
            .list.eval(pl.element().struct.field("count"))
            .alias("rating_classification_count"),
        pl.col("ratings")
            .fill_null([])
            .list.eval(pl.element().struct.field("percent"))
            .alias("rating_classification_percent"),
        pl.col("added")
            .alias("added_in_catalog_count"),
        pl.col("added_by_status")
            .struct.field("yet")
            .fill_null(0)
            .alias("added_status_yet"),
        pl.col("added_by_status")
            .struct.field("owned")
            .fill_null(0)
            .alias("added_status_owned"),
        pl.col("added_by_status")
            .struct.field("beaten")
            .fill_null(0)
            .alias("added_status_beaten"),
        pl.col("added_by_status")
            .struct.field("toplay")
            .fill_null(0)
            .alias("added_status_toplay"),
        pl.col("added_by_status")
            .struct.field("dropped")
            .fill_null(0)
            .alias("added_status_dropped"),
        pl.col("added_by_status")
            .struct.field("playing")
            .fill_null(0)
            .alias("added_status_playing"),
        # pl.col("rating_id"),
        pl.col("reviews_count"),
        pl.col("reviews_text_count"),
        pl.col("suggestions_count"),
        pl.col("tba"),
        pl.col("updated")
            .alias("updated_on_rawg"),
        pl.col("esrb_rating")
            .struct.field("id")
            .alias("esrb_rating_id"),
        pl.col("platforms")
            .fill_null([])
            .list.eval(pl.element().struct.field("platform").struct.field("id"))
            .alias("platform_id"),
        pl.col("platforms")
            .fill_null([])
            .list.eval(pl.element().struct.field("platform").struct.field("name"))
            .alias("platforms"),
        pl.col("platforms")
            .fill_null([])
            .list.eval(pl.element().struct.field("requirements")
            .struct.field("minimum"))
            .alias("requirements_minimum"),
        pl.col("platforms")
            .fill_null([])
            .list.eval(pl.element().struct.field("requirements")
            .struct.field("recommended"))
            .alias("requirements_recommended"),
        pl.col("genres")
            .fill_null([])
            .list.eval(pl.element().struct.field("name"))
            .alias("genres"),
        pl.col("stores")
            .fill_null([])
            .list.eval(pl.element().struct.field("store").struct.field("id"))
            .alias("store_id"),
    ).with_columns(pl.lit(latest_file_timestamp).alias("updated_at"))


    df = df.cast(GAME_SCHEMA)

    logger.info(
        "Finished reading JSON and creating DataFrame at %s. Shape: %s",
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        df.shape,
    )
    # fmt: on

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

    upload_file(
        local_path=str(filename), filename=filename, bucket="rawg-data", folder="games"
    )
    update_table(table_name="rawg_games", data_to_update=df_cleaned)

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
