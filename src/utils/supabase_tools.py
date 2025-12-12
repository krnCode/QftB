"""
File to handle functions that interact with the supabase database and tables
"""

import polars as pl
import os

from pathlib import Path
from supabase import create_client, Client
from dotenv import load_dotenv
from src.utils.supabase_client import supabase
from src.utils.logger import setup_logger


# region ------------ Load env variables ------------
load_dotenv()

SUPABASE_URL: str = os.getenv("SUPABASE_URL")
SUPABASE_KEY: str = os.getenv("SUPABASE_KEY")

# endregion


# region ------------ Logger setup ------------
logger = setup_logger(__name__)
# endregion


# region ------------ Create connection ------------
def init_connection():
    """
    Runs the create_client function of supabase that returns a connection to the
    database

    Returns:
        Client: Instance of the client that connects to the database
    """
    url = SUPABASE_URL
    key = SUPABASE_KEY
    return create_client(url, key)


# endregion


# region ------------ File uploads ------------
# Upload file to supabase bucket
def upload_file(local_path: Path, filename: Path, bucket: str):
    """
    Uploads a file to a supabase bucket

    ARGS:
        local_path (Path): The local path of the file to upload
        bucket (str, optional): The name of the bucket to upload to.
    """
    with open(local_path, "rb") as f:
        file_bytes = f.read()
        supabase.storage.from_(bucket).upload(f"games/{filename.name}", file_bytes)
        logger.info(
            "Uploaded file to supabase bucket: FILE: %s / BUCKET: %s",
            local_path,
            bucket,
        )


# endregion


# region ------------ Update table ------------
# Update a table in the supabase database
def update_table(table_name: str, data_to_update: pl.DataFrame):
    """
    Update a table in the supabase database

    ARGS:
        table_name (str): The name of the table to update
        data_to_update (pl.DataFrame): The data to update the table with
    """
    # Convert dates to strings with polars to be JSON serialized by supabase
    data_to_update = data_to_update.with_columns(
        [
            data_to_update[col].cast(pl.String)
            for col, dtype in data_to_update.schema.items()
            if dtype in (pl.Date, pl.Datetime)
        ]
    )

    rows: list[dict] = data_to_update.to_dicts()

    response = supabase.table(table_name).upsert(rows, on_conflict="game_id").execute()
    logger.info("Updated table: %s", table_name)
    logger.info("Number of rows updated: %s", len(rows))


# endregion


# region ------------ Table Query ------------
# Query all data from rawg_games_cleaned
def query_all_data_rawg_games_cleaned() -> list[dict]:
    """
    Function to paginate all the data available on the table "rawg_games_cleaned"

    Returns:
        list[dict]: List of dictionaries with all the data
    """
    all_results: list[dict] = []
    batch_size: int = 1000
    start = 0

    while True:
        response = (
            supabase.table("rawg_games_cleaned")
            .select("*")
            .range(start=start, end=start + batch_size - 1)
            .execute()
        )

        data: list[dict] = response.data
        if not data:
            break
        all_results.extend(data)
        start += batch_size

    return all_results


# endregion
