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
def upload_file(local_path: Path, filename: Path, bucket: str, folder: str):
    """
    Uploads a file to a supabase bucket

    ARGS:
        local_path (Path): The local path of the file to upload
        bucket (str, optional): The name of the bucket to upload to
        folder (str, optional): The folder on the bucket to upload the file to
    """
    with open(local_path, "rb") as f:
        file_bytes = f.read()
        supabase.storage.from_(bucket).upload(f"{folder}/{filename.name}", file_bytes)
        logger.info(
            "Uploaded file to supabase bucket: FILE: %s / BUCKET: %s",
            local_path,
            bucket,
        )


# endregion


# region ------------ Update table ------------
# Update a table in the supabase database
def update_table(
    table_name: str, data_to_update: pl.DataFrame, on_conflict: str = "game_id"
):
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

    response = (
        supabase.table(table_name).upsert(rows, on_conflict=on_conflict).execute()
    )
    logger.info("Updated table: %s", table_name)
    logger.info("Number of rows updated: %s", len(rows))


# endregion


# region ------------ Query all games ------------
# Query all data from rawg_games
def query_all_data_rawg_games() -> list[dict]:
    """
    Function to paginate all the data available on the table "rawg_games"

    Returns:
        list[dict]: List of dictionaries with all the data
    """
    all_results: list[dict] = []
    batch_size: int = 1000
    start = 0

    while True:
        response = (
            supabase.table("rawg_games")
            .select("game_id")
            .range(start=start, end=start + batch_size - 1)
            .execute()
        )

        data: list[dict] = response.data
        if not data:
            break
        all_results.extend(data)
        start += batch_size

    return all_results


# Query all data from rawg_games
def query_all_data_rawg_tags() -> list[dict]:
    """
    Function to paginate all the data available on the table "rawg_tags"

    Returns:
        list[dict]: List of dictionaries with all the data
    """
    all_results: list[dict] = []
    batch_size: int = 1000
    start = 0

    while True:
        response = (
            supabase.table("rawg_tags")
            .select("tag_id")
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


# region ------------ Query existing game details ------------
# Search for game ids  already have the game details in the table
def query_existing_game_details_ids() -> list[int]:
    """
    Function to query all the game ids that already have the game details in the
    table "rawg_game_details"

    Returns:
        list[int]: List of game ids
    """
    all_existing_ids: list[int] = []
    limit: int = 1000
    offset: int = 0

    while True:
        response = (
            supabase.table("rawg_game_details")
            .select("game_id")
            .range(start=offset, end=offset + limit - 1)
            .execute()
        )

        data: list[dict] = response.data

        if not data:
            break

        batch_ids: list[int] = [int(item["game_id"]) for item in data]
        all_existing_ids.extend(batch_ids)

        if len(data) < limit:
            break

        offset += limit

    return all_existing_ids


# Search for tag ids  already have the tag details in the table
def query_existing_tag_details_ids() -> list[int]:
    """
    Function to query all the tag ids that already have the tag details in the
    table "rawg_tag_details"

    Returns:
        list[int]: List of tag ids
    """
    all_existing_ids: list[int] = []
    limit: int = 1000
    offset: int = 0

    while True:
        response = (
            supabase.table("rawg_tag_details")
            .select("tag_id")
            .range(start=offset, end=offset + limit - 1)
            .execute()
        )

        data: list[dict] = response.data

        if not data:
            break

        batch_ids: list[int] = [int(item["tag_id"]) for item in data]
        all_existing_ids.extend(batch_ids)

        if len(data) < limit:
            break

        offset += limit

    return all_existing_ids


# endregion


# region ------------ Query all tags ------------
# Query all data from rawg_games
def query_all_data_rawg_tags() -> list[dict]:
    """
    Function to paginate all the data available on the table "rawg_tags"

    Returns:
        list[dict]: List of dictionaries with all the data
    """
    all_results: list[dict] = []
    batch_size: int = 1000
    start = 0

    while True:
        response = (
            supabase.table("rawg_tags")
            .select("tag_id")
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


# region ------------ Query existing tag details ------------
def query_existing_tags_ids() -> list[int]:
    """
    Function to query all the tag ids that already have the tag details in the
    table "rawg_tag_details"

    Returns:
        list[int]: List of game ids
    """
    all_existing_ids: list[int] = []
    limit: int = 1000
    offset: int = 0

    while True:
        response = (
            supabase.table("rawg_tag_details")
            .select("tag_id")
            .range(start=offset, end=offset + limit - 1)
            .execute()
        )

        data: list[dict] = response.data

        if not data:
            break

        batch_ids: list[int] = [int(item["tag_id"]) for item in data]
        all_existing_ids.extend(batch_ids)

        if len(data) < limit:
            break

        offset += limit

    return all_existing_ids


# endregion
