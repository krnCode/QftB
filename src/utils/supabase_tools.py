"""
File to handle functions that interact with the supabase database and tables
"""

import polars as pl

from pathlib import Path
from src.utils.supabase_client import supabase
from src.utils.logger import setup_logger

# region ------------ Logger setup ------------
logger = setup_logger(__name__)
# endregion


# region ------------ File uploads ------------
# Upload file to supabase bucket
def upload_file(local_path: Path, filename: str, bucket: str):
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
def update_table(table_name: str, data_to_update: pl.DataFramet):
    """
    Update a table in the supabase database

    ARGS:
        table_name (str): The name of the table to update
        data_to_update (pl.DataFrame): The data to update the table with
    """
    df_to_convert = pl.read_parquet(data_to_update)
    rows: list[dict] = df_to_convert.to_dicts()

    response = supabase.table(table_name).upsert(rows, on_conflict="game_id").execute()
    logger.info("Response from supabase: %s", response)
    logger.info("Updated table: %s", table_name)
    logger.info("Number of rows updated: %s", len(rows))


# endregion
