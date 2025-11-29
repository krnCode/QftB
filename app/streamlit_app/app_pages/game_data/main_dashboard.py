"""
File for the main dashboard of the game data page
"""

import os
import streamlit as st
import polars as pl

from pathlib import Path
from supabase import create_client, Client


st.set_page_config(page_title="Game Data", layout="wide")


# region ------------ Supabase connection ------------
@st.cache_resource
def init_connection():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)


supabase: Client = init_connection()

# endregion


# region ------------ Query data ------------
@st.cache_data(ttl=3600)
def run_query():
    return supabase.table("rawg_games_cleaned").select("*").execute()


# endregion

# # region ------------ Get project path ------------
# PROJECT_ROOT: Path = Path(__file__).parent.parent.parent.parent.parent
# DATA_LOCAL: Path = PROJECT_ROOT / "data_local" / "temp" / "rawg" / "games"
# # endregion

# # region ------------ Get recent data ------------
# # Get all files in the folder and sort them by most recent
# files: list[Path] = [
#     os.path.join(DATA_LOCAL, f)
#     for f in os.listdir(DATA_LOCAL)
#     if f.endswith(".parquet")
# ]

# files.sort(key=os.path.getmtime, reverse=True)
# latest_file: Path = files[0]
# # endregion


# # region ------------ Read parquet ------------
# data: pl.DataFrame = pl.read_parquet(latest_file)
# # endregion


# region ------------ Columns config ------------
columns_config: dict = {
    "game_id": st.column_config.TextColumn(label="Game ID"),
    "slug": st.column_config.TextColumn(label="Slug"),
    "name": st.column_config.TextColumn(label="Name", pinned=True),
    "released": st.column_config.DateColumn(label="Released", format="DD/MM/YYYY"),
    "rating": st.column_config.NumberColumn(label="Rating", pinned=True),
    "ratings_count": st.column_config.NumberColumn(label="Ratings Count"),
    "platforms": st.column_config.ListColumn(label="Platforms"),
    "genres": st.column_config.ListColumn(label="Genres"),
    "updated_at": st.column_config.DateColumn(label="Updated At", format="DD/MM/YYYY"),
}
# endregion


# region ------------ App ------------
st.title("Game Data")
st.write("Data from the RAWG API: https://rawg.io/")
st.write("---")


data: pl.DataFrame = pl.read_database(run_query)

if data is not None:
    st.dataframe(
        data=data,
        column_config=columns_config,
        hide_index=True,
        selection_mode=["multi-cell", "multi-column"],
    )
else:
    st.warning("No data to show.")
# endregion
