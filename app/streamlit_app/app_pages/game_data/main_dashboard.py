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


data = run_query()

if data:
    st.dataframe(
        data=data,
        column_config=columns_config,
        hide_index=True,
        selection_mode=["multi-cell", "multi-column"],
    )
else:
    st.warning("No data to show.")
# endregion
