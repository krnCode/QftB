"""
File for the main dashboard of the game data page
"""

import streamlit as st
import polars as pl
import altair as alt

from pathlib import Path
from supabase import create_client, Client

st.set_page_config(page_title="Overview", layout="wide")


# region ------------ Supabase connection ------------
@st.cache_resource
def init_connection():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)


supabase: Client = init_connection()

# endregion


# region ------------ Query data from supabase------------
@st.cache_data(ttl=3600)
def get_all_mart_games_data() -> list[dict]:
    """
    Function to paginate all the data available on supabase and save as a polars
    dataframe.

    Returns:
        pl.DataFrame: Polars dataframe with all the queried data
    """
    all_results: list[dict] = []
    batch_size: int = 1000
    start = 0

    while True:
        response = (
            supabase.schema("public_marts")
            .table("mart_games")
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


def get_releases_by_month_year() -> pl.DataFrame:
    """
    Function to get the table mart_game_releases_by_month_year.

    Returns:
        pl.DataFrame: Polars dataframe with the queried data
    """
    return pl.DataFrame(
        data=supabase.schema("public_marts")
        .table("mart_game_releases_by_month_year")
        .select("*")
        .execute()
        .data,
        strict=False,
    )


# endregion


# region ------------ Releases by month/year ------------
st.write("All data from the RAWG API: https://rawg.io/")

st.title("Game Releases by month/year")

releases_by_month_year: pl.DataFrame = pl.DataFrame(
    data=get_releases_by_month_year(),
    strict=False,
)

releases_by_month_year = releases_by_month_year.select(
    pl.col("month_year"), pl.col("game_count")
)

releases_by_month_year = releases_by_month_year.with_columns(
    (pl.col("month_year") + "-01").str.to_date(format="%Y-%m-%d").alias("month_year")
)

# Creating labels for the dates
chart_data = releases_by_month_year.with_columns(
    pl.col("month_year").dt.strftime("%Y-%m").alias("month_year_sort"),
    pl.col("month_year").dt.strftime("%b %Y").alias("month_year_label"),
).sort(by="month_year_sort", descending=False)

max_games_released = chart_data.select("game_count").max().item()
min_games_released = chart_data.select("game_count").min().item()

# Creating highlights for max and min games released
chart_data = chart_data.with_columns(
    pl.when(pl.col("game_count") == max_games_released)
    .then(pl.lit("Most Releases"))
    .when(pl.col("game_count") == min_games_released)
    .then(pl.lit("Least Releases"))
    .otherwise(pl.lit("Other"))
    .alias("highlight"),
).sort(by="month_year_sort", descending=True)

base = alt.Chart(data=chart_data).encode(
    x=alt.X(
        "month_year_label:O",
        title="Month / Year",
        sort=None,
        axis=alt.Axis(labelAngle=0),
    ),
    y=alt.Y("game_count:Q", title="Total Games Released"),
    color=alt.Color(
        "highlight:N",
        scale=alt.Scale(
            domain=["Most Releases", "Least Releases", "Other"],
            range=["#55c05b", "#F1A025", "#7F9CB4"],
        ),
        legend=alt.Legend(title=""),
    ),
)

releases_chart = base.mark_bar() + base.mark_text(
    dy=-8, color="grey", fontSize=13, fontWeight="bold"
).encode(text="game_count:Q")

st.altair_chart(altair_chart=releases_chart, width="stretch")

st.write("---")

# endregion


# region ------------ Game Data ------------
st.title("Game Data")

game_data_columns_config: dict = {
    "game_id": st.column_config.TextColumn(label="Game ID"),
    "slug": st.column_config.TextColumn(label="Slug"),
    "name": st.column_config.TextColumn(label="Name", pinned=True),
    "released": st.column_config.DateColumn(
        label="Released",
        format="DD/MM/YYYY",
        pinned=True,
    ),
    "rating": st.column_config.NumberColumn(label="Rating", pinned=True),
    "ratings_count": st.column_config.NumberColumn(label="Ratings Count", pinned=True),
    "platform_id": st.column_config.ListColumn(label="Platforms IDs"),
    "genre_id": st.column_config.ListColumn(label="Genres IDs"),
    "updated_at": st.column_config.DateColumn(label="Updated At", format="DD/MM/YYYY"),
    "description_raw": st.column_config.TextColumn(label="Game Description"),
    "tba": st.column_config.CheckboxColumn(
        label="TBA",
        help="""TBA means that the game does not have a release date yet
        (To Be Announced).""",
    ),
    "rating_overall": st.column_config.NumberColumn(label="Rating (Overall)"),
    "reviews_count": st.column_config.NumberColumn(
        label="Reviews (Creators)",
        help="""Total creators who have reviewed the game.""",
    ),
    "reviews_text_count": st.column_config.NumberColumn(
        label="Reviews (Writings)",
        help="""Reviews in writing/articles.""",
    ),
    "achievements_count": st.column_config.NumberColumn(label="Achievements"),
    "background_image": st.column_config.ImageColumn(
        label="Background Image on RAWG",
    ),
    "reddit_name": st.column_config.TextColumn(label="Reddit Name"),
    "reddit_url": st.column_config.LinkColumn(label="Reddit URL"),
    "esrb_rating_id": st.column_config.ListColumn(label="ESRB Rating ID"),
    "developer_id": st.column_config.ListColumn(label="Developers IDs"),
    "publisher_id": st.column_config.ListColumn(label="Publishers IDs"),
    "tag_id": st.column_config.ListColumn(label="Tags IDs"),
    "updated_on_rawg": st.column_config.DateColumn(
        label="Updated On RAWG",
        format="DD/MM/YYYY",
        help="""Date when the game was last updated on RAWG.""",
    ),
    "updated_at": st.column_config.DateColumn(
        label="Updated At",
        format="DD/MM/YYYY",
        help="""Date when the game was last updated on the QftB database.""",
    ),
}

data = pl.DataFrame(data=get_all_mart_games_data(), strict=False)

data = data.sort(by="released", descending=True)

toggle_rating_count = st.toggle(
    label="Show only games that have at least 1 rating count",
    value=True,
    help="""If this button is off, it will list all the games available.""",
)

# Drop background images column - include again after classification of NSFW games
data = data.drop(["background_image"])

if toggle_rating_count:
    data = data.filter(pl.col("ratings_count") > 0)

if data is not None:
    st.dataframe(
        data=data,
        column_config=game_data_columns_config,
        hide_index=True,
        selection_mode=["multi-cell", "multi-column"],
    )

else:
    st.warning("No data to show.")

st.write("---")
# endregion
