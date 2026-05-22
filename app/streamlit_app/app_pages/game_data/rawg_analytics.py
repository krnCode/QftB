"""
File for the main dashboard of the game data page
"""

import streamlit as st
import polars as pl
import altair as alt
import datetime

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
def get_mart_rawg__games() -> list[dict]:
    """
    Function to paginate all the data available on supabase and save as a polars
    dataframe.

    Returns:
        pl.DataFrame: list of dicts with all the queried data
    """
    all_results: list[dict] = []
    batch_size: int = 1000
    start = 0

    while True:
        response = (
            supabase.schema("public_marts")
            .table("mart_rawg__games")
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


@st.cache_data(ttl=3600)
def get_mart_rawg__releases_by_games_monthyear() -> pl.DataFrame:
    """
    Function to get the table mart_game_releases_by_month_year.

    Returns:
        pl.DataFrame: Polars dataframe with the queried data
    """
    return pl.DataFrame(
        data=supabase.schema("public_marts")
        .table("mart_rawg__releases_by_games_monthyear")
        .select("*")
        .execute()
        .data,
        strict=False,
    )


@st.cache_data(ttl=3600)
def get_mart_rawg__releases_by_gametags_monthyear() -> list[dict]:
    """
    Function to paginate all the data available on the table
    mart_rawg__releases_by_gametags_monthyear and save as a polars dataframe.

    Returns:
        pl.DataFrame: list of dicts with the queried data
    """
    all_results: list[dict] = []
    batch_size: int = 1000
    start = 0

    while True:
        response = (
            supabase.schema("public_marts")
            .table("mart_rawg__releases_by_gametags_monthyear")
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


@st.cache_data(ttl=3600)
def get_mart_rawg__releases_by_games_platform() -> pl.DataFrame:
    """
    Function to get the table mart_rawg__releases_by_games_platform.

    Returns:
        pl.DataFrame: Polars dataframe with the queried data
    """
    all_results: list[dict] = []
    batch_size: int = 1000
    start = 0

    while True:
        response = (
            supabase.schema("public_marts")
            .table("mart_rawg__releases_by_games_platform")
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


st.title(body="RAWG Analytics", text_alignment="center")
st.write("---")

# region ------------ Metrics ------------

metric_col1, metric_col2, metric_col3 = st.columns(spec=3, vertical_alignment="center")

with metric_col1:
    total_game_releases: int = (
        pl.DataFrame(
            data=get_mart_rawg__releases_by_games_monthyear(),
            strict=False,
        )
        .select("game_count")
        .sum()
        .item()
    )

    last_update_date: str = (
        pl.DataFrame(
            data=get_mart_rawg__games(),
            strict=False,
        )
        .select("updated_at")
        .max()
        .item()[:10]
    )

    start_date: str = "2025-09-01"
    end__date: str = last_update_date

    st.metric(
        label="Total Released Games",
        value=total_game_releases,
        border=True,
        help=f"Total game releases from {start_date} to {end__date}",
    )

with metric_col2:
    tag_releases: pl.DataFrame = pl.DataFrame(
        data=get_mart_rawg__releases_by_gametags_monthyear(),
        strict=False,
    )

    total_by_tag: pl.DataFrame = tag_releases.group_by("game_tag").agg(
        pl.col("tag_count").sum().alias("total_by_tag")
    )

    most_released_tags: pl.DataFrame = (
        total_by_tag.sort(by="total_by_tag", descending=True)
        .limit(1)
        .select("game_tag")
        .item()
    )

    st.metric(
        label="Most Released Tag",
        value=most_released_tags,
        border=True,
    )

with metric_col3:
    total_game_releases: int = pl.DataFrame(
        data=get_mart_rawg__releases_by_games_monthyear(),
        strict=False,
    )

    period_most_releases: pl.DataFrame = (
        total_game_releases.sort(by="game_count", descending=True)
        .limit(1)
        .select("month_year")
        .item()
    )

    period_least_releases: pl.DataFrame = (
        total_game_releases.sort(by="game_count", descending=False)
        .limit(1)
        .select("month_year")
        .item()
    )

    st.metric(
        label="Period with Most Releases",
        value=period_most_releases,
        delta_arrow="off",
        delta=period_least_releases,
        delta_color="orange",
        delta_description="Least Releases",
        border=True,
    )

st.write("---")
# endregion

# region ------------ Visualizations ------------

col1, col2 = st.columns(spec=2, vertical_alignment="top", gap="large")

with col1:
    # region ------------ Releases by games and month/year ------------
    st.markdown("""
        ### Total Game Releases by Month / Year
        """)

    releases_by_month_year: pl.DataFrame = pl.DataFrame(
        data=get_mart_rawg__releases_by_games_monthyear(),
        strict=False,
    )

    releases_by_month_year = releases_by_month_year.select(
        pl.col("month_year"), pl.col("game_count")
    )

    releases_by_month_year = releases_by_month_year.with_columns(
        (pl.col("month_year") + "-01")
        .str.to_date(format="%Y-%m-%d")
        .alias("month_year")
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
    ).sort(by="month_year_sort", descending=False)

    tags_chart_base = alt.Chart(data=chart_data).encode(
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
            legend=alt.Legend(title="", orient="top"),
        ),
        tooltip=[
            alt.Tooltip("month_year_label:O", title="Month / Year"),
            alt.Tooltip("game_count:Q", title="Total Games Released"),
            alt.Tooltip("highlight:N", title="Highlight"),
        ],
    )

    releases_chart = tags_chart_base.mark_bar() + tags_chart_base.mark_text(
        dy=-8, color="grey", fontSize=13, fontWeight="bold"
    ).encode(text="game_count:Q")

    st.altair_chart(altair_chart=releases_chart, width="stretch")

    # endregion

with col2:
    # region ------------ Releases by Platform ------------
    st.markdown("### Most Releases by Platform")

    releases_by_platform: pl.DataFrame = pl.DataFrame(
        data=get_mart_rawg__releases_by_games_platform(),
        strict=False,
    )

    releases_by_platform = releases_by_platform.select(
        pl.col("platform_name"),
        pl.col("parent_platform_name"),
        pl.col("platform_releases_count"),
        pl.col("month_year"),
        pl.col("rank_category"),
    )

    releases_by_platform = releases_by_platform.with_columns(
        (pl.col("month_year") + "-01")
        .str.to_date(format="%Y-%m-%d")
        .alias("month_year")
    )

    games_platform_chart_base = alt.Chart(data=releases_by_platform).encode(
        y=alt.Y(
            "platform_releases_count:Q",
            title="Releases",
            sort=None,
            axis=alt.Axis(labelAngle=0),
            aggregate="sum",
            scale=alt.Scale(type="symlog"),
        ),
        x=alt.X(
            "parent_platform_name:N",
            title="Parent Platform",
            sort="-y",
            axis=alt.Axis(labelAngle=0),
            stack=True,
        ),
        color=alt.Color(
            "platform_name:N",
            scale=alt.Scale(
                domain=releases_by_platform.select("platform_name")
                .unique()
                .to_series(),
            ),
            legend=alt.Legend(
                title="Platform",
                orient="right",
                columns=1,
            ),
        ),
    )

    games_platform_chart = games_platform_chart_base.mark_bar()

    st.altair_chart(altair_chart=games_platform_chart, width="stretch")

    with st.expander("More info for this chart"):
        st.caption(
            body="""
            Each game can be released on multiple platforms.

            This chart (Most Releases by Platform) is using a symlog scale to better 
            display the data.

            The source don't distinguish some platforms, for example, Nintendo only have 
            Nintendo Switch displayed, even though there are releases on 
            Nintendo Switch 2.
            """,
            text_alignment="left",
        )

    # endregion

st.write("---")

# region ------------ Releases by Tags and month/year ------------
st.markdown("""
    ### Most Released Tags (Top 5)
    """)


games_by_tag: pl.DataFrame = pl.DataFrame(
    data=get_mart_rawg__releases_by_gametags_monthyear(),
    strict=False,
)

games_by_tag = games_by_tag.with_columns(
    (pl.col("month_year") + "-01").str.to_date(format="%Y-%m-%d").alias("month_year")
)

chart_data = games_by_tag.with_columns(
    pl.col("month_year").dt.strftime("%Y-%m").alias("month_year_sort"),
    pl.col("month_year").dt.strftime("%b %Y").alias("month_year_label"),
).sort(by="month_year_sort", descending=False)

chart_data = chart_data.filter(pl.col("rank") <= 5)

current_month_year = datetime.datetime.now().strftime("%Y-%m")
chart_data = chart_data.filter(pl.col("month_year_sort") != current_month_year)

tags_chart_base = (
    alt.Chart(data=chart_data)
    .encode(
        y=alt.Y(
            "month_year_label:O",
            title="Month / Year",
            sort=None,
            axis=alt.Axis(labelAngle=0),
        ),
        x=alt.X(
            "tag_count:Q",
            title="Releases by Tags",
            sort=None,
            axis=alt.Axis(
                labelAngle=0,
            ),
        ),
        tooltip=[
            alt.Tooltip("game_tag:N", title="Tag"),
            alt.Tooltip("month_year_label:O", title="Release Date"),
            alt.Tooltip("rank_category:N", title="Rank Category"),
            alt.Tooltip("tag_count:Q", title="Total Releases"),
        ],
    )
    .properties(
        width=210,
        height=200,
    )
)

tags_name_chart_text = tags_chart_base.mark_text(
    dx=5,
    fontSize=12,
    fontWeight="normal",
    fill="sienna",
    align="left",
).encode(
    text="game_tag:N",
)

tags_chart = (tags_chart_base.mark_bar() + tags_name_chart_text).facet(
    facet=alt.Facet(
        "rank_category:N",
        title="Tags by Rank / Period",
    ),
    columns=5,
)

st.altair_chart(altair_chart=tags_chart)

with st.expander("More info for this chart"):
    st.caption(
        body="""
        The charts displays the rank of the tags in the period displayed to show the 
        evolution of the ranking over time.
        
        Each game can have multiple tags, they are used to classify the games into
        different categories or describe features.

        Removed current month since we can have multiple tags in the ranking until there 
        are more releases.
        """,
        text_alignment="left",
    )

# endregion

st.write("---")

# endregion

st.markdown(
    body=""" ###### All data from the RAWG API: https://rawg.io/""",
)
