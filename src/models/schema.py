"""
File to keep the schemas to define the data
"""

import polars as pl

GAME_SCHEMA = {
    "game_id": pl.Int64,
    "slug": pl.Utf8,
    "name": pl.Utf8,
    "released": pl.Date,
    "rating": pl.Float64,
    "ratings_count": pl.Int64,
    "platforms": pl.List(pl.Utf8),
    "genres": pl.List(pl.Utf8),
    "updated_at": pl.Datetime,
}


GAME_DETAILS_SCHEMA = {
    "game_id": pl.Int64,
    "description_raw": pl.Utf8,
    "tba": pl.Boolean,
    "platform_id": pl.List(pl.Int64),
    "updated_on_rawg": pl.Datetime,
    "background_image": pl.Utf8,
    "background_image_additional": pl.Utf8,
    "reviews_count": pl.Int64,
    "reviews_text_count": pl.Int64,
    "ratings_count": pl.Int64,
    "rating_overall": pl.Float64,
    "rating_id": pl.List(pl.Int64),
    "esrb_rating_id": pl.Int64,
    "genre_id": pl.List(pl.Int64),
    "tag_id": pl.List(pl.Int64),
    "achievements_count": pl.Int64,
    "reddit_logo": pl.Utf8,
    "reddit_name": pl.Utf8,
    "reddit_description": pl.Utf8,
    "reddit_url": pl.Utf8,
    "developer_id": pl.List(pl.Int64),
    "publisher_id": pl.List(pl.Int64),
    "updated_at": pl.Datetime,
}


TAGS_SCHEMA = {
    "tag_id": pl.Int64,
    "name": pl.Utf8,
    "slug": pl.Utf8,
    "games_count": pl.Int64,
    "image_background": pl.Utf8,
    "language": pl.Utf8,
    "updated_at": pl.Datetime,
}


TAG_DETAILS_SCHEMA = {
    "tag_id": pl.Int64,
    "name": pl.Utf8,
    "slug": pl.Utf8,
    "games_count": pl.Int64,
    "image_background": pl.Utf8,
    "description": pl.Utf8,
    "updated_at": pl.Datetime,
}


PLATFORMS_SCHEMA = {
    "platform_id": pl.Int64,
    "name": pl.Utf8,
    "slug": pl.Utf8,
    "games_count": pl.Int64,
    "image_background": pl.Utf8,
    "image": pl.Utf8,
    "year_start": pl.Int64,
    "year_end": pl.Int64,
    "updated_at": pl.Datetime,
}
