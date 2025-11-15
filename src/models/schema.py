"""
File to keep the schemas to define the data
"""

import polars as pl

SCHEMA = {
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
