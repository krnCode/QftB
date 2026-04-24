# dbt_qftb

dbt project for transforming raw [RAWG](https://rawg.io/) video game data into analytics-ready models.

## Sources

Raw data loaded from the RAWG API via a Python ETL pipeline into the `public` schema:

| Table | Description |
|---|---|
| `rawg_games` | One row per game (summary data) |
| `rawg_game_details` | Detailed per-game attributes |
| `rawg_tags` | Game tags reference table |

## Models

**Staging** — materialized as views in the `staging` schema.

| Model | Source |
|---|---|
| `stg_rawg__games` | `rawg_games` |
| `stg_rawg__game_details` | `rawg_game_details` |
| `stg_rawg__tags` | `rawg_tags` |

**Marts** — materialized as tables in the `marts` schema.

| Model | Description |
|---|---|
| `mart_games` | Primary analytics table. Joins games + details. One row per game. |
| `mart_game_releases_by_month_year` | Game release counts aggregated by month/year. |

## Usage

```bash
# Install dependencies
dbt deps

# Run all models
dbt run

# Run tests
dbt test

# Run staging only
dbt run --select staging

# Run marts only
dbt run --select marts
```

## Stack

- dbt-core
- dbt_utils `>=1.0.0`
- Target: Supabase (PostgreSQL)
