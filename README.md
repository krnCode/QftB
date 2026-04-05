# Quest for the Best (QftB)

## Description

Quest for the Best (QftB) is a data pipeline and dashboard application designed to help users find the best video games based on their scores. It gathers data from various sources, starting with the [RAWG API](https://rawg.io/), cleans and processes the information, and presents it in an interactive Streamlit dashboard. The primary goal is to provide a centralized place to compare game scores and other metrics across different platforms.

## Architecture

```
RAWG API → Async Fetchers (aiohttp) → Polars Cleaners → Supabase (DB + Storage) → Streamlit Dashboard
```

The pipeline runs automatically via **GitHub Actions** every Monday, Wednesday, and Friday at 2:00 AM UTC. Raw responses are saved locally as JSON/JSONL files, cleaned data is stored as Parquet files, and final records are upserted into Supabase using a delta load pattern (only new data is fetched each run).

## Features

*   **ETL Pipeline:** A robust ETL (Extract, Transform, Load) pipeline to fetch data from the RAWG API.
*   **Async Fetching:** Concurrent API requests with semaphore control, retries, and exponential backoff.
*   **Delta Loads:** Only new games not already in the database are fetched on each pipeline run.
*   **Data Cleaning:** Schema-enforced Polars transforms to clean and prepare raw data for analysis.
*   **Interactive Dashboard:** A user-friendly dashboard built with Streamlit to visualize and explore the game data.
*   **Automated Scheduling:** GitHub Actions runs the pipeline automatically 3 times per week.
*   **Extensible:** Designed to be easily extended with new data sources in the future.

## Project Structure

```
QftB/
├── .github/workflows/      # GitHub Actions CI/CD pipeline
├── app/streamlit_app/      # Streamlit dashboard and pages
├── src/
│   ├── etl/
│   │   ├── fetchers/       # Extract: async API fetchers
│   │   └── cleaners/       # Transform: Polars data cleaners
│   ├── models/             # Data schemas (Polars types)
│   └── utils/              # Logger, Supabase client, DB tools
├── data_local/             # Local raw JSON and cleaned Parquet files (not tracked in git)
└── pyproject.toml          # Project metadata and dependencies
```

## Technologies Used

*   **Python 3.13:** The primary programming language for the project.
*   **Streamlit:** For building the interactive web dashboard.
*   **Polars:** For high-performance, schema-enforced data manipulation and analysis.
*   **aiohttp:** For async concurrent HTTP requests to the RAWG API.
*   **Supabase:** Cloud PostgreSQL database and file storage for cleaned data.
*   **GitHub Actions:** Automated scheduling of the ETL pipeline.
*   **uv:** For dependency management.
*   **dbt:** Planned — for building a datamart layer on top of the raw Supabase tables.

## Setup and Installation

### 1. Clone the repository

```bash
git clone https://github.com/krnCode/QftB.git
cd QftB
```

### 2. Install dependencies

This project uses `uv` for package management. Install all dependencies with:

```bash
uv sync
```

### 3. Set up environment variables

Create a `.env` file in the root of the project with the following variables:

```env
# RAWG API keys (register at https://rawg.io/apidocs)
RAWG_API_KEY="your_rawg_api_key_here"
RAWG_API_KEY_2="your_second_rawg_api_key_here"

# Supabase credentials (found in your Supabase project settings)
SUPABASE_URL="your_supabase_project_url"
SUPABASE_KEY="your_supabase_service_role_key"

# Logging level (optional — 10=DEBUG, 20=INFO; defaults to INFO)
LOGGER_LEVEL=20
```

## Usage

### Running the ETL Pipeline Manually

The pipeline consists of 4 scripts that must be run in the following order:

```bash
# Step 1: Fetch game listings from RAWG
uv run src/etl/fetchers/rawg_fetcher_games.py

# Step 2: Clean and load game data into Supabase
uv run src/etl/cleaners/rawg_cleaner_games.py

# Step 3: Fetch detailed info for each game
uv run src/etl/fetchers/rawg_fetcher_game_details.py

# Step 4: Clean and load game details into Supabase
uv run src/etl/cleaners/rawg_cleaner_game_details.py
```

> The pipeline also runs **automatically** via GitHub Actions every Monday, Wednesday, and Friday at 2:00 AM UTC.

### Launching the Dashboard

```bash
streamlit run app/streamlit_app/main.py
```

## Future Work

I plan to expand the project by:

*   **Additional data sources:** Steam, OpenCritic, and Metacritic to provide a more comprehensive view of game scores.
*   **dbt datamart:** A transformation layer on top of the raw Supabase tables to power richer dashboard queries.
*   **Enhanced dashboard:** Charts, filters, and KPIs for deeper exploration of the data.
*   **Tags pipeline:** Complete and integrate the RAWG tags fetcher into the automated pipeline.
