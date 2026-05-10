# Quest for the Best (QftB)

**End-to-end data analytics project** that demonstrates production-grade ETL, dbt data modeling, and interactive analytics dashboards. Built to showcase modern data engineering practices and cloud-native architecture.

## What This Project Demonstrates

- **Async/concurrent ETL:** High-throughput API ingestion with `asyncio` + `aiohttp`, semaphore-controlled concurrency, exponential backoff, and streaming writes to prevent data loss
- **Advanced dbt modeling:** 3-layer dimensional architecture (staging → intermediate → mart) with array unnesting, window functions, and source contracts with data quality tests
- **Automated pipelines:** Scheduled ETL via GitHub Actions; idempotent, delta-load ingestion pattern
- **Production observability:** Structured logging, error handling, Supabase as both raw storage and query layer
- **Analytics dashboards:** Interactive Streamlit app with KPIs, time-series analysis, and optimized Supabase queries with client-side Polars processing

## Architecture

```
RAWG API → Async Fetchers (aiohttp) → Polars Cleaners → Supabase (DB + Storage) → dbt Models → Streamlit Dashboard
```

**Pipeline:** Runs automatically via GitHub Actions every Monday, Wednesday, and Friday at 2:00 AM UTC. Raw responses are written as JSONL (streaming-safe), cleaned data staged as Parquet, and final records upserted into Supabase using a delta load pattern.

## Technical Highlights

### Async ETL Pipeline
- Concurrent page fetching with `Semaphore(5)` concurrency control to respect API rate limits
- Exponential backoff retry strategy (up to 3 attempts) for transient failures
- JSONL streaming writes to prevent data loss during long-running API quota exhaustion scenarios
- Delta load pattern: only new records not already in Supabase are fetched each run
- Schema-enforced Polars transforms with type safety and nullable nested struct handling

### dbt Data Modeling (3-Layer Architecture)
- **Staging:** Thin rename/cast wrappers over raw Supabase source tables (5 models: games, game_details, tags, platforms, parent_platforms)
- **Intermediate:** Array unnesting via PostgreSQL `unnest()` function; game+tag and platform hierarchy joins (2 models)
- **Mart:** Fact tables and time-series aggregations with `DENSE_RANK()` window functions for top-N analysis across games, tags, platforms, and release timelines (4 models)
- Full dbt source contracts + `unique` and `not_null` tests on all primary keys for data quality assurance

### Automated Scheduling & Observability
- GitHub Actions orchestration running 3x per week on a fixed schedule
- Structured logging (debug/info levels) for pipeline visibility and error diagnostics
- Supabase as the source-of-truth: raw Parquet files stored as backup; mart tables serve dashboard queries directly

### Interactive Analytics Dashboard
- **KPIs:** Total game releases, most-released tag, peak/trough release periods
- **Visualizations:** Month/year time-series bar charts with highlighted extrema; stacked normalized bar charts for tag trends
- **Performance:** Polars client-side aggregations with `st.cache_data` (1-hour TTL) and paginated Supabase reads (1K rows/page)
- Built with Altair for declarative charting and Streamlit for rapid iteration

## Tech Stack

| Layer | Technology |
|-------|-----------|
| **Orchestration** | GitHub Actions, Python 3.13 |
| **ETL** | asyncio, aiohttp, Polars (typed schemas), Python |
| **Storage** | Supabase (PostgreSQL), Parquet, JSONL |
| **Transformation** | dbt-core, SQL, PostgreSQL functions (`unnest`, window functions) |
| **Analytics** | Streamlit, Altair, Polars (client-side) |
| **Dependency Mgmt** | uv |

## Status & Roadmap

**Currently:** Fully functional end-to-end pipeline with live dashboard. Actively maintained with continuous enhancements to data coverage and dashboard visualizations.

**Planned:** Additional data sources (Steam, OpenCritic, Metacritic) to broaden game score coverage and comparative analysis capabilities.
