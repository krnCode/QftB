"""
File for the about page in the app.
"""

import streamlit as st

st.set_page_config(page_icon="🎮", layout="wide")

st.title(body="About")
st.divider()

col1, col2 = st.columns(spec=2, border=True)

with col1:
    st.markdown(
        body="""
        ## Purpose
        A production-grade data analytics platform that ingests game data from multiple sources,
        transforms it through dimensional modeling, and exposes it via interactive dashboards.
        Demonstrates end-to-end data engineering: async ETL → dbt → analytics layer.


        ## Technical Approach
        - **Async/concurrent ETL:** High-throughput API ingestion with backoff retries and streaming writes
        - **3-layer dbt modeling:** Staging → Intermediate (array unnesting, joins) → Mart (aggregations, window functions)
        - **Automated orchestration:** GitHub Actions scheduling (3x/week delta loads)
        - **Observability:** Structured logging, data quality tests, Supabase as source-of-truth


        ## Tech Stack
        - **ETL:** Python 3.13, asyncio, aiohttp, Polars (typed schemas)
        - **Storage:** Supabase (PostgreSQL), Parquet, JSONL
        - **Transformation:** dbt-core, SQL, PostgreSQL functions
        - **Dashboard:** Streamlit, Altair, Polars (client-side)
        - **Orchestration:** GitHub Actions, uv

        """
    )

with col2:
    st.markdown(
        body="""
        ## Key Features
        - **Delta load ingestion:** Only new records fetched each run (idempotent, cost-efficient)
        - **Array modeling:** Native PostgreSQL arrays unnested via dbt intermediate layer
        - **Window functions:** `DENSE_RANK()` for top-N analysis across time periods
        - **Data quality:** dbt source contracts, `unique`/`not_null` tests on all PKs
        - **KPIs & time-series:** Release trends, tag analysis, platform comparisons

        ## Roadmap
        - Additional sources: Steam, OpenCritic, Metacritic
        - Advanced analytics: genre trends, rating distributions, temporal patterns
        - Enhanced filtering & drill-downs


        ## Acknowledgments
        Thanks to the RAWG API team for making game data openly accessible.

        Thank you for exploring this project! 🎮

        """
    )
