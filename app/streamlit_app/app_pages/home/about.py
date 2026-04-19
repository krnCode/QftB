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
        The main purpose of the app is to gather video games data (such as scores,
        ratings and other info) from various sources to show it in a single place. For 
        now, there's only data from RAWG, but over time I'll add more sources.


        ## Motivation
        Built out of curiosity and passion for games, this project helps me practice 
        modern tools while creating something fun.


        ## Tech Stack
        - **Python & uv** - Environment and package management  
        - **Streamlit**  - Intereactive dashboard  
        - **Supabase** - Database & storage (PostgreSQL)
        - **dbt** - Data transformation and modeling
        - **Polars**  - Fast data processing
        - **RAWG API** - Data source

        """
    )

with col2:
    st.markdown(
        body="""
        ## Features
        - Automated data fetching & cleaning from the RAWG API
        - Game overview dashboard with sorting and filtering by rating
        - Explore release dates, ratings, platforms, and genres
        
        ## Roadmap
        - More filtering optins (genre, platform, release year, etc)
        - Additional data sources beyond RAWG
        - Visualizations: genre trends, platform comparisons, rating distributions, etc  


        ## Acknowledgments
        Thanks for all the sources for making the data accessible for the general public
        and turning this project a reality.
        
        Also, thank you for your visit!

        """
    )
