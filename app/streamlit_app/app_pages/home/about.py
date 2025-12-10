"""
File for the about page in the app.
"""

import streamlit as st

st.set_page_config(page_icon="About", layout="wide")

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
        - Python & uv for environment management  
        - Streamlit for the dashboard  
        - Supabase for database & storage  

        """
    )

with col2:
    st.markdown(
        body="""
        ## Features
        - Automated data fetching & cleaning using the sources API
        - Interactive dashboard with filters and charts (on the way!)
        - Ability to explore ratings, release dates, trends and see informations about
        a game
        
        ## Future Plans
        - Add more options for filtering
        - More sources
        - Visualizations (e.g., genre trends, platform comparisons, game information, 
        etc)  


        ## Acknowledgments
        Thanks for all the sources for making the data accessible for the general public
        and turning this project a reality.
        
        Also, thank you for your visit!

        """
    )
