"""
Main file for the Streamlit app, will be used to run the app and the app pages
"""

import streamlit as st

pgs: dict = {
    "HOME": [
        st.Page(title="Welcome", page="./app_pages/home/welcome.py"),
        st.Page(title="About", page="./app_pages/home/about.py"),
    ],
    "RAWG": [
        st.Page(title="RAWG Analytics", page="./app_pages/game_data/rawg_analytics.py"),
    ],
}

pages = st.navigation(pgs)

pages.run()
