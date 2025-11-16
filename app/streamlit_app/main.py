"""
Main file for the Streamlit app, will be used to run the app and the app pages
"""

import streamlit as st

pgs: dict = {
    "HOME": [
        st.Page(title="Welcome", page="./app_pages/home/welcome.py"),
        st.Page(title="About", page="./app_pages/home/about.py"),
    ],
    "DATA": [
        st.Page(title="Overview", page="./app_pages/game_data/main_dashboard.py"),
        # st.Page(title="Cleaning", page="cleaning.py"),
        # st.Page(title="Exploration", page="exploration.py"),
    ],
}

pages = st.navigation(pgs)

pages.run()
