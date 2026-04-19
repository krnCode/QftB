"""
Welcome page for the Streamlit app
"""

import os
import random
import streamlit as st

from pathlib import Path

st.set_page_config(page_title="Welcome")

# region ------------ Random logo selection ------------
logos_path: Path = Path(__file__).parent.parent.parent.parent / "assets"
logos_list: list[str] = os.listdir(logos_path)


def select_random_logo() -> str:
    """
    Selects a random logo from the list.

    Returns:
        str: Path to the selected logo.
    """
    return logos_path / random.choice(logos_list)


selected_logo: str = select_random_logo()
# endregion

# region ------------ Welcome page ------------
st.title("Quest for the Best")
st.write("---")

col1, col2 = st.columns(spec=2, vertical_alignment="center")
with col1:
    st.image(image=selected_logo)

with col2:
    st.write(
        """
        Welcome to **Quest for the Best** - a dashboard to help you discover and 
        compare video games based on ratings, release dates, and more. 

        Data is sourced from the [RAWG](https://rawg.io/) API, one of the largest video
        game databases avaliable.

        👈 Head to **Overview** in the sidebar to start exploring.
        """
    )
# endregion
