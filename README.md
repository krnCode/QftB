# Quest for the Best (QftB)

## Description

Quest for the Best (QftB) is a data pipeline and dashboard application designed to help users find the best video games based on their scores. It gathers data from various sources, starting with the [RAWG API](https://rawg.io/), cleans and processes the information, and presents it in an interactive Streamlit dashboard. The primary goal is to provide a centralized place to compare game scores and other metrics across different platforms.

## Features

*   **ETL Pipeline:** A robust ETL (Extract, Transform, Load) pipeline to fetch data from the RAWG API.
*   **Data Cleaning:** Scripts to clean and prepare the raw data for analysis.
*   **Interactive Dashboard:** A user-friendly dashboard built with Streamlit to visualize and explore the game data.
*   **Extensible:** The project is designed to be easily extended with new data sources in the future.

## Project Structure

The project is organized into two main directories:

*   `src/`: Contains the core ETL pipeline logic, including data fetchers, cleaners, and loaders.
*   `app/`: Contains the Streamlit application code for the user-facing dashboard.

## Technologies Used

*   **Python:** The primary programming language for the project.
*   **Streamlit:** For building the interactive web dashboard.
*   **Polars:** For high-performance data manipulation and analysis.
*   **Requests:** For making HTTP requests to the RAWG API.
*   **uv:** For dependency management.

## Setup and Installation

To get the project up and running on your local machine, follow these steps:

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/krnCode/QftB.git
    cd QftB
    ```

2.  **Install dependencies:**
    This project uses `uv` for package management. To install the dependencies, run:
    *(Note: You may need to generate a `requirements.txt` file from `pyproject.toml` if one is not present.)*
    ```bash
    uv pip install -r requirements.txt 
    ```

3.  **Set up environment variables:**
    The application requires an API key from RAWG.
    *   Create a `.env` file in the root of the project.
    *   Add your RAWG API key to the `.env` file:
        ```
        RAWG_API_KEY="your_api_key_here"
        ```

## Usage

### Running the ETL Pipeline

To run the ETL pipeline and fetch the latest game data, you will need to execute the main pipeline script.
*(For now, the project is running manually the pipeline scripts, but I plan to add a scheduler in the future, or integrate it in GitHub Actions.)*

### Launching the Dashboard

To launch the Streamlit dashboard, run the following command in your terminal:

```bash
streamlit run app/streamlit_app/main.py
```

## Future Work

I plan to expand the project by integrating additional data sources, such as:

*   Steam
*   OpenCritic
*   Metacritic

Also, to make the data collection automated, I plan to add a scheduler to run the pipeline scripts automatically.

This will provide a more comprehensive view of game scores and help users make even more informed decisions.
