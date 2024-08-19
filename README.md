# Project-3-bootcamp

## Objective
This data engineering project uses Dagster to orchestrate a batch ELT solution. The solution pulls environmental data from two NZ regional councils, parses XML responses with a Python-based pipeline, updates a Snowflake database with new records, prepares tables with DBT for consumers, and visualizes the results in a Streamlit app. This platform allows data scientists to run new data pulls if additional data updates are required.

The goal is to provide analytical tools to present lake health information across New Zealand. Councils collect various environmental data to assess aquatic life quality in lakes, including:

- Chlorophyll a: Measures phytoplankton growth affecting ecological communities and water clarity.

- Total Phosphorus and Total Nitrogen: Nutrients that can lead to elevated plant and algae growth.

- Ammonia Toxicity: A nutrient that can be toxic to aquatic life.

This data is collected monthly across different dates and schedules. The project checks for new data records daily and uses Dagster to handle incremental updates to the RAW schema in Snowflake. DBT is used to summarize the dataset, generate fact and dimension tables, and prepare a large table for use in the Streamlit app. Dagster GraphQL will also be used to trigger jobs from the Streamlit app.

The final product will enable users to visualize their data, view diagnostic tables comparing data pull statuses, and submit new data pulls if needed. The final streamlit website is hosted [here](https://project3-de-master.azurewebsites.net/).

## Data Consumers
Data Scientists: Users who will analyze and visualize the data.

## Datasets
Council's API Endpoints: Access to environmental data from lake sites.

## Solution Architecture
![alt text](images/diagram.png)

- Council's API Endpoints:

  - Custom connection.
  - Scheduled API requests run daily.
  - Data is saved to the RAW schema in Snowflake.

- Snowflake:

  - RAW_STAGING: Incremental table with environmental data and lake sites.
  - RAW_MARTS: Includes a basic fact table (records), dimensions (errors, sites, variables), summary tables (min, max, mean, date_of_last_record), and an accumulative fact table with the number of successful data pulls per day.

- Streamlit: Visualizes results and provides diagnostic tables for data pull status.

## Run Dagster pipeline locally

1. Create new virtual environment and activate it

`py -3.11 -m venv .venv`
`source .venv/Scripts/activate`

1. Install deps

`pip install -r requirements.txt`

1. Run dagster

`dagster dev`

1. Access Dagster Web UI:

Open http://127.0.0.1:3000 in your browser.

DBT is integrated into the pipeline and will appear as assets in Dagster.

## Run Streamlit app locally

1. Activate venv

`source .venv/Scripts/activate`

1. Install deps

`pip install -r data_analysis/requirements.txt`

1. Run app

`cd data_analysis`
`streamlit run app.py`

1. Access Streamlit Web UI:

Open http://localhost:8501 in your browser.



