import streamlit as st
import os
from dotenv import load_dotenv
from dagster_graphql import DagsterGraphQLClient
from gql.transport.requests import RequestsHTTPTransport
import snowflake.connector
import pandas as pd
import plotly.express as px
from streamlit_extras.metric_cards import style_metric_cards 


# Load environment variables
load_dotenv()

# Snowflake connection parameters
snowflake_user = os.environ.get('SNOWFLAKE_USER')
snowflake_password = os.environ.get('SNOWFLAKE_PASSWORD')
snowflake_account = os.environ.get('SNOWFLAKE_ACCOUNT')
snowflake_warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE')
snowflake_database = os.environ.get('SNOWFLAKE_DATABASE')
snowflake_schema = "MARTS_MARTS"
# os.environ.get('SNOWFLAKE_SCHEMA_MARTS')
snowflake_table = 'FACT_RECORDS'
snowflake_summary_table = 'FACT_SUMMARY'
root_url = os.environ.get('DAGSTER_URL')
user_token = os.environ.get('DAGSTER_API_KEY')
map_key = os.environ.get('PLOTLY_API_KEY') 

# Establish Snowflake connection
conn = snowflake.connector.connect(
    user=snowflake_user,
    password=snowflake_password,
    account=snowflake_account,
    warehouse=snowflake_warehouse,
    database=snowflake_database,
    schema=snowflake_schema
)


# Initialize session state if not already done
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False

# Credentials for the admin login
actual_email = "" #"admin@example.com"
actual_password = "" #"adminpassword"

# Initialize the URL for the GraphQL endpoint 
url = f"{root_url}/graphql"  # Ensure the URL points to the GraphQL endpoint

# Initialize the transport
transport = RequestsHTTPTransport(
    url=url,
    headers={"Dagster-Cloud-Api-Token": user_token},
    use_json=True,
)

# Initialize the DagsterGraphQLClient
client = DagsterGraphQLClient(hostname="dagster.prod",transport=transport)

# Function to trigger Dagster job
def update_wfs_dagster(council: str, module: str = ["lwq"]) -> str:
    try:
        # Trigger the Dagster job and get the run ID
        response = client.submit_job_execution(
            'update_wfs_job',
            repository_location_name="analytics",
            run_config={"ops": {"process_wfs_data": {"inputs": {"modules": module}}}},
            tags={"dagster/partition": "{council}"}
        )
        # Extract the run ID from the response
        new_run_id = response
        return new_run_id
    except Exception as exc:
        st.error(f"Error triggering Dagster job: {exc}")
        return None

with st.sidebar: #1st filter
    st.title("Filters")                                     
    COUNCIL = st.selectbox("Select your council", ("ecan", "gdc", "es")) #1st filter
    MODULE = st.selectbox("Select your module", ("lwq", "swq", "gw")) #2nd filter

if COUNCIL and MODULE:
    # Fetch summary data from FACT_RECORDS table based on filters  
    cursor = conn.cursor()
    query = f"""
        SELECT *
        FROM {snowflake_database}.PROJ3_RAW.lwq_wfs_table_latest
        WHERE COUNCIL = '{COUNCIL}'
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    
    sites = pd.DataFrame(rows, columns=columns)


tab1, tab2 = st.tabs(["Summary data and site list", "Admin - update data"])

with tab1:
    # select parameter, select box
    PARAMETER = st.selectbox("Select a parameter", ("NH4N", "CHLA", "TN", "TP", "ECOLI", 
                                                    "pH", "CYANOTOX", "CYANOTOT", "Secchi"),
                            index=None, 
                            placeholder = "Select a parameter") #3rd filter

    # wait for the user to select a parameter
    if PARAMETER:

        # Fetch summary data from FACT_RECORDS table based on filters  
        cursor = conn.cursor()
        query = f"""
            SELECT *
            FROM {snowflake_database}.{snowflake_schema}.{snowflake_summary_table}
            WHERE COUNCIL = '{COUNCIL}'
            AND VARIABLE = '{PARAMETER}'
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        data_summary = pd.DataFrame(rows, columns=columns)
        unique_sites_count = sites[['SITEID', 'LAWASITEID', 'COUNCILSITEID']].drop_duplicates().shape[0]
        unique_combinations_count = data_summary[['SITEID', 'LAWASITEID', 'COUNCILSITEID']].drop_duplicates().shape[0]
        
        col1, col2 = st.columns(2)
        col1.metric("Number of lake sites published", unique_sites_count)
        col2.metric(f"Number of lake sites with {PARAMETER} data", unique_combinations_count)
        style_metric_cards(border_radius_px = 3, background_color = "#f0f0f0")
        
        # latitude and longitude should be numeric
        data_summary['LATITUDE'] = pd.to_numeric(data_summary['LATITUDE'], errors='coerce')
        data_summary['LONGITUDE'] = pd.to_numeric(data_summary['LONGITUDE'], errors='coerce')

        px.set_mapbox_access_token(map_key)

        # remove rows with missing AVERAGE_VALUE
        data_summary_clean = data_summary.dropna(subset=['AVERAGE_VALUE'])
        data_summary_clean['AVERAGE_size'] = data_summary_clean['AVERAGE_VALUE'] * 10

        fig = px.scatter_mapbox(
            data_summary_clean,
            lat="LATITUDE",
            lon="LONGITUDE",
            hover_name="COUNCILSITEID",
            hover_data=["LAWASITEID", "SITEID"],
            color="AVERAGE_VALUE",
            size="AVERAGE_size",
            color_continuous_scale=px.colors.sequential.Viridis_r,
            zoom=3.5,
            center = {"lat": data_summary_clean["LATITUDE"].iloc[0], "lon": data_summary_clean["LONGITUDE"].iloc[0]},
            
        )
        fig.update_layout(mapbox_style="light", mapbox_accesstoken=map_key)        

        # Display the summary data
        st.header(f"{COUNCIL.upper()} - Summary data for {PARAMETER}") 
        st.write("The table below shows the summary data for the selected parameter.")
        st.write(data_summary)

        st.plotly_chart(fig)

        
        # Display the site list
        st.header(f"{COUNCIL.upper()} - Full site list published in the WFS")
        st.write(sites)

with tab2:
    # Create an empty container
   placeholder = st.empty()

   if st.session_state.logged_in:
        # If already logged in, show the protected content
        st.success("Login successful")
        
        # Show the content related to lake site list update
        st.header("Submit a lake site list update")
        st.write(
            "If you updated your lake sites, please submit a site update by clicking the Submit button below. "
            "This will trigger a Dagster job to fetch the latest data from the WFS endpoint."
        )

        if st.button("Submit"):
            run_id = update_wfs_dagster(COUNCIL)
            if run_id:
                st.success(f"Form submitted for {MODULE} module: {COUNCIL}. Job run ID: {run_id}")
            else:
                st.error("Failed to submit job.")

        # Show the content related to lake data update
        st.header("Submit a lake data update")
        st.write(
            "If historical data requires update, please submit an update request by clicking the Submit button below. "
            "This will trigger a Dagster job to fetch the latest data from the WFS endpoint."
        )

        # Date range selection
        start_date = st.date_input("Start date", value=None)
        end_date = st.date_input("End date", value=None)

        # Parameter and site selection
        param_selection = st.multiselect(
            "Select parameters that require update",
            ["NH4N", "CHLA", "TN", "TP", "ECOLI", "pH", "CYANOTOX", "CYANOTOT", "Secchi"]
        )
        sites_selection = st.multiselect(
            "Select sites that require update",
            ["SITEID", "LAWASITEID", "COUNCILSITEID"]
        )

        if start_date and end_date and param_selection and sites_selection:
            # Check the number of sites and parameters selected
            days_range = (end_date - start_date).days
            if days_range <= 30 and len(sites_selection) <= 2 and len(param_selection) <= 2:
                st.success("Your data update was accepted.")
            else:
                st.error(
                    "Your data update was not accepted. The error messages below will guide you on the requirements. "
                    "Please contact the admin for further assistance."
                )
                if len(sites_selection) > 2:
                    st.warning("Select less than 2 sites.")
                if len(param_selection) > 2:
                    st.warning("Select less than 2 parameters.")
                if days_range > 30:
                    st.warning("Date range should be less than 30 days.")

            st.write(f"Start date: {start_date}")
            st.write(f"End date: {end_date}")

   else:
        # Login form
        with placeholder.form("login"):
            st.markdown("#### Enter your credentials")
            st.markdown("This is a protected area. If you are an admin, please enter your credentials.")
            email = st.text_input("Email")
            password = st.text_input("Password", type="password")
            submit = st.form_submit_button("Login")

        if submit:
            if email == actual_email and password == actual_password:
                st.session_state.logged_in = True
                placeholder.empty()
                st.experimental_rerun()  # Rerun the app to reflect the logged-in state
            else:
                st.error("Login failed")

