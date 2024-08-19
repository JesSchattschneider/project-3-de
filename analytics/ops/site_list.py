import pandas as pd
from dagster import OpExecutionContext, op, Config
import requests
import datetime
import numpy as np
from analytics.ops import parse_wfs_data, load_data_to_snowflake

# Define columns to be used in the WFS data
VARS = ["councilsiteid", "siteid", "lawasiteid",
        "lfenzid", "ltype", "geomorphicltype",
        "region", "agency", "catchment", "lwquality", "macro", "swquality",
        "latitude", "longitude", "council", "url", "status", "partition_key"]

class SitesDataConfig(Config):
    councils: list[str]
    modules: list[str]
    vars: list[str] = VARS

@op(required_resource_keys={"snowflake_resource"})
def process_wfs_data(context: OpExecutionContext,
                     config: SitesDataConfig
                     ) -> pd.DataFrame:
    """Processes WFS data for the given council partition."""

    # Read configs
    councils = config.councils
    modules = config.modules
    vars = config.vars

    context.log.info("Opening file with URLs")

    try:
        snowflake_resource_con = context.resources.snowflake_resource
        with snowflake_resource_con.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT agency as council, wfs from list_of_urls")
                # Fetch all rows from the executed query
                rows = cursor.fetchall()
                
                # Get the column names from the cursor description
                column_names = [desc[0] for desc in cursor.description]
                
                # Create a DataFrame from the fetched data
                councils_wfs = pd.DataFrame(rows, columns=column_names)
                # all columns are converted to lowercase
                councils_wfs.columns = map(str.lower, councils_wfs.columns)
                
    except Exception as e:
        context.log.error(f"Error getting sites from snowflake: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error

    context.log.info("Filtering data based on partition")

    # keep only the rows that are in the councils list
    councils_wfs = councils_wfs[councils_wfs["council"].str.lower().isin(councils)]
    
    # for each row in the dataframe, loop through the rows and get sites
    results = []
    for index, row in councils_wfs.iterrows():
        council_str=str(row["council"])
        context.log.info(f"Getting WFS data for: {council_str}")

        response = requests.get(url=row["wfs"])
        data = {
            "council": council_str,
            "url": row["wfs"],
            "status": "success" if response.status_code == 200 else "failed",
            "partition_key": council_str,
            "creation_date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "raw_data": response.content if response.status_code == 200 else response.text
        }
        results.append(data)
        # delete data and council_str
        del data, council_str

    context.log.info("Parsing WFS data")
    
    df = pd.concat([parse_wfs_data(result) for result in results if result['status'] == 'success'], ignore_index=True)

    context.log.info("Wrangling WFS data")
    df.columns = map(str.lower, df.columns)
    df = df[df.columns.intersection(vars)]

    for var in vars:
        if var not in df.columns:
            context.log.info(f"Adding column for {var} to WFS dataframe")
            df[var] = np.nan

    data_raw = df.copy()

    context.log.info("Processing modules")
    for module in modules:
        context.log.info(f"Processing module: {module}")

        if module == "mac":
            var_module = "macro"
        elif module in ["lwq", "swq"]:
            var_module = f"{module}uality"
        else:
            context.log.error(f"Unknown module: {module}")
            continue

        if var_module not in df.columns:
            context.log.error(f"Column {var_module} not in the data")
            continue 

        # Filter data based on module
        df = data_raw[data_raw[var_module].str.lower().isin(["y", "yes", "true"])]
        df = df.drop_duplicates(subset=['siteid', 'councilsiteid', 'lawasiteid'], keep='first')

        if len(df) == 0:
            context.log.info(f"No sites found for {module} module {var_module}")
            continue
        else:
            # Replace np.nan with None
            context.log.info("Replace np.nan with None and add lawa_site and council column")
            df = df.replace({np.nan: None})
            df['lawa_site'] = "yes"

            context.log.info(f"Add to DB: Found sites for {module} module")
            snowflake_resource_con = context.resources.snowflake_resource
            load_data_to_snowflake(snowflake_resource_con = snowflake_resource_con, 
                                     df = df, 
                                     table_name =  f"{module}_wfs_table",
                                     logger = context.log)
    return df
