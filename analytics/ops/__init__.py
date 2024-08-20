from dagster import op
import xml.etree.ElementTree as ET
import pandas as pd
from typing import Optional, Any

import pandas as pd
import xml.etree.ElementTree as ET

def parse_wfs_data(data: dict) -> pd.DataFrame:
    """Parses the WFS data and extracts latitude and longitude."""
    root = ET.fromstring(data['raw_data'])
    records = []
    
    for feature in root.iter("{http://www.opengis.net/gml}featureMember"):
        record = {}
        # Extract lat and long from <gml:pos>
        pos_element = feature.find(".//{http://www.opengis.net/gml}pos")
        if pos_element is not None:
            # Split lat and long and add to record
            lat_long = pos_element.text.strip().split()
            if len(lat_long) == 2:
                record['latitude'] = lat_long[0]
                record['longitude'] = lat_long[1]
            else:
                record['latitude'] = None
                record['longitude'] = None
        
        # Extract other properties
        for property in feature.iter():
            if property.tag != feature.tag:
                property_name = property.tag.split("}")[1]
                property_value = property.text
                record[property_name] = property_value

        # Add additional columns
        record['council'] = data['council']
        record['url'] = data['url']
        record['status'] = data['status']
        record['partition_key'] = data['partition_key']

        records.append(record)
    
    # Create DataFrame
    df = pd.DataFrame(records)
    
    # Fill NaN values with -99999
    df = df.fillna(-99999)

    return df

def _create_table_if_not_exists(cursor, table_name, df, logger, datetime_columns=None):
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        table_exists = cursor.fetchone() is not None
        
        if not table_exists:
            if datetime_columns:
                # get all columns from the dataframe that are not datetime columns
                columns = df.columns.difference(datetime_columns)
                column_definitions = ", ".join([f"{col} STRING" for col in columns])  # Assuming all columns are strings
                column_definitions += ", created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"  # Add created_at column with default current timestamp
                for col in datetime_columns:
                    column_definitions += f", {col} TIMESTAMP"
            else:
                columns = df.columns
                column_definitions = ", ".join([f"{col} STRING" for col in columns])  # Assuming all columns are strings
                column_definitions += ", created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"  # Add created_at column with default current timestamp            
            create_stmt = f"CREATE TABLE {table_name} ({column_definitions})"
            cursor.execute(create_stmt)
            logger.info(f"Table {table_name} created.")

def _insert_data(cursor, table_name, data_to_insert):
        columns = data_to_insert[0].keys()
        insert_cols = ', '.join(columns)
        insert_vals = ', '.join([f'%({col})s' for col in columns])
        insert_stmt = f"""
        INSERT INTO {table_name} ({insert_cols})
        VALUES ({insert_vals})
        """
        for row in data_to_insert:
            cursor.execute(insert_stmt, row)

def _replace_symbols(value):
    if isinstance(value, str):
        # Replace "<" with "less_than_" and ">" with "greater_than_"
        if value.startswith('<'):
            return f"less_than_{value[1:]}"
        elif value.startswith('>'):
            return f"greater_than_{value[1:]}"
    return value

def _convert_dict_values_to_strings(data):
    for row in data:
        for key in row:
            row[key] = str(row[key])  # Convert each value to string
    return data

def _upsert_data(cursor, table_name, data_to_insert, primary_key):
    if not data_to_insert:
        return  # Exit if there's no data to insert

    # Replace symbols in the 'value' column and convert all values to string
    for row in data_to_insert:
        if 'value' in row:
            row['value'] = _replace_symbols(row['value'])
            row['value'] = str(row['value'])        

    data_to_insert = _convert_dict_values_to_strings(data_to_insert)

    # Extract columns from data
    columns = data_to_insert[0].keys()

    # Add missing columns to the table
    for column in columns:
        try:
            # Assuming default type VARCHAR(255) for simplicity; adjust as needed
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column} VARCHAR(255)")
        except Exception as e:
            continue
            # print(f"Error adding column {column} to table {table_name}: {e}")
    
    # Re-fetch columns to include any new ones added
    columns_list = ', '.join(columns)
    # Handle values and convert None to NULL
    values_list = ', '.join([
        f"({', '.join([f'NULL' if value is None else repr(value) for value in row.values()])})"
        for row in data_to_insert
    ])

    # Check if history table exists
    history_table_name = "LWQ_DATA_HISTORY"
    cursor.execute(f"SHOW TABLES LIKE '{history_table_name}'")
    table_exists = cursor.fetchone() is not None

    # Create the history table if it doesn't exist
    if not table_exists:
        cursor.execute(f"""
            CREATE TABLE {history_table_name} AS
            SELECT * FROM {table_name} LIMIT 0;
        """)
        cursor.execute(f"""
            ALTER TABLE {history_table_name}
            ADD COLUMN UPDATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP();
        """)
        print(f"History table {history_table_name} created.")

    if table_name == 'lwq_data':
        try:
           # Prefix all columns with `target.`
            qualified_columns_list = ', '.join([f'target.{col}' for col in columns])

            cursor.execute(f"""
                INSERT INTO {history_table_name} ({columns_list}, CREATED_AT)
                SELECT {qualified_columns_list}, target.CREATED_AT
                FROM {table_name} target
                JOIN (VALUES {values_list}) AS source ({columns_list})
                ON target.{primary_key} = source.{primary_key}
                WHERE target.VALUE != source.VALUE;
            """)
        except Exception as e:
            print(f"Error inserting into history table: {e}")
            raise

        # Step 2: Perform the upsert (update or insert)
        try:            
            cursor.execute(f"""
                MERGE INTO {table_name} AS target
                USING (VALUES {values_list}) AS source ({columns_list})
                ON target.{primary_key} = source.{primary_key}
                WHEN MATCHED THEN
                    UPDATE SET {', '.join([f'target.{col} = source.{col}' for col in columns])}
                WHEN NOT MATCHED THEN
                    INSERT ({columns_list})
                    VALUES ({', '.join([f'source.{col}' for col in columns])});
            """)
        except Exception as e:
            print(f"Error executing upsert: {e}")
            raise    
    else:      
        
        # Define the INSERT SQL statement with WHERE NOT EXISTS clause
        upsert_stmt = f"""
        INSERT INTO {table_name} ({columns_list})
        SELECT {', '.join([f'source.{col}' for col in columns])}
        FROM (VALUES {values_list}) AS source ({columns_list})
        WHERE NOT EXISTS (
            SELECT 1
            FROM {table_name} target
            WHERE target.{primary_key} = source.{primary_key}
        );
        """
        try:
            # Execute the INSERT statement
            cursor.execute(upsert_stmt)
        except Exception as e:
            print(f"Error executing upsert: {e}")


def _insert_data_snowflake(snowflake_resource_con: Any, df: pd.DataFrame, table_name: str, logger: Any) -> None:
    """Insert data into a Snowflake table, creating the table if it does not exist.

    Args:
        snowflake_resource_con (Any): The Snowflake connection resource.
        df (pd.DataFrame): The DataFrame containing data to insert.
        table_name (str): The name of the target table.
        logger (Any): Logger for logging information and errors.
    """
    
    # select unique councils from the dataframe, it should be a string
    council = df['council'].unique()[0].lower()

    try:
            with snowflake_resource_con.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Insert into main table
                    _create_table_if_not_exists(cursor, table_name, df, logger)
                    
                    data_to_insert = df.to_dict(orient='records')
                    _insert_data(cursor, table_name, data_to_insert)
                    
                    # Insert into latest table
                    latest_table_name = f"{table_name}_latest"
                    _create_table_if_not_exists(cursor, latest_table_name, df, logger)
                    print("here")

                    # Try to delete existing rows for the council in the latest table
                    try:
                        delete_stmt = f"DELETE FROM {latest_table_name} WHERE council = %s"
                        cursor.execute(delete_stmt, (council,))
                    except Exception as e:
                        print(f"Error deleting rows from latest table: {e}")
                    
                    # Insert new rows into the latest table
                    _insert_data(cursor, latest_table_name, data_to_insert)
                    
                    logger.info("Data successfully inserted into the database and latest table.")
        
    except Exception as e:
        logger.error(f"Failed to append to database: {e}")

def load_data_to_snowflake(snowflake_resource_con: Any, 
                             df: pd.DataFrame, 
                             table_name: str,
                             logger: Any,
                             method: str = "insert"
                             ) -> None:
    """Insert data into a Snowflake table, creating the table if it does not exist.

    Args:
        snowflake_resource_con (Any): The Snowflake connection resource.
        df (pd.DataFrame): The DataFrame containing data to insert.
        table_name (str): The name of the target table.
        logger (Any): Logger for logging information and errors.
    """
    
    if df.empty:
        logger.info("No data to insert.")
        return
    
    if method == "insert":
        logger.info(f"Inserting data into the database: {table_name} table.")
        _insert_data_snowflake(snowflake_resource_con, df, table_name, logger)
    
    if method == "upsert":
        logger.info(f"Upserting data into the database: {table_name} table.")

        try:
            with snowflake_resource_con.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Insert into main table
                    _create_table_if_not_exists(cursor, table_name, df, logger)
                                        
                    data_to_upsert = df.to_dict(orient='records')
                    _upsert_data(cursor, table_name, data_to_upsert, primary_key = 'id')
                    
                    logger.info("Data successfully inserted into the database and latest table.")
        
        except Exception as e:
            logger.error(f"Failed to upsert to database: {e}")
