from dagster import Definitions
from analytics.jobs import get_lake_sites, run_etl_all_councils
from analytics.schedules import environ_data_etl_schedule
from analytics.resources import snowflake_resource
from analytics.assets.dbt.dbt import dbt_warehouse, dbt_warehouse_resource

defs = Definitions(
    jobs=[get_lake_sites, run_etl_all_councils],
    resources={
        "snowflake_resource": snowflake_resource,
        "dbt_warehouse_resource": dbt_warehouse_resource  # Add the dbt resource

    },
    assets=[dbt_warehouse],  # Add the dbt assets
    schedules=[environ_data_etl_schedule]  # Add the schedule(s)

)