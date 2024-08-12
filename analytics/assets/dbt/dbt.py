import os
from pathlib import Path
from dagster_dbt import DbtCliResource, dbt_assets
from dagster import AssetExecutionContext

# Corrected path to point to the correct dbt project directory
dbt_project_dir = Path(__file__).joinpath("..", "..", "..", "..", "warehouse_snowflake").resolve()
print(f"dbt_project_dir: {dbt_project_dir}")
dbt_warehouse_resource = DbtCliResource(project_dir=os.fspath(dbt_project_dir))

# generate manifest
dbt_manifest_path = (
    dbt_warehouse_resource.cli(
        ["--quiet", "parse"],
        target_path=Path("target"),
    )
    .wait()
    .target_path.joinpath("manifest.json")
)

print(f"dbt_manifest_path: {dbt_manifest_path}")

# Check if the manifest.json file exists and raise an error if not
if not dbt_manifest_path.exists():
    raise FileNotFoundError(f"manifest.json does not exist at {dbt_manifest_path}. Please check your dbt run.")

# load manifest to produce asset defintion
@dbt_assets(manifest=dbt_manifest_path)
def dbt_warehouse(context: AssetExecutionContext, dbt_warehouse_resource: DbtCliResource):
    yield from dbt_warehouse_resource.cli(["run"], context=context).stream()