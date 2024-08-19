import os
from pathlib import Path
from dagster_dbt import DbtCliResource, dbt_assets
from dagster import AssetExecutionContext

# Corrected path to point to the correct dbt project directory
dbt_project_dir = Path(__file__).joinpath("..", "..", "..", "..", "warehouse_snowflake").resolve()
dbt_warehouse_resource = DbtCliResource(project_dir=os.fspath(dbt_project_dir))

# Run dbt deps
def install_dbt_deps_if_needed(dbt_resource: DbtCliResource):
    # Define the path to the dbt_packages directory
    dbt_packages_dir = Path(dbt_resource.project_dir).joinpath("dbt_packages")
    
    # Check if dbt_packages directory exists and contains any packages
    if not any(dbt_packages_dir.iterdir()):
        # No packages found, run dbt deps to install dependencies
        print("No dbt packages found. Installing dependencies...")
        dbt_resource.cli(["deps"]).wait()
    else:
        print("dbt packages already installed.")

# Run dbt deps if necessary
install_dbt_deps_if_needed(dbt_warehouse_resource)

# generate manifest
dbt_manifest_path = (
    dbt_warehouse_resource.cli(
        ["--quiet", "parse"],
        target_path=Path("target"),
    )
    .wait()
    .target_path.joinpath("manifest.json")
)

# Check if the manifest.json file exists and raise an error if not
if not dbt_manifest_path.exists():
    raise FileNotFoundError(f"manifest.json does not exist at {dbt_manifest_path}. Please check your dbt run.")

# load manifest to produce asset defintion
@dbt_assets(manifest=dbt_manifest_path)
def dbt_warehouse(context: AssetExecutionContext, dbt_warehouse_resource: DbtCliResource):
    yield from dbt_warehouse_resource.cli(["run"], context=context).stream()