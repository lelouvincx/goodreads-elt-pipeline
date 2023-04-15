from dagster import load_assets_from_modules, file_relative_path
from dagster_dbt import load_assets_from_dbt_project

from . import bronze, silver, gold, warehouse


bronze_layer_assets = load_assets_from_modules([bronze])
silver_layer_assets = load_assets_from_modules([silver])
gold_layer_assets = load_assets_from_modules([gold])
warehouse_assets = load_assets_from_modules([warehouse])

DBT_PROJECT_PATH = file_relative_path(__file__, "../../dbt_transform")
DBT_PROFILES = file_relative_path(__file__, "../../dbt_transform/config")

dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_PATH,
    profiles_dir=DBT_PROFILES,
    key_prefix=["dbt"],
)
