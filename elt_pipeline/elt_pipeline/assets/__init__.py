from dagster import load_assets_from_modules

from . import bronze, silver, gold


bronze_layer_assets = load_assets_from_modules([bronze])
silver_layer_assets = load_assets_from_modules([silver])
gold_layer_assets = load_assets_from_modules([gold])
