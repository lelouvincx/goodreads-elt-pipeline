from dagster import IOManager, InputContext, OutputContext
import polars as pl
from datetime import datetime


def connect_psql(config) -> str:
    conn_info = (
        f"postgresql://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )
    return conn_info


class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def handle_output(self, context: "OutputContext", obj: pl.DataFrame):
        schema, table = context.asset_key.path[-2], context.asset_key.path[-1]
        tmp_tbl = f"{table}_{datetime.today().strftime('%Y%m%d%H%M%S')}"

    def load_input(self, context: "InputContext"):
        pass
