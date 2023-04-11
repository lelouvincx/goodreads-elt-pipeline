from dagster import IOManager, InputContext, OutputContext
from contextlib import contextmanager
from sqlalchemy import create_engine
import polars as pl


def connect_mysql(config) -> str:
    conn_info = (
        f"mysql://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )
    return conn_info


class MySQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def handle_output(self, context: "OutputContext", obj: pl.DataFrame):
        pass

    def load_input(self, context: "InputContext"):
        pass

    def extract_data(self, sql: str) -> pl.DataFrame:
        """
        Extract data from MySQL database as polars DataFrame
        """
        conn_info = connect_mysql(self._config)
        df_data = pl.read_database(query=sql, connection_uri=conn_info)
        return df_data
