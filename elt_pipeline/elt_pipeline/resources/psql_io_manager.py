from dagster import IOManager, InputContext, OutputContext
import polars as pl

from contextlib import contextmanager
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

    def handle_output(self, context: "OutputContext", obj: None):
        pass

    def load_input(self, context: "InputContext"):
        pass
