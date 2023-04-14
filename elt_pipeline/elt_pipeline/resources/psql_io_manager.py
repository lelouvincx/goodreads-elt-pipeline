from dagster import IOManager, InputContext, OutputContext
from contextlib import contextmanager
import polars as pl
from datetime import datetime
import psycopg2
from psycopg2 import sql
import psycopg2.extras


@contextmanager
def connect_psql(config):
    try:
        yield psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"],
        )
    except (Exception) as e:
        print(f"Error while connecting to PostgreSQL: {e}")


class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def handle_output(self, context: "OutputContext", obj: pl.DataFrame):
        # E.g context.asset_key.path = ['warehouse', 'gold', 'book_genre']
        schema = context.asset_key.path[-2]
        # NOTE: Replace pattern is 'warehouse', not general
        table = str(context.asset_key.path[-1]).replace("warehouse_", "")
        context.log.debug(f"Schema: {schema}, Table: {table}")
        tmp_tbl = f"{table}_tmp_{datetime.now().strftime('%Y_%m_%d')}"
        try:
            with connect_psql(self._config) as conn:
                context.log.debug(f"Connected to PostgreSQL: {conn}")
                primary_keys = (context.metadata or {}).get("primary_keys", [])
                context.log.debug(f"Primary keys: {primary_keys}")

                with conn.cursor() as cursor:
                    context.log.debug(f"Cursor info: {cursor}")
                    cursor.execute("SELECT version()")
                    context.log.info(f"PostgreSQL version: {cursor.fetchone()}")
                    # Create temp file
                    cursor.execute(
                        f"CREATE TEMP TABLE IF NOT EXISTS {tmp_tbl} (LIKE {schema}.{table})"
                    )
                    cursor.execute(f"SELECT COUNT(*) FROM {tmp_tbl}")
                    context.log.debug(
                        f"Log for creating temp table: {cursor.fetchone()}"
                    )
                    # Create sql identifiers for the column names
                    # Do this to safely insert into a sql query
                    columns = sql.SQL(",").join(
                        sql.Identifier(name.lower()) for name in obj.columns
                    )
                    # Create a placeholder for the values. These will be filled later
                    values = sql.SQL(",").join(sql.Placeholder() for _ in obj.columns)
                    # Create the insert query
                    context.log.debug("Inserting data into temp table")
                    insert_query = sql.SQL("INSERT INTO {} ({}) VALUES({});").format(
                        sql.Identifier(tmp_tbl), columns, values
                    )
                    # Execute the insert query
                    psycopg2.extras.execute_batch(cursor, insert_query, obj.rows())
                    conn.commit()

                    # Check data inserted
                    context.log.debug("Checking data inserted")
                    cursor.execute(f"SELECT COUNT(*) FROM {tmp_tbl};")
                    context.log.info(f"Number of rows inserted: {cursor.fetchone()}")
                    # Upsert data
                    if len(primary_keys) > 0:
                        context.log.debug("Table has primary keys, upserting data")
                        conditions = " AND ".join(
                            [
                                f""" {schema}.{table}."{k}" = {tmp_tbl}."{k}" """
                                for k in primary_keys
                            ]
                        )
                        command = f"""
                            BEGIN TRANSACTION;
                            DELETE FROM {schema}.{table}
                            USING {tmp_tbl}
                            WHERE {conditions};

                            INSERT INTO {schema}.{table}
                            SELECT * FROM {tmp_tbl};

                            END TRANSACTION;
                        """
                    else:
                        context.log.debug("Table has no primary keys, replacing data")
                        command = f"""
                            BEGIN TRANSACTION;
                            DELETE FROM {schema}.{table};

                            INSERT INTO {schema}.{table}
                            SELECT * FROM {tmp_tbl};

                            END TRANSACTION;
                        """

                    # context.log.debug(f"Command: {command}")
                    context.log.debug(f"Upserting data into {schema}.{table}")
                    cursor.execute(command)
                    context.log.debug(f"{cursor.statusmessage}")
                    conn.commit()
        except (Exception) as e:
            print(f"Error while handling output to PostgreSQL: {e}")

        try:
            with connect_psql(self._config) as conn:
                with conn.cursor() as cursor:
                    context.log.debug(f"{cursor.fetchone()}")
                    cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table};")
                    context.log.info(
                        f"Number of rows upserted in {schema}.{table}: {cursor.fetchone()}"
                    )

                    # Drop temp table
                    cursor.execute(f"DROP TABLE {tmp_tbl}")
                    conn.commit()
        except (Exception) as e:
            print(f"Error while testing handle_output to PostgreSQL: {e}")

    def load_input(self, context: "InputContext"):
        pass
