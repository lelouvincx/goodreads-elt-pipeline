from dagster import IOManager, OutputContext, InputContext
from minio import Minio
import polars as pl

from contextlib import contextmanager
from datetime import datetime
from typing import Union
import os


@contextmanager
def connect_minio(config):
    client = Minio(
        endpoint=config.get("endpoint_url"),
        access_key=config.get("minio_access_key"),
        secret_key=config.get("minio_secret_key"),
        secure=False,
    )

    try:
        yield client
    except Exception as e:
        raise e


# Make bucket if not exists
def make_bucket(client: Minio, bucket_name):
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
    else:
        print(f"Bucket {bucket_name} already exists.")


class MinIOIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def _get_path(self, context: Union[InputContext, OutputContext]):
        """
        Returns (key_name, tmp_file_path) where key_name is the path to the file in minIO
        and tmp_file_path is the path to the temp file in local disk, which will
        be uploaded to minIO and then deleted after the upload is done.
        """
        # E.g context.asset_key.path: ['bronze', 'goodreads', 'book']
        layer, schema, table = context.asset_key.path
        # NOTE: E.g: bronze/goodreads/book
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        # E.g /tmp/file_bronze_goodreads_book_20210101000000.parquet
        tmp_file_path = "/tmp/file_{}_{}.parquet".format(
            "_".join(context.asset_key.path), datetime.today().strftime("%Y%m%d%H%M%S")
        )  # Partition by year

        if context.has_partition_key:
            # E.g partition_str: book_2021
            partition_str = str(table) + "_" + context.asset_partition_key
            # E.g key_name: bronze/goodreads/book/book_2021.parquet
            # tmp_file_path: /tmp/file_bronze_goodreads_book_20210101000000.parquet
            return os.path.join(key, f"{partition_str}.parquet"), tmp_file_path
        else:
            # E.g key_name: bronze/goodreads/book.parquet
            return f"{key}.parquet", tmp_file_path

    def handle_output(self, context: "OutputContext", obj: pl.DataFrame):
        """
        Receives output from upstream asset,
        and converts to parquet format and upload to minIO.
        """

        key_name, tmp_file_path = self._get_path(context)

        # Convert from polars DataFrame to parquet format
        obj.write_parquet(tmp_file_path)

        # Upload file to minIO
        try:
            bucket_name = self._config.get("bucket")
            with connect_minio(self._config) as client:
                # Make bucket if not exist
                make_bucket(client, bucket_name)

                # Upload file to minIO
                # E.g bucket_name: lakehouse,
                # key_name: bronze/goodreads/book/book_2021.parquet,
                # tmp_file_path: /tmp/file_bronze_goodreads_book_20210101000000.parquet
                client.fput_object(bucket_name, key_name, tmp_file_path)
                context.log.info(
                    f"(MinIO handle_output) Number of rows and columns: {obj.shape}"
                )
                context.add_output_metadata({"path": key_name, "tmp": tmp_file_path})

                # Clean up tmp file
                os.remove(tmp_file_path)
        except Exception as e:
            raise e

    def load_input(self, context: "InputContext") -> pl.DataFrame:
        """
        Prepares input for downstream asset,
        and downloads parquet file from minIO and converts to polars DataFrame
        """

        bucket_name = self._config.get("bucket")
        key_name, tmp_file_path = self._get_path(context)

        try:
            with connect_minio(self._config) as client:
                # Make bucket if not exist
                make_bucket(client=client, bucket_name=bucket_name)

                # E.g bucket_name: lakehouse,
                # key_name: bronze/goodreads/book/book_2021.parquet,
                # tmp_file_path: /tmp/file_bronze_goodreads_book_20210101000000.parquet
                context.log.info(f"(MinIO load_input) from key_name: {key_name}")
                client.fget_object(bucket_name, key_name, tmp_file_path)
                df_data = pl.read_parquet(tmp_file_path)
                context.log.info(
                    f"(MinIO load_input) Got polars dataframe with shape: {df_data.shape}"
                )

                return df_data
        except Exception as e:
            raise e
