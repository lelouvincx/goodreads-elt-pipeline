from dagster import (
    asset,
    AssetIn,
    Output,
    StaticPartitionsDefinition,
)
from pyspark.sql import DataFrame
from datetime import datetime
import pyarrow as pa
import polars as pl


COMPUTE_KIND = "Postgres"
LAYER = "warehouse"
YEARLY = StaticPartitionsDefinition(
    [str(year) for year in range(1975, datetime.today().year)]
)


# Asset warehouse_book_with_info
@asset(
    description="Load book_with_info data from spark to postgres",
    ins={
        "gold_book_with_info": AssetIn(
            key_prefix=["gold", "goodreads"],
        ),
    },
    metadata={
        "primary_keys": ["isbn"],
        "columns": ["isbn", "name", "authors", "language", "pagesnumber"],
    },
    io_manager_key="psql_io_manager",
    key_prefix=["goodreads", "gold"],  # Database: goodreads, Schema: gold
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def warehouse_book_with_info(context, gold_book_with_info: DataFrame):
    """
    Load book_with_info data from spark to postgres
    """

    context.log.info("Got spark DataFrame, loading to postgres")
    # Convert from spark DataFrame to polars DataFrame
    df = pl.from_arrow(pa.Table.from_batches(gold_book_with_info._collect_as_arrow()))
    context.log.debug(f"Got polars DataFrame with shape: {df.shape}")

    return Output(
        value=df,
        metadata={
            "database": "goodreads",
            "schema": "gold",
            "table": "book_with_info",
            "primary_keys": ["isbn"],
            "columns": ["isbn", "name", "authors", "language", "pagesnumber"],
        },
    )


# Asset warehouse_book_with_publish
@asset(
    description="Load book_with_publish data from spark to postgres",
    ins={
        "gold_book_with_publish": AssetIn(
            key_prefix=["gold", "goodreads"],
        ),
    },
    metadata={
        "primary_keys": ["isbn"],
        "columns": ["isbn", "publisher", "publishyear", "publishmonth", "publishday"],
    },
    io_manager_key="psql_io_manager",
    key_prefix=["goodreads", "gold"],  # Database: goodreads, Schema: gold
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def warehouse_book_with_publish(context, gold_book_with_publish: DataFrame):
    """
    Load book_with_publish data from spark to postgres
    """

    context.log.info("Got spark DataFrame, loading to postgres")
    # Convert from spark DataFrame to polars DataFrame
    df = pl.from_arrow(
        pa.Table.from_batches(gold_book_with_publish._collect_as_arrow())
    )
    context.log.debug(f"Got polars DataFrame with shape: {df.shape}")

    return Output(
        value=df,
        metadata={
            "database": "goodreads",
            "schema": "gold",
            "table": "book_with_publish",
            "primary_keys": ["isbn"],
            "columns": [
                "isbn",
                "publisher",
                "publishyear",
                "publishmonth",
                "publishday",
            ],
        },
    )


# Asset warehouse_book_with_rating
@asset(
    description="Load book_with_rating data from spark to postgres",
    ins={
        "gold_book_with_rating": AssetIn(
            key_prefix=["gold", "goodreads"],
        ),
    },
    metadata={
        "primary_keys": ["isbn"],
        "columns": [
            "isbn",
            "rating",
            "ratingdist5",
            "ratingdist4",
            "ratingdist3",
            "ratingdist2",
            "ratingdist1",
            "ratingdisttotal",
            "countoftextreviews",
        ],
    },
    io_manager_key="psql_io_manager",
    key_prefix=["goodreads", "gold"],  # Database: goodreads, Schema: gold
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def warehouse_book_with_rating(context, gold_book_with_rating: DataFrame):
    """
    Load book_with_rating data from spark to postgres
    """

    context.log.info("Got spark DataFrame, loading to postgres")
    # Convert from spark DataFrame to polars DataFrame
    df = pl.from_arrow(pa.Table.from_batches(gold_book_with_rating._collect_as_arrow()))
    context.log.debug(f"Got polars DataFrame with shape: {df.shape}")

    return Output(
        value=df,
        metadata={
            "database": "goodreads",
            "schema": "gold",
            "table": "book_with_rating",
            "primary_keys": ["isbn"],
            "columns": [
                "isbn",
                "rating",
                "ratingdist5",
                "ratingdist4",
                "ratingdist3",
                "ratingdist2",
                "ratingdist1",
                "ratingdisttotal",
                "countoftextreviews",
            ],
        },
    )


# Asset warehouse_book_download_link
@asset(
    description="Load book_download_link data from minio to postgres",
    ins={
        "bronze_book_download_link": AssetIn(
            key_prefix=["bronze", "goodreads"],
        ),
    },
    metadata={
        "primary_keys": ["isbn"],
        "columns": ["isbn", "link"],
    },
    io_manager_key="psql_io_manager",
    key_prefix=[
        "goodreads",
        "recommendations",
    ],  # Database: goodreads, Schema: recommendations
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def warehouse_book_download_link(context, bronze_book_download_link: pl.DataFrame):
    """
    Load book_download_link data from minio to postgres
    """

    df = bronze_book_download_link
    # Rename column BookISBN to isbn
    df = df.rename({"BookISBN": "isbn"})
    context.log.info(f"Columns: {df.columns}")

    return Output(
        value=df,
        metadata={
            "database": "goodreads",
            "schema": "recommendations",
            "table": "book_download_link",
            "primary_keys": ["isbn"],
            "columns": ["isbn", "link"],
        },
    )
