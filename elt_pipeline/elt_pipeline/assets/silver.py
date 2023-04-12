from dagster import asset, AssetIn, Output
import polars as pl
import os

from ..resources.spark_io_manager import get_spark_session


@asset(
    description="Load book table from bronze layer in minIO, into a Spark dataframe, then clean data",
    ins={
        "bronze_book": AssetIn(
            key_prefix=["bronze", "goodreads"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "goodreads"],
    compute_kind="PySpark",
    group_name="silver",
)
def silver_cleaned_book(context, bronze_book: pl.DataFrame):
    """
    Load book table from bronze layer in minIO, into a Spark dataframe, then clean data
    """

    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    context.log.debug("Start creating spark session")
    # Convert bronze_book from polars DataFrame to Spark DataFrame
    with get_spark_session(config) as spark:
        bronze_book = bronze_book[:100]
        pandas_df = bronze_book.to_pandas()
        context.log.debug(f"Got pandas DataFrame with shape: {pandas_df.shape}")

        context.log.debug("Converting bronze_book to Spark DataFrame...")
        spark_df = spark.createDataFrame(pandas_df)

        spark_df.cache()

        return Output(
            value=spark_df,
            metadata={
                "table": "silver_cleaned_book",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )


@asset(
    description="Load genre table from bronze layer in minIO, into a Spark dataframe, then clean data",
    ins={
        "bronze_genre": AssetIn(
            key_prefix=["bronze", "goodreads"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "goodreads"],
    compute_kind="PySpark",
    group_name="silver",
)
def silver_cleaned_genre(context, bronze_genre: pl.DataFrame):
    """
    Load genre table from bronze layer in minIO, into a Spark dataframe, then clean data
    """

    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    context.log.debug("Start creating spark session")
    # Convert bronze_genre from polars DataFrame to Spark DataFrame
    with get_spark_session(config) as spark:
        pandas_df = bronze_genre.to_pandas()
        context.log.debug(f"Got pandas DataFrame with shape: {pandas_df.shape}")

        context.log.debug("Converting bronze_genre to Spark DataFrame...")
        spark_df = spark.createDataFrame(pandas_df)

        spark_df.cache()

        return Output(
            value=spark_df,
            metadata={
                "table": "silver_cleaned_genre",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )
