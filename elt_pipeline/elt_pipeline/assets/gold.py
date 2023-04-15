from dagster import (
    asset,
    multi_asset,
    AssetIn,
    AssetOut,
    Output,
    StaticPartitionsDefinition,
)
from pyspark.sql import DataFrame
from datetime import datetime
import pyarrow as pa
import polars as pl


COMPUTE_KIND = "Python"
YEARLY = StaticPartitionsDefinition(
    [str(year) for year in range(1975, datetime.today().year)]
)


# genre to gold (minIO) and warehouse (postgres)
@multi_asset(
    ins={
        "silver_collected_genre": AssetIn(
            key_prefix=["silver", "goodreads"],
        )
    },
    outs={
        "gold_genre": AssetOut(
            description="Load genre data from spark to minIO",
            io_manager_key="spark_io_manager",
            key_prefix=["gold", "goodreads"],
            group_name="gold",
        ),
        "warehouse_genre": AssetOut(
            description="Load genre data from spark to postgres",
            io_manager_key="psql_io_manager",
            key_prefix=["goodreads", "gold"],  # Database: goodreads, Schema: gold
            metadata={
                "primary_keys": ["id", "name"],
                "columns": ["id", "name"],
            },
            group_name="warehouse",
        ),
    },
    compute_kind=COMPUTE_KIND,
)
def genre(context, silver_collected_genre: DataFrame):
    """
    Load genre data from spark to minIO and postgres
    """

    spark_df = silver_collected_genre

    context.log.info("Got spark DataFrame, converting to polars DataFrame")
    # Convert from spark DataFrame to polars DataFrame
    df = pl.from_arrow(
        pa.Table.from_batches(silver_collected_genre._collect_as_arrow())
    )
    context.log.debug(f"Got polars DataFrame with shape: {df.shape}")

    return Output(
        value=spark_df,
        metadata={
            "table": "gold_genre",
            "row_count": spark_df.count(),
            "column_count": len(spark_df.columns),
            "columns": spark_df.columns,
        },
    ), Output(
        value=df,
        metadata={
            "database": "goodreads",
            "schema": "gold",
            "table": "genre",
            "primary_keys": ["id", "name"],
            "columns": ["id", "name"],
        },
    )


# book_genre to gold (minIO) and warehouse (postgres)
@multi_asset(
    ins={
        "silver_collected_book_genre": AssetIn(
            key_prefix=["silver", "goodreads"],
        )
    },
    outs={
        "gold_book_genre": AssetOut(
            description="Load book_genre data from spark to minIO",
            io_manager_key="spark_io_manager",
            key_prefix=["gold", "goodreads"],
            group_name="gold",
        ),
        "warehouse_book_genre": AssetOut(
            description="Load book_genre data from spark to postgres",
            io_manager_key="psql_io_manager",
            key_prefix=["goodreads", "gold"],  # Database: goodreads, Schema: gold
            metadata={
                "primary_keys": ["bookisbn", "genreid"],
                "columns": ["bookisbn", "genreid"],
            },
            group_name="warehouse",
        ),
    },
    compute_kind=COMPUTE_KIND,
)
def book_genre(context, silver_collected_book_genre: DataFrame):
    """
    Load book_genre data from spark to minIO and postgres
    """

    spark_df = silver_collected_book_genre

    context.log.info("Got spark DataFrame, converting to polars DataFrame")
    # Convert from spark DataFrame to polars DataFrame
    df = pl.from_arrow(
        pa.Table.from_batches(silver_collected_book_genre._collect_as_arrow())
    )
    context.log.debug(f"Got polars DataFrame with shape: {df.shape}")

    return Output(
        value=spark_df,
        metadata={
            "table": "gold_book_genre",
            "row_count": spark_df.count(),
            "column_count": len(spark_df.columns),
            "columns": spark_df.columns,
        },
    ), Output(
        value=df,
        metadata={
            "database": "goodreads",
            "schema": "gold",
            "table": "book_genre",
            "record_count": df.shape[0],
        },
    )


# Asset book_with_info
@asset(
    description="Split book table to get basic info",
    # partitions_def=YEARLY,
    ins={
        "silver_collected_book": AssetIn(
            key_prefix=["silver", "goodreads"],
            metadata={"full_load": True},
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["gold", "goodreads"],
    compute_kind="PySpark",
    group_name="gold",
)
def gold_book_with_info(context, silver_collected_book: DataFrame):
    """
    Split book table to get basic info
    """

    spark_df = silver_collected_book
    context.log.info("Got spark DataFrame, getting neccessary columns")

    # Drop rows with null value in Language column
    spark_df = spark_df.dropna(subset=["Language"])

    # Select columns ISBN, Name, Authors, Language, Description, PagesNumber
    spark_df = spark_df.select(
        "ISBN",
        "Name",
        "Authors",
        "Language",
        "PagesNumber",
    )
    spark_df.collect()

    return Output(
        value=spark_df,
        metadata={
            "table": "gold_book_with_info",
            "row_count": spark_df.count(),
            "column_count": len(spark_df.columns),
            "columns": spark_df.columns,
        },
    )


# Asset book_with_publish
@asset(
    description="Split book table to get publishing info",
    ins={
        "silver_collected_book": AssetIn(
            key_prefix=["silver", "goodreads"],
            metadata={"full_load": True},
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["gold", "goodreads"],
    compute_kind="PySpark",
    group_name="gold",
)
def gold_book_with_publish(context, silver_collected_book: DataFrame):
    """
    Split book table to get publishing info
    """

    spark_df = silver_collected_book
    context.log.info("Got spark DataFrame, getting neccessary columns")

    # Drop rows with null value in Language column
    spark_df = spark_df.dropna(subset=["Language"])

    # Select columns ISBN, Publisher, PublishYear, PublishMonth, PublishDay
    spark_df = spark_df.select(
        "ISBN",
        "Publisher",
        "PublishYear",
        "PublishMonth",
        "PublishDay",
    )
    spark_df.collect()

    return Output(
        value=spark_df,
        metadata={
            "table": "gold_book_with_publish",
            "row_count": spark_df.count(),
            "column_count": len(spark_df.columns),
            "columns": spark_df.columns,
        },
    )


# Asset book_with_rating
@asset(
    description="Split book table to get rating info",
    ins={
        "silver_collected_book": AssetIn(
            key_prefix=["silver", "goodreads"],
            metadata={"full_load": True},
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["gold", "goodreads"],
    compute_kind="PySpark",
    group_name="gold",
)
def gold_book_with_rating(context, silver_collected_book: DataFrame):
    """
    Split book table to get rating info
    """

    spark_df = silver_collected_book
    context.log.info("Got spark DataFrame, getting neccessary columns")

    # Drop rows with null value in Language column
    spark_df = spark_df.dropna(subset=["Language"])

    # Select columns ISBN, Rating, RatingDist1, RatingDist2, RatingDist3, RatingDist4, RatingDist5, CountOfTextReviews
    spark_df = spark_df.select(
        "ISBN",
        "Rating",
        "RatingDist5",
        "RatingDist4",
        "RatingDist3",
        "RatingDist2",
        "RatingDist1",
        "RatingDistTotal",
        "CountOfTextReviews",
    )
    spark_df.collect()

    return Output(
        value=spark_df,
        metadata={
            "table": "gold_book_with_rating",
            "row_count": spark_df.count(),
            "column_count": len(spark_df.columns),
            "columns": spark_df.columns,
        },
    )
