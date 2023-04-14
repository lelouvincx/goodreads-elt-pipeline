from dagster import asset, AssetIn, Output, StaticPartitionsDefinition
from datetime import datetime
import polars as pl
import requests
import os

from pyspark.sql import DataFrame

from ..resources.spark_io_manager import get_spark_session
from pyspark.sql.functions import udf, col, regexp_replace, lower, when


COMPUTE_KIND = "PySpark"
LAYER = "silver"
YEARLY = StaticPartitionsDefinition(
    [str(year) for year in range(1975, datetime.today().year)]
)


@udf
def split_take_second(value):
    return value.split(":")[1]


# Silver cleaned book
@asset(
    description="Load book table from bronze layer in minIO, into a Spark dataframe, then clean data",
    partitions_def=YEARLY,
    ins={
        "bronze_book": AssetIn(
            key_prefix=["bronze", "goodreads"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "goodreads"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
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

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        # Convert bronze_book from polars DataFrame to Spark DataFrame
        pandas_df = bronze_book.to_pandas()
        context.log.debug(
            f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
        )

        spark_df = spark.createDataFrame(pandas_df)
        spark_df.cache()
        context.log.info("Got Spark DataFrame")

        # Dedupe books
        spark_df = spark_df.dropDuplicates()
        # Drop rows with null value in column 'Name'
        spark_df = spark_df.na.drop(subset=["Name"])
        # Drop rows with null values in column ISBN
        spark_df = spark_df.na.drop(subset=["isbn"])
        # Drop rows will null values in column 'Language'
        spark_df = spark_df.na.drop(subset=["Language"])
        # Drop rows with value '--' in column 'Language'
        spark_df = spark_df.filter(spark_df.Language != "--")
        # Drop rows with value > 350 in column 'PagesNumber'
        spark_df = spark_df.filter(spark_df.PagesNumber <= 350)
        # Drop column 'CountsOfReview' (overlap with 'RatingDistTotal')
        spark_df = spark_df.drop("CountsOfReview")
        # Choose rows with 'PublishYear' from 1900 to datetime.today().year
        spark_df = spark_df.filter(
            (spark_df.PublishYear >= 1975)
            & (spark_df.PublishYear <= datetime.today().year)
        )
        # Update value of column 'RatingDist...', splitting by ':' and take the second value
        spark_df = spark_df.withColumn(
            "RatingDist5", split_take_second(col("RatingDist5"))
        )
        spark_df = spark_df.withColumn(
            "RatingDist4", split_take_second(col("RatingDist4"))
        )
        spark_df = spark_df.withColumn(
            "RatingDist3", split_take_second(col("RatingDist3"))
        )
        spark_df = spark_df.withColumn(
            "RatingDist2", split_take_second(col("RatingDist2"))
        )
        spark_df = spark_df.withColumn(
            "RatingDist1", split_take_second(col("RatingDist1"))
        )
        spark_df = spark_df.withColumn(
            "RatingDistTotal", split_take_second(col("RatingDistTotal"))
        )
        # Cast column 'RatingDist...' to Interger
        spark_df = spark_df.withColumn(
            "RatingDist5", spark_df.RatingDist5.cast("Integer")
        )
        spark_df = spark_df.withColumn(
            "RatingDist4", spark_df.RatingDist4.cast("Integer")
        )
        spark_df = spark_df.withColumn(
            "RatingDist3", spark_df.RatingDist3.cast("Integer")
        )
        spark_df = spark_df.withColumn(
            "RatingDist2", spark_df.RatingDist2.cast("Integer")
        )
        spark_df = spark_df.withColumn(
            "RatingDist1", spark_df.RatingDist1.cast("Integer")
        )
        spark_df = spark_df.withColumn(
            "RatingDistTotal", spark_df.RatingDistTotal.cast("Integer")
        )
        # Change column name 'Count of text reviews' to 'CountOfTextReviews'
        spark_df = spark_df.withColumnRenamed(
            "Count of text reviews", "CountOfTextReviews"
        )
        # Change value of column 'Language' from ['en-US', 'en-GB', 'en-CA'],  to 'eng', from 'nl' to 'nld'
        spark_df = spark_df.withColumn(
            "Language", regexp_replace("Language", "en-US", "eng")
        )
        spark_df = spark_df.withColumn(
            "Language", regexp_replace("Language", "en-GB", "eng")
        )
        spark_df = spark_df.withColumn(
            "Language", regexp_replace("Language", "en-CA", "eng")
        )
        spark_df = spark_df.withColumn(
            "Language", regexp_replace("Language", "nl", "nld")
        )

        spark_df.unpersist()

        return Output(
            value=spark_df,
            metadata={
                "table": "silver_cleaned_book",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )


# Silver cleaned genre
@asset(
    description="Load genre table from bronze layer in minIO, into a Spark dataframe, then clean data",
    ins={
        "bronze_genre": AssetIn(
            key_prefix=["bronze", "goodreads"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "goodreads"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
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

    with get_spark_session(config) as spark:
        pandas_df = bronze_genre.to_pandas()
        context.log.debug(f"Got pandas DataFrame with shape: {pandas_df.shape}")

        spark_df = spark.createDataFrame(pandas_df)
        spark_df.cache()
        context.log.info("Got Spark DataFrame")

        # Downcase the column 'Name'
        spark_df = spark_df.withColumn("Name", lower(col("Name")))

        return Output(
            value=spark_df,
            metadata={
                "table": "silver_cleaned_genre",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )


# Silver collected book
@asset(
    description="Collect more infomation about cleaned books, such as authors, number of pages",
    partitions_def=YEARLY,
    ins={
        "silver_cleaned_book": AssetIn(
            key_prefix=["silver", "goodreads"],
            metadata={"full_load": False},
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "goodreads"],
    compute_kind="OpenLibrary API",
    group_name=LAYER,
)
def silver_collected_book(context, silver_cleaned_book: DataFrame) -> Output[DataFrame]:
    """
    Collect more infomation about cleaned books
    - Authors: if missing
    - Number of pages: if missing
    """

    spark_df = silver_cleaned_book
    context.log.debug("Caching spark_df ...")
    spark_df.cache()

    context.log.info("Starting filling missing data ...")
    null_authors_df = spark_df.filter(
        (spark_df.Authors.isNull()) | (spark_df.Authors == "")
    )
    null_pages_number_df = spark_df.filter((spark_df.PagesNumber.isNull()))

    count = 0
    for row in null_authors_df.select("ISBN").collect():
        isbn = row[0]
        context.log.debug(f"Got isbn: {isbn}")
        if isbn is not None:
            # Get request from OpenLibrary API
            req = requests.get(
                f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data"
            )
            json = req.json()
            if len(json.keys()) > 0:
                context.log.debug("Got json with data")
                # Check if spark_df with column 'ISBN' = isbn has missing value in column 'Authors'
                row_from_df = spark_df.filter(spark_df.ISBN == isbn).collect()[0]
                if row_from_df.Authors is None or row_from_df.Authors is "":
                    context.log.debug("Authors is missing, start filling ...")
                    # Take the first author
                    author = json.get(f"ISBN:{isbn}" or {}).get("authors" or [])
                    author = author[0].get("name") if len(author) > 0 else None
                    if author:
                        count += 1
                        # Update spark_df with column 'ISBN' = isbn and column 'Authors' = author
                        spark_df = spark_df.withColumn(
                            "Authors",
                            when(
                                (spark_df.ISBN == isbn)
                                & (
                                    (spark_df.Authors.isNull())
                                    | (spark_df.Authors == "")
                                ),
                                author,
                            ).otherwise(spark_df.Authors),
                        )
    context.log.info(f"Filled in {count} authors")

    count = 0
    for row in null_pages_number_df.select("ISBN").collect():
        isbn = row[0]
        context.log.debug(f"Got isbn: {isbn}")
        if isbn is not None:
            # Get request from OpenLibrary API
            req = requests.get(
                f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data"
            )
            json = req.json()
            if len(json.keys()) > 0:
                context.log.debug("Got json with real data")
                # Check if spark_df with column 'ISBN' = isbn has missing value in column 'Authors'
                row_from_df = spark_df.filter(spark_df.ISBN == isbn).collect()[0]
                # Check if spark_df with column 'ISBN' = isbn has missing value in column 'PagesNumber'
                if row_from_df.PagesNumber is None or row_from_df.PagesNumber == 0:
                    context.log.debug("PagesNumber is missing, start filling ...")
                    # Take the number of pages
                    pages_number = json.get(f"ISBN:{isbn}" or {}).get("number_of_pages")
                    if pages_number:
                        count += 1
                        # Update spark_df with column 'ISBN' = isbn and column 'PagesNumber' = pages_number
                        spark_df = spark_df.withColumn(
                            "PagesNumber",
                            when(
                                (spark_df.ISBN == isbn)
                                & (spark_df.PagesNumber.isNull()),
                                pages_number,
                            ).otherwise(spark_df.PagesNumber),
                        )
    context.log.info(f"Filled in {count} pages numbers")

    spark_df.unpersist()

    return Output(
        value=spark_df,
        metadata={
            "table": "silver_collected_book",
            "row_count": spark_df.count(),
            "column_count": len(spark_df.columns),
            "columns": spark_df.columns,
        },
    )


# ISBN
@asset(
    description="Extract column 'ISBN' from silver_cleaned_book",
    ins={
        "silver_cleaned_book": AssetIn(
            key_prefix=["silver", "goodreads"],
            metadata={"full_load": True},
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "goodreads"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_isbn(context, silver_cleaned_book: DataFrame) -> Output[DataFrame]:
    """
    Extract column 'ISBN' from cleaned book
    """

    context.log.debug("Extracting ISBN ...")
    spark_df = silver_cleaned_book.select("ISBN")

    return Output(
        value=spark_df,
        metadata={
            "table": "silver_isbn",
            "row_count": spark_df.count(),
            "column_count": len(spark_df.columns),
            "columns": spark_df.columns,
        },
    )


# Silver collected genre
@asset(
    description="Collect more infomation about cleaned genres",
    ins={
        "silver_isbn": AssetIn(
            key_prefix=["silver", "goodreads"],
        ),
        "silver_cleaned_genre": AssetIn(
            key_prefix=["silver", "goodreads"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "goodreads"],
    compute_kind="OpenLibrary API",
    group_name=LAYER,
)
def silver_collected_genre(
    context, silver_isbn: DataFrame, silver_cleaned_genre: DataFrame
) -> Output[DataFrame]:
    """
    Collect more infomation about cleaned genres, with upstream isbn
    Connect to OpenLibrary API to get more information about genre,
    and union to silver_cleaned_genre, unique by 'Name'
    """

    return Output(
        value=silver_cleaned_genre,
        metadata={
            "table": "silver_collected_genre",
            "row_count": silver_cleaned_genre.count(),
            "column_count": len(silver_cleaned_genre.columns),
            "columns": silver_cleaned_genre.columns,
        },
    )


# Silver collected book_genre
@asset(
    description="Collect more relationships about books and genres",
    ins={
        "silver_isbn": AssetIn(
            key_prefix=["silver", "goodreads"],
        ),
        "silver_collected_genre": AssetIn(
            key_prefix=["silver", "goodreads"],
        ),
        "bronze_book_genre": AssetIn(
            key_prefix=["bronze", "goodreads"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "goodreads"],
    compute_kind="OpenLibrary API",
    group_name=LAYER,
)
def silver_collected_book_genre(
    context,
    silver_isbn: DataFrame,
    silver_collected_genre: DataFrame,
    bronze_book_genre: pl.DataFrame,
) -> Output[DataFrame]:
    """
    Collect more relationships about books and genres
    """

    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    context.log.debug("Start creating spark session")

    # Convert bronze_book from polars DataFrame to Spark DataFrame
    pandas_df = bronze_book_genre.to_pandas()
    context.log.debug(f"Converted to pandas DataFrame with shape: {pandas_df.shape}")

    with get_spark_session(config) as spark:
        spark_df = spark.createDataFrame(pandas_df)
        spark_df.cache()
        context.log.info("Got Spark DataFrame")

        return Output(
            value=spark_df,
            metadata={
                "table": "silver_collected_book_genre",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )
