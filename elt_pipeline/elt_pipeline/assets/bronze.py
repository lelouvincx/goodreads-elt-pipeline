from dagster import asset, AssetIn, Output, StaticPartitionsDefinition
from datetime import datetime
import polars as pl


COMPUTE_KIND = "SQL"
LAYER = "bronze"
YEARLY = StaticPartitionsDefinition(
    [str(year) for year in range(1975, datetime.today().year)]
)


# genre from my_sql
@asset(
    description="Load table 'genre' from MySQL database as polars DataFrame, and save to minIO",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "goodreads"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def bronze_genre(context) -> Output[pl.DataFrame]:
    query = "SELECT * FROM genre;"
    df_data = context.resources.mysql_io_manager.extract_data(query)
    context.log.info(f"Table extracted with shape: {df_data.shape}")

    return Output(
        value=df_data,
        metadata={
            "table": "genre",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": df_data.columns,
        },
    )


# book from my_sql
@asset(
    description="Load table 'book' from MySQL database as polars DataFrame, and save to minIO",
    partitions_def=YEARLY,
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "goodreads"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def bronze_book(context) -> Output[pl.DataFrame]:
    query = "SELECT * FROM book"
    try:
        partion_year_str = context.asset_partition_key_for_output()
        partition_by = "PublishYear"
        query += f" WHERE {partition_by} = {partion_year_str}"
        context.log.info(f"Partition by {partition_by} = {partion_year_str}")
    except Exception:
        context.log.info("No partition key found, full load data")

    df_data = context.resources.mysql_io_manager.extract_data(query)
    context.log.info(f"Table extracted with shape: {df_data.shape}")

    return Output(
        value=df_data,
        metadata={
            "table": "book",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": df_data.columns,
        },
    )


# book_genre from my_sql
@asset(
    description="Load table 'book_genre' from MySQL database as polars DataFrame, and save to minIO",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    non_argument_deps={"bronze_book", "bronze_genre"},
    key_prefix=["bronze", "goodreads"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def bronze_book_genre(context) -> Output[pl.DataFrame]:
    query = "SELECT * FROM book_genre;"
    df_data = context.resources.mysql_io_manager.extract_data(query)
    context.log.info(f"Table extracted with shape: {df_data.shape}")

    return Output(
        value=df_data,
        metadata={
            "table": "book_genre",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": df_data.columns,
        },
    )


# book_download_link from my_sql
@asset(
    description="Load table 'book_download_link' from MySQL database as polars DataFrame, and save to minIO",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    non_argument_deps={"bronze_book"},
    key_prefix=["bronze", "goodreads"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def bronze_book_download_link(context) -> Output[pl.DataFrame]:
    query = "SELECT * FROM book_download_link;"
    df_data = context.resources.mysql_io_manager.extract_data(query)
    context.log.info(f"Table extracted with shape: {df_data.shape}")

    return Output(
        value=df_data,
        metadata={
            "table": "book_download_link",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": df_data.columns,
        },
    )


# download files from gdrive, given a download link
@asset(
    description="Download image and epub file for books from gdrive, given a download link",
    io_manager_key="gdrive_io_manager",
    ins={
        "bronze_book_download_link": AssetIn(
            key_prefix=["bronze", "goodreads"],
        )
    },
    compute_kind="google drive",
    group_name=LAYER,
)
def bronze_images_and_files_download(
    context, bronze_book_download_link: pl.DataFrame
) -> Output[dict]:
    """
    From upstream table 'book_download_link', download files from google drive
    with given download link, extract the images and files from it,
    then return the path to the folder containing the downloaded files.
    """

    # Create temp folder path, e.g '/tmp/bronze/download/2021-08-01T00:00:00+00:00' -> images|files
    # WARN: If change the key_prefix above, also change the path here
    tmp_folder_path = f"/tmp/bronze/download/{datetime.now().isoformat()}"
    context.log.info(f"Path: {tmp_folder_path}")

    # Download folders by call download_folders function from gdrive_io_manager
    context.resources.gdrive_io_manager.download_folders(
        context=context,
        dataframe=bronze_book_download_link,
        tmp_folder_path=tmp_folder_path,
    )

    return Output(
        value={
            "tmp_folder_path": tmp_folder_path,
            "isbn": bronze_book_download_link["BookISBN"].to_list(),
        },
        metadata={
            "isbn": bronze_book_download_link["BookISBN"].to_list(),
            "download_link": bronze_book_download_link["Link"].to_list(),
        },
    )
