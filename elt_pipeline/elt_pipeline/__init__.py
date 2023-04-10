import os
from dagster import Definitions

from .assets.bronze import *
from .assets.silver import *
from .assets.gold import *
from .resources.mysql_io_manager import MySQLIOManager
from .resources.minio_io_manager import MinIOIOManager
from .resources.gdrive_io_manager import GDriveIOManager


MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "port": os.getenv("MYSQL_PORT"),
    "database": os.getenv("MYSQL_DATABASE"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
}

MINIO_CONFIG = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "bucket": os.getenv("DATALAKE_BUCKET"),
    "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
    "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
}

GDRIVE_CONFIG = {
    "client_secret_file": os.path.join(
        os.getcwd(), str(os.getenv("GDRIVE_CLIENT_SECRET_FILE"))
    ),
    "pickle_file": os.path.join(
        os.getcwd(), "elt_pipeline", str(os.getenv("GDRIVE_PICKLE_FILE"))
    ),
    "api_name": os.getenv("GDRIVE_API_NAME"),
    "api_version": os.getenv("GDRIVE_API_VERSION"),
    "scopes": os.getenv("GDRIVE_SCOPES"),
    "bucket": os.getenv("DATALAKE_BUCKET"),
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
    "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
}

PSQL_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

defs = Definitions(
    assets=[
        bronze_genre,
        bronze_book,
        bronze_book_genre,
        bronze_book_download_link,
        bronze_images_and_files_download,
    ],
    resources={
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "gdrive_io_manager": GDriveIOManager(GDRIVE_CONFIG),
    },
)
