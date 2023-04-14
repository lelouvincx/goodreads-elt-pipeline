from dagster import Definitions, load_assets_from_modules
import os

from . import assets
from .resources.mysql_io_manager import MySQLIOManager
from .resources.minio_io_manager import MinIOIOManager
from .resources.gdrive_io_manager import GDriveIOManager
from .resources.spark_io_manager import SparkIOManager
from .resources.psql_io_manager import PostgreSQLIOManager


MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "port": os.getenv("MYSQL_PORT"),
    "database": os.getenv("MYSQL_DATABASE"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
}

MINIO_CONFIG = {
    "bucket": os.getenv("DATALAKE_BUCKET"),
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
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

SPARK_CONFIG = {
    "spark_master": os.getenv("SPARK_MASTER_URL"),
    "spark_version": os.getenv("SPARK_VERSION"),
    "hadoop_version": os.getenv("HADOOP_VERSION"),
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


resources = {
    "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
    "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
    "gdrive_io_manager": GDriveIOManager(GDRIVE_CONFIG),
    "spark_io_manager": SparkIOManager(SPARK_CONFIG),
    "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
}

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    resources=resources,
)
