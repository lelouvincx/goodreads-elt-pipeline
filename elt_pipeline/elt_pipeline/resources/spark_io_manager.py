from dagster import IOManager, InputContext, OutputContext
from pyspark.sql import SparkSession, DataFrame

from contextlib import contextmanager


@contextmanager
def get_spark_session(config):
    try:
        spark = (
            SparkSession.builder.master("spark://spark-master:7077")
            .appName("Spark IO Manager")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config(
                "spark.jars",
                "/usr/local/spark/jars/delta-core_2.12-2.2.0.jar,/usr/local/spark/jars/hadoop-aws-3.3.2.jar,/usr/local/spark/jars/delta-storage-2.2.0.jar,/usr/local/spark/jars/aws-java-sdk-1.12.367.jar,/usr/local/spark/jars/s3-2.18.41.jar,/usr/local/spark/jars/aws-java-sdk-bundle-1.11.1026.jar",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{config['endpoint_url']}")
            .config("spark.hadoop.fs.s3a.access.key", str(config["minio_access_key"]))
            .config("spark.hadoop.fs.s3a.secret.key", str(config["minio_secret_key"]))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.connection.ssl.enabled", "false")
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
            .getOrCreate()
        )
        yield spark
    except Exception as e:
        raise Exception(f"Error while creating spark session: {e}")


class SparkIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def handle_output(self, context: "OutputContext", obj: DataFrame):
        """
        Write output to s3a (aka minIO) as parquet file
        """

        context.log.debug("(Spark handle_output) Writing output to MinIO ...")
        context.log.debug(
            f"(Spark handle_output) Got dataframe with rows count: {obj.count()}"
        )

        # E.g file_path: s3a://lakehouse/silver/goodreads/book
        file_path = "s3a://lakehouse/" + "/".join(context.asset_key.path) + ".parquet"
        context.log.debug(f"(Spark handle_output) File path: {file_path}")
        file_name = str(context.asset_key.path[-1])
        context.log.debug(f"(Spark handle_output) File name: {file_name}")

        try:
            obj.write.mode("overwrite").parquet(file_path)
            context.log.debug(f"Saved {file_name} to {file_path}")
        except Exception as e:
            raise Exception(f"(Spark handle_output) Error while writing output: {e}")

    def load_input(self, context: "InputContext") -> DataFrame:
        """
        Load input from s3a (aka minIO) from parquet file to spark.sql.DataFrame
        """

        # E.g context.asset_key.path: ['silver', 'goodreads', 'book']
        context.log.debug(f"Loading input from {context.asset_key.path}...")
        file_path = "s3a://" + "/".join(context.asset_key.path)
        context.log.debug("File path: " + file_path)
        # E.g file_name: book
        file_name = str(context.asset_key.path[-1])

        try:
            with get_spark_session(self._config) as spark:

                df = spark.read.parquet(file_path)
                context.log.debug(f"Loaded {df.count()} rows from {file_path}")

                df.cache()
                return df
        except Exception as e:
            raise Exception(f"Error while loading input: {e}")
