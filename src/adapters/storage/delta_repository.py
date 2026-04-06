from pyspark.sql import DataFrame, SparkSession

from src.config import Settings


def get_spark_session(settings: Settings) -> SparkSession:
    """
    Builds a SparkSession connected to the Spark cluster and configured
    to read/write Delta Lake tables on MinIO via the S3A connector.

    JARs (delta-spark, hadoop-aws, aws-java-sdk-bundle) are pre-baked
    into the custom Spark Docker image — no runtime package downloads needed.
    """
    return (
        SparkSession.builder.master(settings.spark_master_url)
        .appName("olist-medallion")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", settings.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


class DeltaRepository:
    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def write(self, df: DataFrame, path: str, mode: str = "overwrite") -> None:
        df.write.format("delta").mode(mode).save(path)

    def read(self, path: str) -> DataFrame:
        return self._spark.read.format("delta").load(path)
