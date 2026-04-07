from datetime import timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class SparkClient:
    """
    Handles raw CSV reads from MinIO.

    Responsible only for I/O: reading CSVs from S3 and inferring the analysis
    date window. All business logic (which tables to join, which filters to apply,
    which aggregations to compute) lives in domain/pricing/master_dataset.py.
    """

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def read_csv(self, bucket: str, filename: str) -> DataFrame:
        return (
            self._spark.read.option("header", "true")
            .option("inferSchema", "false")
            .csv(f"s3a://{bucket}/{filename}")
        )

    def get_date_range(self, bucket: str, months_back: int) -> tuple[str, str]:
        """Infers the analysis window from the latest order date in the bronze bucket."""
        orders = self.read_csv(bucket, "olist_orders_dataset.csv")
        latest = (
            orders.select(F.max(F.to_date("order_purchase_timestamp")).alias("latest"))
            .collect()[0]["latest"]
        )
        start = latest - timedelta(days=months_back * 30)
        return start.isoformat(), latest.isoformat()
