import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    spark_master_url: str
    bronze_bucket: str = "bronze"
    silver_bucket: str = "silver"
    gold_bucket: str = "gold"


def get_settings() -> Settings:
    return Settings(
        minio_endpoint=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        minio_access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin123"),
        spark_master_url=os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"),
    )
