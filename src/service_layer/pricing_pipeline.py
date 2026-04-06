"""
Pricing pipeline service layer.

This module is the "maestro": it orchestrates adapters and domain logic
but knows nothing about Airflow, Prefect, or any other orchestrator.
Swapping the orchestrator means only the entrypoints/ files change.
"""

import datetime
import io

import pandas as pd

import kagglehub
import boto3
from botocore.client import Config

from src.adapters.processing.polars_processor import PolarsProcessor
from src.adapters.processing.spark_processor import SparkProcessor
from src.adapters.storage.delta_repository import DeltaRepository, get_spark_session
from src.adapters.storage.minio_client import MinioClient
from src.config import Settings
from src.domain.pricing import services as pricing_services
from src.domain.pricing.services import MONTHS_BACK


def run_bronze_ingestion(settings: Settings) -> None:
    """
    Downloads the Olist public dataset via kagglehub and uploads all CSVs
    to the MinIO bronze bucket as-is (raw landing zone).
    """

    dataset_path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
    client = MinioClient(settings)
    uploaded = client.upload_directory(
        local_dir=dataset_path,
        bucket=settings.bronze_bucket,
    )
    print(f"Bronze ingestion complete. Uploaded {len(uploaded)} files: {uploaded}")


def run_silver_transformation(settings: Settings) -> None:
    """
    Reads raw CSVs from the bronze bucket with Spark, joins and aggregates them
    into a daily product/location master dataset, and writes it as a Delta table
    to the silver bucket.

    Spark is used because the raw Olist joins span ~100k orders × multiple tables —
    appropriate for distributed processing even in single-worker mode.
    """
    spark = get_spark_session(settings)
    processor = SparkProcessor(spark, settings.bronze_bucket)
    repo = DeltaRepository(spark)

    start_date, end_date = processor.get_date_range(months_back=MONTHS_BACK)
    print(f"Silver transformation: window {start_date} → {end_date}")

    master_df = processor.build_master_dataset(start_date=start_date, end_date=end_date)
    silver_path = f"s3a://{settings.silver_bucket}/olist_master/"
    repo.write(master_df, silver_path)
    print(f"Silver layer written to {silver_path}")


def run_gold_elasticity(settings: Settings) -> None:
    """
    Reads the silver master dataset with Polars, runs the log-log price elasticity
    regression using pure domain logic, and uploads the results as CSV to the gold bucket.

    Polars is used here because the aggregated silver dataset is medium-sized —
    no need for Spark's cluster overhead for analytical computations.
    """
    processor = PolarsProcessor(settings)

    silver_path = f"s3a://{settings.silver_bucket}/olist_master/"
    master_df = processor.to_pandas(processor.read_delta(silver_path))

    flagged_df = pricing_services.flag_spike_days(master_df)
    filtered_df = pricing_services.filter_low_price_variation(flagged_df)
    seasonal_index = pricing_services.calculate_seasonal_index(filtered_df)
    adjusted_df = pricing_services.adjust_units_for_seasonality(filtered_df, seasonal_index)
    tiered_df = pricing_services.build_price_tiers(adjusted_df)

    results = [
        pricing_services.run_log_log_regression(group_df)
        for _, group_df in tiered_df.groupby(["product_id", "sale_location"])
    ]

    elasticity_df = pricing_services.impute_elasticities(results)
    elasticity_df["data_calculo"] = datetime.date.today().isoformat()

    csv_buffer = io.StringIO()
    elasticity_df.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode("utf-8")



    s3 = boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    s3.put_object(
        Bucket=settings.gold_bucket,
        Key="pricing/elasticity_regression_loglog.csv",
        Body=csv_bytes,
    )
    print(
        f"Gold layer written: {len(elasticity_df)} products "
        f"to s3a://{settings.gold_bucket}/pricing/elasticity_regression_loglog.csv"
    )
