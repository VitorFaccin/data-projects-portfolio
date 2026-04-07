"""
Olist Medallion Pipeline DAG.

This file is the only Airflow-specific code in the project. It defines when
tasks run and in what order. Each task follows the I/O sandwich pattern:
READ (infrastructure client) → TRANSFORM (pure domain function) → WRITE (infrastructure client).

If you swap Airflow for Prefect or Dagster, only this file changes.
"""

import kagglehub
from datetime import date, datetime

from airflow.decorators import dag, task

from data_projects_portfolio.config import get_settings
from data_projects_portfolio.infrastructure.delta_client import DeltaClient, get_spark_session
from data_projects_portfolio.infrastructure.minio_client import MinioClient
from data_projects_portfolio.infrastructure.polars_client import PolarsClient
from data_projects_portfolio.infrastructure.s3_client import S3Client
from data_projects_portfolio.infrastructure.spark_client import SparkClient
from data_projects_portfolio.domain.pricing.master_dataset import build_master_dataset
from data_projects_portfolio.domain.pricing import elasticity as pricing
from data_projects_portfolio.domain.pricing.elasticity import MONTHS_BACK


@dag(
    dag_id="olist_medallion",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["pricing", "medallion", "olist", "portfolio"],
    doc_md="""
    ## Olist Medallion Pipeline

    Implements the Bronze → Silver → Gold medallion architecture over the public
    [Olist e-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

    | Layer  | Storage          | Tool   | What happens                                      |
    |--------|------------------|--------|---------------------------------------------------|
    | Bronze | MinIO `bronze/`  | boto3  | Raw CSVs uploaded from Kaggle as-is               |
    | Silver | MinIO `silver/`  | Spark  | Joins, filters, aggregations → Delta Lake table   |
    | Gold   | MinIO `gold/`    | Polars | Log-log elasticity regression → result CSV        |
    """,
)
def olist_medallion():

    @task(task_id="bronze_ingest_olist_csvs")
    def bronze() -> None:
        settings = get_settings()
        # READ
        dataset_path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
        # WRITE
        uploaded = MinioClient(settings).upload_directory(dataset_path, settings.bronze_bucket)
        print(f"Bronze ingestion complete. Uploaded {len(uploaded)} files: {uploaded}")

    @task(task_id="silver_build_master_dataset")
    def silver() -> None:
        settings = get_settings()
        spark = get_spark_session(settings)
        client = SparkClient(spark)
        delta = DeltaClient(spark)
        # READ
        start_date, end_date = client.get_date_range(settings.bronze_bucket, months_back=MONTHS_BACK)
        print(f"Silver transformation: window {start_date} → {end_date}")
        orders      = client.read_csv(settings.bronze_bucket, "olist_orders_dataset.csv")
        items       = client.read_csv(settings.bronze_bucket, "olist_order_items_dataset.csv")
        customers   = client.read_csv(settings.bronze_bucket, "olist_customers_dataset.csv")
        products    = client.read_csv(settings.bronze_bucket, "olist_products_dataset.csv")
        translation = client.read_csv(settings.bronze_bucket, "product_category_name_translation.csv")
        # TRANSFORM
        master = build_master_dataset(
            orders, items, customers, products, translation,
            start_date=start_date, end_date=end_date,
        )
        # WRITE
        silver_path = f"s3a://{settings.silver_bucket}/olist_master/"
        delta.write(master, silver_path)
        print(f"Silver layer written to {silver_path}")

    @task(task_id="gold_compute_price_elasticity")
    def gold() -> None:
        settings = get_settings()
        calc_date = date.today()
        # READ
        silver_path = f"s3a://{settings.silver_bucket}/olist_master/"
        df = PolarsClient(settings).read_delta(silver_path).to_pandas()
        # TRANSFORM
        df = pricing.flag_spike_days(df)
        df = pricing.filter_low_price_variation(df)
        seasonal_index = pricing.calculate_seasonal_index(df)
        df = pricing.adjust_units_for_seasonality(df, seasonal_index)
        df = pricing.build_price_tiers(df)
        results = [
            pricing.run_log_log_regression(group_df)
            for _, group_df in df.groupby(["product_id", "sale_location"])
        ]
        elasticity_df = pricing.impute_elasticities(results)
        elasticity_df["data_calculo"] = calc_date.isoformat()
        # WRITE
        gold_key = "pricing/elasticity_regression_loglog.csv"
        S3Client(settings).upload_csv(elasticity_df, settings.gold_bucket, gold_key)
        print(
            f"Gold layer written: {len(elasticity_df)} products "
            f"to s3a://{settings.gold_bucket}/{gold_key}"
        )

    bronze() >> silver() >> gold()


olist_medallion()
