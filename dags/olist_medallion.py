"""
Olist Medallion Pipeline DAG.

This file is the only Airflow-specific code in the project. Airflow acts as a
pure control plane: it schedules tasks and monitors their status, but does not
perform any processing itself.

Architecture by task:
- bronze:  @task (Python) — boto3 download from Kaggle + upload to MinIO. No Spark.
- silver:  SparkSubmitOperator — submits spark_jobs/silver.py to the Spark cluster.
           The driver runs on a Spark Worker (deploy-mode=cluster). All S3A and Delta
           JARs are resolved inside the Spark environment, not in Airflow.
- gold:    @task (Python) — Polars reads Delta from MinIO, runs elasticity regression,
           uploads CSV result. No Spark.

If you swap Airflow for Prefect or Dagster, only this file changes.
"""

import kagglehub
from datetime import date, datetime

from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from data_projects_portfolio.config import get_settings
from data_projects_portfolio.infrastructure.minio_client import MinioClient
from data_projects_portfolio.infrastructure.polars_client import PolarsClient
from data_projects_portfolio.infrastructure.s3_client import S3Client
from data_projects_portfolio.domain.pricing import elasticity as pricing


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

    | Layer  | Storage          | Tool                  | What happens                                    |
    |--------|------------------|-----------------------|-------------------------------------------------|
    | Bronze | MinIO `bronze/`  | boto3 (Airflow task)  | Raw CSVs uploaded from Kaggle as-is             |
    | Silver | MinIO `silver/`  | Spark (cluster job)   | Joins, filters, aggregations → Delta Lake table |
    | Gold   | MinIO `gold/`    | Polars (Airflow task) | Log-log elasticity regression → result CSV      |
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

    
    silver = SparkSubmitOperator(
        task_id="silver_build_master_dataset",
        application="/opt/entrypoints/pricing/silver.py",
        conn_id="spark_default",
        deploy_mode="cluster",
        name="olist-silver",
    )

    @task(task_id="gold_compute_price_elasticity")
    def gold() -> None:
        settings = get_settings()
        calc_date = date.today()
        silver_path = f"s3a://{settings.silver_bucket}/olist_master/"
        df = PolarsClient(settings).read_delta(silver_path).to_pandas()
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
        gold_key = "pricing/elasticity_regression_loglog.csv"
        S3Client(settings).upload_csv(elasticity_df, settings.gold_bucket, gold_key)
        print(
            f"Gold layer written: {len(elasticity_df)} products "
            f"to s3a://{settings.gold_bucket}/{gold_key}"
        )

    bronze() >> silver >> gold()


olist_medallion()
