"""
Olist Medallion Pipeline DAG.

This file is the only Airflow-specific code in the project. Airflow acts as a
pure control plane: it schedules tasks and monitors their status, but does not
perform any processing itself. No Spark dependencies (no Java, no pyspark,
no JARs) exist in the Airflow container.

Architecture by task:
- bronze:  @task (Python) — boto3 download from Kaggle + upload to MinIO. No Spark.
- silver:  @task (Python) — submits entrypoints/pricing/silver.py to the Spark cluster
           via the Spark REST API (HTTP POST to port 6066), then polls for completion.
           The driver runs on a Spark Worker. All JARs live in the Spark image only.
- gold:    @task (Python) — Polars reads Delta from MinIO, runs elasticity regression,
           uploads CSV result. No Spark.

If you swap Airflow for Prefect or Dagster, only this file changes.
"""

import time
import kagglehub
import requests
from datetime import date, datetime

from airflow.decorators import dag, task

from data_projects_portfolio.config import get_settings
from data_projects_portfolio.infrastructure.minio_client import MinioClient
from data_projects_portfolio.infrastructure.polars_client import PolarsClient
from data_projects_portfolio.infrastructure.s3_client import S3Client
from data_projects_portfolio.domain.pricing import elasticity as pricing

_SPARK_REST_URL = "http://spark-master:6066"
_SPARK_JARS = ",".join([
    "/opt/spark/jars/delta-spark_2.12-3.1.0.jar",
    "/opt/spark/jars/delta-storage-3.1.0.jar",
    "/opt/spark/jars/hadoop-aws-3.3.4.jar",
    "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
])


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

    | Layer  | Storage          | Tool                     | What happens                                    |
    |--------|------------------|--------------------------|-------------------------------------------------|
    | Bronze | MinIO `bronze/`  | boto3 (Airflow task)     | Raw CSVs uploaded from Kaggle as-is             |
    | Silver | MinIO `silver/`  | Spark REST API → cluster | Joins, filters, aggregations → Delta Lake table |
    | Gold   | MinIO `gold/`    | Polars (Airflow task)    | Log-log elasticity regression → result CSV      |
    """,
)
def olist_medallion():

    @task(task_id="bronze_ingest_olist_csvs")
    def bronze() -> None:
        settings = get_settings()
        dataset_path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
        uploaded = MinioClient(settings).upload_directory(dataset_path, settings.bronze_bucket)
        print(f"Bronze ingestion complete. Uploaded {len(uploaded)} files: {uploaded}")

    @task(task_id="silver_build_master_dataset")
    def silver() -> None:
        settings = get_settings()

        payload = {
            "action": "CreateSubmissionRequest",
            "appResource": "/opt/entrypoints/pricing/silver.py",
            "clientSparkVersion": "3.5.1",
            "mainClass": "org.apache.spark.deploy.PythonRunner",
            "environmentVariables": {
                "SPARK_ENV_LOADED": "1",
                "MINIO_ENDPOINT": settings.minio_endpoint,
                "MINIO_ACCESS_KEY": settings.minio_access_key,
                "MINIO_SECRET_KEY": settings.minio_secret_key,
                "SPARK_MASTER_URL": settings.spark_master_url,
                "PYSPARK_PYTHON": "python3",
            },
            "sparkProperties": {
                "spark.master": settings.spark_master_url,
                "spark.app.name": "olist-silver",
                "spark.submit.deployMode": "cluster",
                "spark.jars": _SPARK_JARS,
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.hadoop.fs.s3a.endpoint": settings.minio_endpoint,
                "spark.hadoop.fs.s3a.access.key": settings.minio_access_key,
                "spark.hadoop.fs.s3a.secret.key": settings.minio_secret_key,
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            },
            "appArgs": ["/opt/entrypoints/pricing/silver.py", ""],
        }

        resp = requests.post(
            f"{_SPARK_REST_URL}/v1/submissions/create",
            json=payload,
            timeout=60,
        )
        resp.raise_for_status()
        submission_id = resp.json()["submissionId"]
        print(f"Silver job submitted: {submission_id}")

        while True:
            time.sleep(10)
            status_resp = requests.get(
                f"{_SPARK_REST_URL}/v1/submissions/status/{submission_id}",
                timeout=10,
            )
            status_resp.raise_for_status()
            state = status_resp.json()["driverState"]
            print(f"Silver job state: {state}")
            if state == "FINISHED":
                print("Silver job completed successfully.")
                return
            if state in ("FAILED", "KILLED", "ERROR", "UNKNOWN"):
                raise RuntimeError(f"Silver job failed with state: {state}")

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
