"""
Olist Medallion Pipeline DAG.

This file is the only Airflow-specific code in the project. It defines when
tasks run and in what order. All business logic lives in src/.

If you swap Airflow for Prefect or Dagster, only this file changes.
"""

from datetime import datetime

from airflow.decorators import dag, task

from src.entrypoints.bronze_ingestion import run as run_bronze
from src.entrypoints.silver_transformation import run as run_silver
from src.entrypoints.gold_elasticity import run as run_gold


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
        run_bronze()

    @task(task_id="silver_build_master_dataset")
    def silver() -> None:
        run_silver()

    @task(task_id="gold_compute_price_elasticity")
    def gold() -> None:
        run_gold()

    bronze() >> silver() >> gold()


olist_medallion()
