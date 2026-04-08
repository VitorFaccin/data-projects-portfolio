"""
Silver layer Spark job.

This script is submitted to the Spark cluster via SparkSubmitOperator (deploy-mode=cluster).
The driver process runs on a Spark Worker — Airflow only submits and monitors.

Why this separation matters:
- S3A JARs (hadoop-aws, aws-java-sdk-bundle) and Delta JARs live in the Spark image.
- The driver needs those JARs to resolve s3a:// and Delta format.
- With deploy-mode=cluster, the driver runs where the JARs are (Spark Worker),
  not where Airflow is. Airflow becomes a true control plane with no Spark dependencies.
"""
from data_projects_portfolio.config import get_settings
from data_projects_portfolio.infrastructure.delta_client import DeltaClient, get_spark_session
from data_projects_portfolio.infrastructure.spark_client import SparkClient
from data_projects_portfolio.domain.pricing.master_dataset import build_master_dataset, MONTHS_BACK


def main() -> None:
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


if __name__ == "__main__":
    main()
