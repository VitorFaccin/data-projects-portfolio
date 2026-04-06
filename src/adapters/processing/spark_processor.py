from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType


class SparkProcessor:
    """
    Handles Bronze → Silver transformations using PySpark.

    Reads raw Olist CSVs from the MinIO bronze bucket, performs the join
    and aggregation logic that mirrors extract_daily_sales_pricing.sql,
    and returns a cleaned Spark DataFrame ready for Delta Lake.

    Spark is chosen here because the raw Olist dataset is several hundred MB
    across five CSVs — appropriate for distributed processing.
    """

    def __init__(self, spark: SparkSession, bronze_bucket: str) -> None:
        self._spark = spark
        self._base = f"s3a://{bronze_bucket}"

    def _read_csv(self, filename: str) -> DataFrame:
        return (
            self._spark.read.option("header", "true")
            .option("inferSchema", "false")
            .csv(f"{self._base}/{filename}")
        )

    def build_master_dataset(self, start_date: str, end_date: str) -> DataFrame:
        """
        Joins the five Olist CSVs and aggregates to daily product/location metrics.

        Replicates the logic in sql/extract_daily_sales_pricing.sql:
        - Inner join: order_items → orders → customers
        - Left join: → products → category translation
        - Filters: delivered orders, non-November, date range, price > 0, delivery_days > 0
        - Aggregates: daily COUNT, AVG(price), AVG(estimated_delivery_days)
        """
        orders = self._read_csv("olist_orders_dataset.csv")
        items = self._read_csv("olist_order_items_dataset.csv")
        customers = self._read_csv("olist_customers_dataset.csv")
        products = self._read_csv("olist_products_dataset.csv")
        translation = self._read_csv("product_category_name_translation.csv")

        delivered_items = (
            items.join(orders, "order_id", "inner")
            .join(customers, "customer_id", "inner")
            .join(products, "product_id", "left")
            .join(translation, "product_category_name", "left")
            .withColumn("data_date", F.to_date("order_purchase_timestamp"))
            .withColumn("item_price", F.col("price").cast(DoubleType()))
            .withColumn(
                "estimated_delivery_days",
                F.datediff(
                    F.to_date("order_estimated_delivery_date"),
                    F.to_date("order_purchase_timestamp"),
                ),
            )
            .withColumn(
                "product_category",
                F.coalesce(
                    F.col("product_category_name_english"),
                    F.col("product_category_name"),
                    F.lit("unknown"),
                ),
            )
            .filter(F.col("order_status") == "delivered")
            .filter(F.col("order_purchase_timestamp").isNotNull())
            .filter(F.col("order_estimated_delivery_date").isNotNull())
            .filter(F.col("item_price") > 0)
            .filter(F.month(F.col("order_purchase_timestamp")) != 11)
            .filter(F.col("data_date").between(start_date, end_date))
            .filter(F.col("estimated_delivery_days") > 0)
            .select(
                "data_date",
                "product_id",
                F.col("customer_state").alias("sale_location"),
                "product_category",
                "item_price",
                "estimated_delivery_days",
            )
        )

        master_df = (
            delivered_items.groupBy("data_date", "product_id", "sale_location", "product_category")
            .agg(
                F.count("*").cast(IntegerType()).alias("quantidade_produto"),
                F.avg("item_price").cast(DoubleType()).alias("preco"),
                F.avg("estimated_delivery_days").cast(DoubleType()).alias("prazo"),
            )
            .orderBy("data_date", "product_id", "sale_location")
        )
        return master_df

    def get_date_range(self, months_back: int) -> tuple[str, str]:
        """Infers the analysis window from the latest order date in the bronze bucket."""
        orders = self._read_csv("olist_orders_dataset.csv")
        latest = (
            orders.select(F.max(F.to_date("order_purchase_timestamp")).alias("latest"))
            .collect()[0]["latest"]
        )
        start = latest - __import__("datetime").timedelta(days=months_back * 30)
        return start.isoformat(), latest.isoformat()
