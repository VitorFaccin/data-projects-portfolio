from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType


def build_master_dataset(
    orders: DataFrame,
    items: DataFrame,
    customers: DataFrame,
    products: DataFrame,
    translation: DataFrame,
    *,
    start_date: str,
    end_date: str,
) -> DataFrame:
    """
    Pure function: joins the five Olist DataFrames and aggregates to daily
    product/location metrics. Receives DataFrames, returns a DataFrame. Zero I/O.

    Business rules applied here:
    - Only delivered orders are counted
    - November is excluded (Black Friday distorts elasticity)
    - Prices must be > 0 and estimated delivery days must be > 0
    - Aggregates: daily COUNT, AVG(price), AVG(estimated_delivery_days)
    """
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

    return (
        delivered_items.groupBy("data_date", "product_id", "sale_location", "product_category")
        .agg(
            F.count("*").cast(IntegerType()).alias("quantidade_produto"),
            F.avg("item_price").cast(DoubleType()).alias("preco"),
            F.avg("estimated_delivery_days").cast(DoubleType()).alias("prazo"),
        )
        .orderBy("data_date", "product_id", "sale_location")
    )
