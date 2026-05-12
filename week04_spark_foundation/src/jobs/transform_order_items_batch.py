from pyspark.sql.functions import (
    col,
    trim,
    to_timestamp,
    round as spark_round
)

from src.common.spark_session import create_spark_session
from src.schemas.order_items_schema import order_items_schema


RAW_PATH = "data/raw/olist_order_items_dataset.csv"
OUTPUT_PATH = "data/curated/order_items"


def main():
    spark = create_spark_session("transform-order-items-batch")

    df_raw = (
        spark.read
        .option("header", True)
        .schema(order_items_schema)
        .csv(RAW_PATH)
    )

    df_curated = (
        df_raw
        .select(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value"
        )
        .withColumn("order_id", trim(col("order_id")))
        .withColumn("product_id", trim(col("product_id")))
        .withColumn("seller_id", trim(col("seller_id")))
        .withColumn("order_item_id", col("order_item_id").cast("int"))
        .withColumn("shipping_limit_ts", to_timestamp("shipping_limit_date"))
        .withColumn("price_amount", col("price").cast("double"))
        .withColumn("freight_value_amount", col("freight_value").cast("double"))
        .filter(col("order_id").isNotNull())
        .filter(col("order_item_id").isNotNull())
        .filter(col("product_id").isNotNull())
        .filter(col("price_amount").isNotNull())
        .filter(col("price_amount") >= 0)
        .filter(col("freight_value_amount").isNotNull())
        .filter(col("freight_value_amount") >= 0)
        .withColumn(
            "line_total_amount",
            spark_round(col("price_amount") + col("freight_value_amount"), 2)
        )
        .drop(
            "shipping_limit_date",
            "price",
            "freight_value"
        )
        .dropDuplicates(["order_id", "order_item_id"])
    )

    print("=== CURATED ORDER ITEMS SCHEMA ===")
    df_curated.printSchema()

    print("=== CURATED ORDER ITEMS SAMPLE ===")
    df_curated.show(10, truncate=False)

    (
        df_curated.write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )

    print(f"Curated order items written to: {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()