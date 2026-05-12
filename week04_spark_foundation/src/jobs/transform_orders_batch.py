from pyspark.sql.functions import (
    col,
    trim,
    lower,
    to_timestamp,
    to_date,
    year,
    month,
    when
)

from src.common.spark_session import create_spark_session
from src.schemas.orders_schema import orders_schema


RAW_PATH = "data/raw/olist_orders_dataset.csv"
OUTPUT_PATH = "data/curated/orders"

VALID_ORDER_STATUSES = [
    "created",
    "approved",
    "invoiced",
    "processing",
    "shipped",
    "delivered",
    "canceled",
    "unavailable"
]


def main():
    spark = create_spark_session("transform-orders-batch")

    df_raw = (
        spark.read
        .option("header", True)
        .schema(orders_schema)
        .csv(RAW_PATH)
    )

    df_curated = (
        df_raw
        .select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_customer_date",
            "order_estimated_delivery_date"
        )
        .withColumn("order_id", trim(col("order_id")))
        .withColumn("customer_id", trim(col("customer_id")))
        .withColumn("order_status", lower(trim(col("order_status"))))
        .filter(col("order_id").isNotNull())
        .filter(col("customer_id").isNotNull())
        .withColumn("order_purchase_ts", to_timestamp("order_purchase_timestamp"))
        .withColumn("order_approved_ts", to_timestamp("order_approved_at"))
        .withColumn("order_delivered_customer_ts", to_timestamp("order_delivered_customer_date"))
        .withColumn("order_estimated_delivery_ts", to_timestamp("order_estimated_delivery_date"))
        .filter(col("order_purchase_ts").isNotNull())
        .withColumn("order_date", to_date("order_purchase_ts"))
        .withColumn("order_year", year("order_purchase_ts"))
        .withColumn("order_month", month("order_purchase_ts"))
        .withColumn(
            "order_status_normalized",
            when(col("order_status").isin(VALID_ORDER_STATUSES), col("order_status"))
            .otherwise("unknown")
        )
        .withColumn(
            "is_delivered",
            when(col("order_status_normalized") == "delivered", 1).otherwise(0)
        )
        .withColumn(
            "is_status_valid",
            when(col("order_status_normalized") != "unknown", 1).otherwise(0)
        )
        .drop(
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_customer_date",
            "order_estimated_delivery_date"
        )
        .dropDuplicates(["order_id"])
    )

    print("=== CURATED ORDERS SCHEMA ===")
    df_curated.printSchema()

    print("=== CURATED ORDERS SAMPLE ===")
    df_curated.show(10, truncate=False)

    (
        df_curated.write
        .mode("overwrite")
        .partitionBy("order_year", "order_month")
        .parquet(OUTPUT_PATH)
    )

    print(f"Curated orders written to: {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()