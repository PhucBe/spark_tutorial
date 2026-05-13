from pyspark.sql.functions import (
    col,
    trim,
    lower,
    to_timestamp,
    to_date,
    when,
)

from src.common.spark_session import create_spark_session
from src.schemas.orders_schema import orders_schema


RAW_PATH = "data/raw/olist_orders_dataset.csv"
OUTPUT_PATH = "data/curated/stg_orders"


VALID_ORDER_STATUSES = [
    "created",
    "approved",
    "invoiced",
    "processing",
    "shipped",
    "delivered",
    "canceled",
    "unavailable",
]


def main():
    spark = create_spark_session("week05-transform-orders")

    df_raw = (
        spark.read
        .option("header", True)
        .schema(orders_schema)
        .csv(RAW_PATH)
    )

    df = df_raw.select(
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    )

    df = (
        df
        .withColumn("order_id", trim(col("order_id")))
        .withColumn("customer_id", trim(col("customer_id")))
        .withColumn("order_status", lower(trim(col("order_status"))))
    )

    df = (
        df
        .withColumn("order_purchase_ts", to_timestamp(col("order_purchase_timestamp")))
        .withColumn("order_approved_ts", to_timestamp(col("order_approved_at")))
        .withColumn("order_delivered_customer_ts", to_timestamp(col("order_delivered_customer_date")))
        .withColumn("order_estimated_delivery_ts", to_timestamp(col("order_estimated_delivery_date")))
    )

    df = (
        df
        .filter(col("order_id").isNotNull())
        .filter(col("customer_id").isNotNull())
        .filter(col("order_purchase_ts").isNotNull())
    )

    df = df.withColumn(
        "order_status_normalized",
        when(col("order_status").isin(VALID_ORDER_STATUSES), col("order_status"))
        .otherwise("unknown")
    )

    df = (
        df
        .withColumn("event_date", to_date(col("order_purchase_ts")))
        .withColumn(
            "is_delivered",
            when(col("order_status_normalized") == "delivered", 1).otherwise(0)
        )
        .withColumn(
            "is_status_valid",
            when(col("order_status_normalized") != "unknown", 1).otherwise(0)
        )
    )

    df = df.dropDuplicates(["order_id"])

    df = df.drop(
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "order_status",
    )

    print("=== STG ORDERS SCHEMA ===")
    df.printSchema()

    print("=== STG ORDERS SAMPLE ===")
    df.show(10, truncate=False)

    (
        df.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(OUTPUT_PATH)
    )

    spark.stop()


if __name__ == "__main__":
    main()