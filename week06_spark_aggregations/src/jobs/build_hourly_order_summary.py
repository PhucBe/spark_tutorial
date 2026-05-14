from pyspark.sql.functions import (
    col,
    countDistinct,
    approx_count_distinct,
    date_trunc,
    to_date,
    hour,
    year,
    month,
)

from src.common.spark_session import create_spark_session


ORDERS_PATH = "data/curated/stg_orders"
OUTPUT_PATH = "data/gold/hourly_order_summary"


def main() -> None:
    spark = create_spark_session("build-hourly-order-summary")

    stg_orders = spark.read.parquet(ORDERS_PATH)

    hourly_order_summary = (
        stg_orders
        .filter(col("order_purchase_ts").isNotNull())
        .withColumn("order_hour_ts", date_trunc("hour", col("order_purchase_ts")))
        .withColumn("event_date", to_date(col("order_hour_ts")))
        .withColumn("order_hour", hour(col("order_hour_ts")))
        .groupBy("order_hour_ts", "event_date", "order_hour")
        .agg(
            countDistinct("order_id").alias("order_count"),
            countDistinct("customer_id").alias("unique_customer_count"),
            approx_count_distinct("customer_id").alias(
                "approx_unique_customer_count"
            ),
        )
        .withColumn("summary_year", year("event_date"))
        .withColumn("summary_month", month("event_date"))
    )

    print("=== HOURLY ORDER SUMMARY SCHEMA ===")
    hourly_order_summary.printSchema()

    print("=== HOURLY ORDER SUMMARY SAMPLE ===")
    hourly_order_summary.orderBy("order_hour_ts").show(24, truncate=False)

    (
        hourly_order_summary.write
        .mode("overwrite")
        .partitionBy("summary_year", "summary_month")
        .parquet(OUTPUT_PATH)
    )

    spark.stop()


if __name__ == "__main__":
    main()