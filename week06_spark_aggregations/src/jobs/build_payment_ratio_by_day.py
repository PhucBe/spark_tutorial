from pyspark.sql.functions import (
    col,
    countDistinct,
    when,
    year,
    month,
    round as spark_round,
)

from src.common.spark_session import create_spark_session


ORDERS_PATH = "data/curated/stg_orders"
PAYMENTS_PATH = "data/curated/stg_payments"
OUTPUT_PATH = "data/gold/payment_ratio_by_day"


def main() -> None:
    spark = create_spark_session("build-payment-ratio-by-day")

    orders = spark.read.parquet(ORDERS_PATH)
    payments = spark.read.parquet(PAYMENTS_PATH)

    payments_by_order = (
        payments
        .groupBy("order_id")
        .agg(
            countDistinct("payment_sequential_int").alias("payment_count")
        )
        .withColumn(
            "has_payment",
            when(col("payment_count") > 0, 1).otherwise(0),
        )
    )

    payment_ratio_by_day = (
        orders
        .join(
            payments_by_order.select("order_id", "has_payment"),
            on="order_id",
            how="left",
        )
        .fillna({"has_payment": 0})
        .groupBy("event_date")
        .agg(
            countDistinct("order_id").alias("order_count"),
            countDistinct(
                when(col("has_payment") == 1, col("order_id"))
            ).alias("orders_with_payment"),
        )
        .withColumn(
            "payment_coverage_ratio",
            when(
                col("order_count") > 0,
                col("orders_with_payment") / col("order_count"),
            ).otherwise(0.0),
        )
        .withColumn(
            "payment_coverage_ratio",
            spark_round(col("payment_coverage_ratio"), 4),
        )
        .withColumn("summary_year", year("event_date"))
        .withColumn("summary_month", month("event_date"))
    )

    payment_ratio_by_day.orderBy("event_date").show(50, truncate=False)

    (
        payment_ratio_by_day.write
        .mode("overwrite")
        .partitionBy("summary_year", "summary_month")
        .parquet(OUTPUT_PATH)
    )

    spark.stop()


if __name__ == "__main__":
    main()