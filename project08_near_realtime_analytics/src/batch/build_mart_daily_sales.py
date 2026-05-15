from pyspark.sql.functions import (
    col,
    to_date,
    sum as spark_sum,
    when,
    current_timestamp,
    round as spark_round,
)

from src.common.spark_session import create_spark_session


ORDERS_5M_PATH = "data/curated/fact_realtime_orders_5m"
PAYMENTS_5M_PATH = "data/curated/fact_payment_status_5m"
OUTPUT_PATH = "data/serving/mart_daily_sales"


def main() -> None:
    spark = create_spark_session("build-mart-daily-sales")

    orders_5m = spark.read.parquet(ORDERS_5M_PATH)
    payments_5m = spark.read.parquet(PAYMENTS_5M_PATH)

    orders_daily = (
        orders_5m
        .withColumn("sales_date", to_date("window_start"))
        .groupBy("sales_date")
        .agg(
            spark_sum("order_created_count").alias("order_count"),
            spark_sum("unique_customer_count").alias("unique_customer_count"),
        )
    )

    payments_daily = (
        payments_5m
        .withColumn("sales_date", to_date("window_start"))
        .groupBy("sales_date")
        .agg(
            spark_sum("payment_confirmed_count").alias("payment_confirmed_count"),
            spark_sum("payment_revenue").alias("total_revenue"),
        )
    )

    mart = (
        orders_daily
        .join(payments_daily, on="sales_date", how="left")
        .fillna({
            "payment_confirmed_count": 0,
            "total_revenue": 0.0,
        })
        .withColumn(
            "avg_order_value",
            when(col("order_count") > 0, col("total_revenue") / col("order_count"))
            .otherwise(0.0),
        )
        .withColumn("total_revenue", spark_round(col("total_revenue"), 2))
        .withColumn("avg_order_value", spark_round(col("avg_order_value"), 2))
        .withColumn("source_refresh_ts", current_timestamp())
    )

    print("=== MART DAILY SALES ===")
    mart.orderBy("sales_date").show(50, truncate=False)

    (
        mart.write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )

    spark.stop()


if __name__ == "__main__":
    main()