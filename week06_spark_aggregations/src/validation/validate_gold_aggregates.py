from pyspark.sql.functions import (
    col,
    sum as spark_sum,
)

from src.common.spark_session import create_spark_session


DAILY_PATH = "data/gold/daily_sales_summary"
HOURLY_PATH = "data/gold/hourly_order_summary"


def assert_zero(df, condition, check_name: str) -> None:
    failed_count = df.filter(condition).count()

    if failed_count > 0:
        raise ValueError(f"[FAILED] {check_name}: {failed_count} bad rows")

    print(f"[PASSED] {check_name}")


def main() -> None:
    spark = create_spark_session("validate-gold-aggregates")

    daily = spark.read.parquet(DAILY_PATH)
    hourly = spark.read.parquet(HOURLY_PATH)

    assert_zero(
        daily,
        col("event_date").isNull(),
        "daily event_date must not be null",
    )

    assert_zero(
        daily,
        col("order_count") <= 0,
        "daily order_count must be greater than 0",
    )

    assert_zero(
        daily,
        col("total_revenue") < 0,
        "daily total_revenue must be non-negative",
    )

    assert_zero(
        daily,
        col("orders_with_payment") > col("order_count"),
        "orders_with_payment must be <= order_count",
    )

    assert_zero(
        daily,
        col("delivered_order_count") > col("order_count"),
        "delivered_order_count must be <= order_count",
    )

    hourly_reconciled = (
        hourly
        .groupBy("event_date")
        .agg(
            spark_sum("order_count").alias("hourly_order_count")
        )
    )

    daily_vs_hourly = (
        daily
        .select("event_date", "order_count")
        .join(hourly_reconciled, on="event_date", how="inner")
        .withColumn(
            "order_count_diff",
            col("order_count") - col("hourly_order_count"),
        )
    )

    assert_zero(
        daily_vs_hourly,
        col("order_count_diff") != 0,
        "daily order_count must match sum of hourly order_count",
    )

    print("=== DAILY VS HOURLY RECONCILIATION SAMPLE ===")
    daily_vs_hourly.orderBy("event_date").show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()