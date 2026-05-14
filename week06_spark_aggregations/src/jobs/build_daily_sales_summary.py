from pyspark.sql.functions import (
    col,
    countDistinct,
    approx_count_distinct,
    sum as spark_sum,
    avg,
    max as spark_max,
    when,
    year,
    month,
    round as spark_round,
)

from src.common.spark_session import create_spark_session


ORDERS_PATH = "data/curated/stg_orders"
PAYMENTS_PATH = "data/curated/stg_payments"
OUTPUT_PATH = "data/gold/daily_sales_summary"


def main():
    spark = create_spark_session("build-daily-sales-summary")

    stg_orders = spark.read.parquet(ORDERS_PATH)
    stg_payments = spark.read.parquet(PAYMENTS_PATH)

    # Bước 1: Aggregate payments về grain 1 dòng / 1 order
    payments_by_order = (
        stg_payments
        .groupBy("order_id")
        .agg(
            spark_sum("payment_value_amount").alias("payment_total_value"),
            countDistinct("payment_sequential_int").alias("payment_count"),
            spark_max(
                when(col("payment_value_amount") > 0, 1).otherwise(0)
            ).alias("has_payment")
        )
    )

    # Bước 2: Join orders với payments đã aggregate
    orders_enriched = (
        stg_orders
        .join(payments_by_order, on="order_id", how="left")
        .fillna(
            {
                "payment_total_value": 0.0,
                "payment_count": 0,
                "has_payment": 0
            }
        )
    )

    # Bước 3: Build daily aggregate
    daily_sales_summary = (
        orders_enriched
        .groupBy("event_date")
        .agg(
            countDistinct("order_id").alias("order_count"),

            # Bài 4: countDistinct chính xác
            countDistinct("customer_id").alias("unique_customer_count"),

            # Bài 4: approx_count_distinct xấp xỉ
            approx_count_distinct("customer_id").alias("approx_unique_customer_count"),

            spark_sum("payment_total_value").alias("total_revenue"),
            avg("payment_total_value").alias("avg_revenue_per_order"),
            spark_sum("has_payment").alias("orders_with_payment"),
            spark_sum("is_delivered").alias("delivered_order_count"),
        )

        # Bài 5: payment coverage ratio
        .withColumn(
            "payment_coverage_ratio",
            when(
                col("order_count") > 0,
                col("orders_with_payment") / col("order_count")
            ).otherwise(0.0)
        )

        # Bài 5: delivered order ratio
        .withColumn(
            "delivered_order_ratio",
            when(
                col("order_count") > 0,
                col("delivered_order_count") / col("order_count")
            ).otherwise(0.0)
        )

        # Làm tròn số cho dễ đọc
        .withColumn("total_revenue", spark_round(col("total_revenue"), 2))
        .withColumn("avg_revenue_per_order", spark_round(col("avg_revenue_per_order"), 2))
        .withColumn("payment_coverage_ratio", spark_round(col("payment_coverage_ratio"), 4))
        .withColumn("delivered_order_ratio", spark_round(col("delivered_order_ratio"), 4))

        # Cột phục vụ partition nhẹ theo năm/tháng
        .withColumn("summary_year", year("event_date"))
        .withColumn("summary_month", month("event_date"))
    )

    print("=== DAILY SALES SUMMARY SCHEMA ===")
    daily_sales_summary.printSchema()

    print("=== DAILY SALES SUMMARY SAMPLE ===")
    daily_sales_summary.orderBy("event_date").show(30, truncate=False)

    print("=== COMPARE COUNT DISTINCT VS APPROX COUNT DISTINCT ===")
    (
        daily_sales_summary
        .select(
            "event_date",
            "unique_customer_count",
            "approx_unique_customer_count"
        )
        .orderBy("event_date")
        .show(30, truncate=False)
    )

    print("=== CHECK RATIO COLUMNS ===")
    (
        daily_sales_summary
        .select(
            "event_date",
            "order_count",
            "orders_with_payment",
            "payment_coverage_ratio",
            "delivered_order_count",
            "delivered_order_ratio"
        )
        .orderBy("event_date")
        .show(30, truncate=False)
    )

    (
        daily_sales_summary.write
        .mode("overwrite")
        .partitionBy("summary_year", "summary_month")
        .parquet(OUTPUT_PATH)
    )

    spark.stop()


if __name__ == "__main__":
    main()