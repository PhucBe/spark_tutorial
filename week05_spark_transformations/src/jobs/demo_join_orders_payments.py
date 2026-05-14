from pyspark.sql.functions import (
    col,
    countDistinct,
    count,
    sum as spark_sum,
    max as spark_max,
    round as spark_round,
    when
)

from src.common.spark_session import create_spark_session


ORDERS_PATH = "data/curated/stg_orders"
PAYMENTS_PATH = "data/curated/stg_payments"
OUTPUT_PATH = "data/curated/daily_orders_payments_summary"


def main():
    spark = create_spark_session("demo-join-orders-payments")

    # 1) Đọc staging outputs từ Bài 1 và Bài 2
    stg_orders = spark.read.parquet(ORDERS_PATH)
    stg_payments = spark.read.parquet(PAYMENTS_PATH)

    print("=== STG ORDERS SCHEMA ===")
    stg_orders.printSchema()

    print("=== STG PAYMENTS SCHEMA ===")
    stg_payments.printSchema()

    # 2) Aggregate payments về order grain trước
    # Vì 1 order có thể có nhiều payment records.
    payments_by_order = (
        stg_payments
        .groupBy("order_id")
        .agg(
            count("*").alias("payment_record_count"),
            spark_sum("payment_value_amount").alias("payment_total_value"),
            spark_max("is_high_value_payment").alias("has_high_value_payment")
        )
        .withColumn(
            "payment_total_value",
            spark_round(col("payment_total_value"), 2)
        )
    )

    print("=== PAYMENTS BY ORDER SAMPLE ===")
    payments_by_order.show(10, truncate=False)

    # 3) Join orders với payments_by_order
    orders_with_payments = (
        stg_orders
        .join(payments_by_order, on="order_id", how="left")
        .fillna({
            "payment_record_count": 0,
            "payment_total_value": 0.0,
            "has_high_value_payment": 0
        })
        .withColumn(
            "has_payment",
            when(col("payment_record_count") > 0, 1).otherwise(0)
        )
    )

    print("=== JOINED SAMPLE ===")
    orders_with_payments.show(10, truncate=False)

    # 4) Bài 6: Tạo bảng thống kê theo event_date
    daily_summary = (
        orders_with_payments
        .groupBy("event_date")
        .agg(
            countDistinct("order_id").alias("order_count"),
            countDistinct("customer_id").alias("unique_customer_count"),
            spark_sum("has_payment").alias("orders_with_payment"),
            spark_sum("payment_total_value").alias("total_payment_value"),
            spark_sum("is_delivered").alias("delivered_order_count"),
            spark_sum("has_high_value_payment").alias("high_value_payment_order_count")
        )
        .withColumn(
            "payment_coverage_ratio",
            when(col("order_count") > 0, col("orders_with_payment") / col("order_count"))
            .otherwise(0.0)
        )
        .withColumn("total_payment_value", spark_round(col("total_payment_value"), 2))
        .withColumn("payment_coverage_ratio", spark_round(col("payment_coverage_ratio"), 4))
        .orderBy("event_date")
    )

    print("=== DAILY ORDERS PAYMENTS SUMMARY ===")
    daily_summary.show(30, truncate=False)

    # 5) Ghi output để dùng tiếp cho Tuần 6
    (
        daily_summary.write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )

    spark.stop()


if __name__ == "__main__":
    main()