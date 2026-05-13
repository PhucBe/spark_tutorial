from pyspark.sql.functions import (
    col,
    countDistinct,
    sum as spark_sum,
    round as spark_round,
)

from src.common.spark_session import create_spark_session


ORDERS_PATH = "data/curated/stg_orders"
PAYMENTS_PATH = "data/curated/stg_payments"


def main():
    spark = create_spark_session("week05-demo-join-orders-payments")

    stg_orders = spark.read.parquet(ORDERS_PATH)
    stg_payments = spark.read.parquet(PAYMENTS_PATH)

    df_joined = stg_orders.join(
        stg_payments,
        on="order_id",
        how="left",
    )

    print("=== JOINED SAMPLE ===")
    df_joined.show(10, truncate=False)

    daily_summary = (
        df_joined
        .groupBy("event_date")
        .agg(
            countDistinct("order_id").alias("order_count"),
            spark_sum("payment_value_amount").alias("total_payment_value"),
        )
        .withColumn("total_payment_value", spark_round(col("total_payment_value"), 2))
        .orderBy("event_date")
    )

    print("=== DAILY PAYMENT SUMMARY ===")
    daily_summary.show(30, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()