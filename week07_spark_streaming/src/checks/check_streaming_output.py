import os
from src.common.spark_session import create_spark_session


ORDER_OUTPUT_PATH = "data/streaming_output/order_count_10m"
PAYMENT_OUTPUT_PATH = "data/streaming_output/payment_revenue_5m"


def read_if_exists(spark, path: str, title: str):
    print(f"\n=== {title} ===")

    if not os.path.exists(path):
        print(f"Path does not exist: {path}")
        return

    try:
        df = spark.read.parquet(path)
        df.orderBy("window_start").show(50, truncate=False)
    except Exception as e:
        print(f"Cannot read parquet yet: {e}")


def main():
    spark = create_spark_session("check-streaming-output")

    read_if_exists(
        spark,
        ORDER_OUTPUT_PATH,
        "ORDER COUNT 10M"
    )

    read_if_exists(
        spark,
        PAYMENT_OUTPUT_PATH,
        "PAYMENT REVENUE 5M"
    )

    spark.stop()


if __name__ == "__main__":
    main()