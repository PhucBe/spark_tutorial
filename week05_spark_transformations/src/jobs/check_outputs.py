from src.common.spark_session import create_spark_session


def main():
    spark = create_spark_session("week05-check-outputs")

    print("=== CHECK STG ORDERS ===")
    orders = spark.read.parquet("data/curated/stg_orders")
    orders.printSchema()
    orders.show(10, truncate=False)
    print("orders count:", orders.count())

    print("=== CHECK STG PAYMENTS ===")
    payments = spark.read.parquet("data/curated/stg_payments")
    payments.printSchema()
    payments.show(10, truncate=False)
    print("payments count:", payments.count())

    spark.stop()


if __name__ == "__main__":
    main()