from src.common.spark_session import create_spark_session


def main():
    spark = create_spark_session("check-staging-outputs")

    print("=== CHECK STG ORDERS ===")
    orders = spark.read.parquet("data/curated/stg_orders")
    orders.printSchema()
    orders.show(5, truncate=False)
    print("stg_orders count:", orders.count())

    print("=== CHECK STG PAYMENTS ===")
    payments = spark.read.parquet("data/curated/stg_payments")
    payments.printSchema()
    payments.show(5, truncate=False)
    print("stg_payments count:", payments.count())

    print("=== CHECK STG ORDER ITEMS ===")
    order_items = spark.read.parquet("data/curated/stg_order_items")
    order_items.printSchema()
    order_items.show(5, truncate=False)
    print("stg_order_items count:", order_items.count())

    print("=== CHECK STG CUSTOMERS ===")
    customers = spark.read.parquet("data/curated/stg_customers")
    customers.printSchema()
    customers.show(5, truncate=False)
    print("stg_customers count:", customers.count())

    spark.stop()


if __name__ == "__main__":
    main()