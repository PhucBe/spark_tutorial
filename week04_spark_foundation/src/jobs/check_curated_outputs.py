from src.common.spark_session import create_spark_session


ORDERS_PATH = "data/curated/orders"
ORDER_ITEMS_PATH = "data/curated/order_items"
ORDER_EVENTS_PATH = "data/curated/order_created_events"


def check_dataset(spark, name: str, path: str):
    print(f"\n=== CHECKING {name} ===")

    df = spark.read.parquet(path)

    print("Schema:")
    df.printSchema()

    print("Row count:")
    print(df.count())

    print("Sample:")
    df.show(10, truncate=False)


def main():
    spark = create_spark_session("check-curated-outputs")

    check_dataset(spark, "curated_orders", ORDERS_PATH)
    check_dataset(spark, "curated_order_items", ORDER_ITEMS_PATH)

    # Chỉ chạy dòng này nếu bạn đã có raw event từ Project 07
    # check_dataset(spark, "curated_order_created_events", ORDER_EVENTS_PATH)

    spark.stop()


if __name__ == "__main__":
    main()