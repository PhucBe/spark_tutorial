from pyspark.sql.functions import col, to_timestamp

from src.common.spark_session import create_spark_session
from src.schemas.orders_schema import orders_schema


RAW_PATH = "data/raw/olist_orders_dataset.csv"


def main():
    spark = create_spark_session("read-orders-demo")

    df = (
        spark.read
        .option("header", True)
        .schema(orders_schema)
        .csv(RAW_PATH)
    )

    print("=== RAW SCHEMA ===")
    df.printSchema()

    print("=== RAW SAMPLE ROWS ===")
    df.show(5, truncate=False)

    print("=== RAW ROW COUNT ===")
    print(df.count())

    # Bài tập buổi 2: select, filter, withColumn, drop
    df_clean = (
        df.select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp"
        )
        .filter(col("order_id").isNotNull())
        .withColumn(
            "order_purchase_ts",
            to_timestamp("order_purchase_timestamp")
        )
        .drop("order_purchase_timestamp")
    )

    print("=== CLEAN SCHEMA ===")
    df_clean.printSchema()

    print("=== CLEAN SAMPLE ROWS ===")
    df_clean.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()