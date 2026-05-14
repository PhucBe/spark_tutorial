from pyspark.sql.functions import (
    col,
    trim,
    to_timestamp,
    when,
    round as spark_round,
)

from src.common.spark_session import create_spark_session
from src.schemas.order_items_schema import order_items_schema


RAW_PATH = "data/raw/olist_order_items_dataset.csv"
OUTPUT_PATH = "data/curated/stg_order_items"


def main():
    spark = create_spark_session("transform-order-items")

    df_raw = (
        spark.read
        .option("header", True)
        .schema(order_items_schema)
        .csv(RAW_PATH)
    )

    # 1) Chọn các cột cần dùng
    df = df_raw.select(
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
    )

    # 2) Chuẩn hóa text/id
    df = (
        df
        .withColumn("order_id", trim(col("order_id")))
        .withColumn("product_id", trim(col("product_id")))
        .withColumn("seller_id", trim(col("seller_id")))
    )

    # 3) Cast kiểu dữ liệu
    df = (
        df
        .withColumn("order_item_id_int", col("order_item_id").cast("int"))
        .withColumn("shipping_limit_ts", to_timestamp(col("shipping_limit_date")))
        .withColumn("price_amount", col("price").cast("double"))
        .withColumn("freight_value_amount", col("freight_value").cast("double"))
    )

    # 4) Loại lỗi key cơ bản
    df = (
        df
        .filter(col("order_id").isNotNull())
        .filter(col("order_item_id_int").isNotNull())
        .filter(col("product_id").isNotNull())
        .filter(col("seller_id").isNotNull())
    )

    # 5) Tạo quality flags cho amount
    df = (
        df
        .withColumn(
            "is_valid_price",
            when(col("price_amount").isNotNull() & (col("price_amount") >= 0), 1)
            .otherwise(0)
        )
        .withColumn(
            "is_valid_freight",
            when(col("freight_value_amount").isNotNull() & (col("freight_value_amount") >= 0), 1)
            .otherwise(0)
        )
    )

    # 6) Với staging phục vụ aggregation, chỉ giữ record có amount hợp lệ
    df = (
        df
        .filter(col("is_valid_price") == 1)
        .filter(col("is_valid_freight") == 1)
    )

    # 7) Tạo metric line amount
    df = (
        df
        .withColumn(
            "item_total_amount",
            spark_round(col("price_amount") + col("freight_value_amount"), 2)
        )
    )

    # 8) Dedup theo business key
    # Grain của stg_order_items: 1 dòng / 1 order_id / 1 order_item_id
    df = df.dropDuplicates(["order_id", "order_item_id_int"])

    # 9) Drop raw columns không cần giữ nữa
    df = df.drop(
        "order_item_id",
        "shipping_limit_date",
        "price",
        "freight_value",
    )

    print("=== STG ORDER ITEMS SCHEMA ===")
    df.printSchema()

    print("=== STG ORDER ITEMS SAMPLE ===")
    df.show(10, truncate=False)

    (
        df.write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )

    spark.stop()


if __name__ == "__main__":
    main()