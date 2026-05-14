from pyspark.sql.functions import (
    col,
    trim,
    lower,
    upper,
    when,
)

from src.common.spark_session import create_spark_session
from src.schemas.customers_schema import customers_schema


RAW_PATH = "data/raw/olist_customers_dataset.csv"
OUTPUT_PATH = "data/curated/stg_customers"


def main():
    spark = create_spark_session("transform-customers")

    df_raw = (
        spark.read
        .option("header", True)
        .schema(customers_schema)
        .csv(RAW_PATH)
    )

    # 1) Chọn cột cần dùng
    df = df_raw.select(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    )

    # 2) Chuẩn hóa text
    df = (
        df
        .withColumn("customer_id", trim(col("customer_id")))
        .withColumn("customer_unique_id", trim(col("customer_unique_id")))
        .withColumn("customer_zip_code_prefix", trim(col("customer_zip_code_prefix")))
        .withColumn("customer_city", lower(trim(col("customer_city"))))
        .withColumn("customer_state", upper(trim(col("customer_state"))))
    )

    # 3) Loại lỗi key cơ bản
    df = (
        df
        .filter(col("customer_id").isNotNull())
        .filter(col("customer_unique_id").isNotNull())
    )

    # 4) Tạo quality flags
    df = (
        df
        .withColumn(
            "is_customer_city_valid",
            when(col("customer_city").isNotNull() & (col("customer_city") != ""), 1)
            .otherwise(0)
        )
        .withColumn(
            "is_customer_state_valid",
            when(col("customer_state").isNotNull() & (col("customer_state") != ""), 1)
            .otherwise(0)
        )
    )

    # 5) Dedup theo customer_id
    # Grain của stg_customers: 1 dòng / 1 customer_id
    df = df.dropDuplicates(["customer_id"])

    print("=== STG CUSTOMERS SCHEMA ===")
    df.printSchema()

    print("=== STG CUSTOMERS SAMPLE ===")
    df.show(10, truncate=False)

    (
        df.write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )

    spark.stop()


if __name__ == "__main__":
    main()