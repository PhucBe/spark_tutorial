from pyspark.sql.functions import col, to_timestamp, to_date

from src.common.spark_session import create_spark_session
from src.schemas.orders_schema import orders_schema


RAW_PATH = "data/raw/olist_orders_dataset.csv"
OUTPUT_PATH = "data/curated/orders_spark"


def main():
    spark = create_spark_session("spark-orders-flow")

    df = (
        spark.read
        .option("header", True)
        .schema(orders_schema)
        .csv(RAW_PATH)
    )

    df_clean = (
        df.select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp"
        )
        .filter(col("order_id").isNotNull())
        .filter(col("customer_id").isNotNull())
        .withColumn("order_purchase_ts", to_timestamp("order_purchase_timestamp"))
        .filter(col("order_purchase_ts").isNotNull())
        .withColumn("order_date", to_date("order_purchase_ts"))
        .drop("order_purchase_timestamp")
    )

    df_clean.write.mode("overwrite").parquet(OUTPUT_PATH)

    print(f"Spark output written to: {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()