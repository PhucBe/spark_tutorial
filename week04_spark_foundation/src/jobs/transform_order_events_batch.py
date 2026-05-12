from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    year,
    month
)

from src.common.spark_session import create_spark_session
from src.schemas.events_schema import event_record_schema


RAW_PATH = "data/raw/raw_events/order_created.jsonl"
OUTPUT_PATH = "data/curated/order_created_events"


def main():
    spark = create_spark_session("transform-order-events-batch")

    df_raw = (
        spark.read
        .schema(event_record_schema)
        .json(RAW_PATH)
    )

    df_curated = (
        df_raw
        .select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("key").alias("message_key"),
            col("value.order_id").alias("order_id"),
            col("value.customer_id").alias("customer_id"),
            col("value.event_type").alias("event_type"),
            col("value.event_time").alias("event_time")
        )
        .filter(col("order_id").isNotNull())
        .filter(col("event_type").isNotNull())
        .withColumn("event_ts", to_timestamp("event_time"))
        .filter(col("event_ts").isNotNull())
        .withColumn("event_date", to_date("event_ts"))
        .withColumn("event_year", year("event_ts"))
        .withColumn("event_month", month("event_ts"))
        .dropDuplicates(["message_key", "offset"])
    )

    print("=== CURATED ORDER CREATED EVENTS SCHEMA ===")
    df_curated.printSchema()

    print("=== CURATED ORDER CREATED EVENTS SAMPLE ===")
    df_curated.show(10, truncate=False)

    (
        df_curated.write
        .mode("overwrite")
        .partitionBy("event_year", "event_month")
        .parquet(OUTPUT_PATH)
    )

    print(f"Curated order events written to: {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()