from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    when,
)

from src.common.spark_session import create_spark_session
from src.schemas.events_schema import events_schema


RAW_PATH = "data/raw/raw_events/order_created.jsonl"
OUTPUT_PATH = "data/curated/stg_order_events"


def main():
    spark = create_spark_session("week05-transform-order-events")

    df_raw = (
        spark.read
        .schema(events_schema)
        .json(RAW_PATH)
    )

    df = (
        df_raw
        .select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("key").alias("message_key"),
            col("value.order_id").alias("order_id"),
            col("value.customer_id").alias("customer_id"),
            col("value.event_type").alias("event_type"),
            col("value.event_time").alias("event_time"),
        )
        .filter(col("order_id").isNotNull())
        .filter(col("event_type").isNotNull())
        .withColumn("event_ts", to_timestamp(col("event_time")))
        .filter(col("event_ts").isNotNull())
        .withColumn("event_date", to_date(col("event_ts")))
        .withColumn(
            "is_order_created_event",
            when(col("event_type") == "order_created", 1).otherwise(0)
        )
        .dropDuplicates(["message_key", "offset"])
        .drop("event_time")
    )

    print("=== STG ORDER EVENTS SCHEMA ===")
    df.printSchema()

    print("=== STG ORDER EVENTS SAMPLE ===")
    df.show(10, truncate=False)

    (
        df.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(OUTPUT_PATH)
    )

    spark.stop()


if __name__ == "__main__":
    main()