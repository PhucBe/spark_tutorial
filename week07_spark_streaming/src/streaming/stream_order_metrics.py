from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    count,
    sum as spark_sum,
    current_timestamp
)

from src.common.spark_session import create_spark_session
from src.schemas.streaming_schemas import (
    order_created_schema,
    payment_confirmed_schema
)


BOOTSTRAP_SERVERS = "localhost:9092"

ORDER_CREATED_TOPIC = "ecom.order_created.v1"
PAYMENT_CONFIRMED_TOPIC = "ecom.payment_confirmed.v1"

ORDER_OUTPUT_PATH = "data/streaming_output/order_count_10m"
PAYMENT_OUTPUT_PATH = "data/streaming_output/payment_revenue_5m"

ORDER_CHECKPOINT_PATH = "data/checkpoints/order_count_10m"
PAYMENT_CHECKPOINT_PATH = "data/checkpoints/payment_revenue_5m"


def read_kafka_topic_stream(spark, topic_name: str):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


def build_order_created_stream(spark):
    raw_orders = read_kafka_topic_stream(spark, ORDER_CREATED_TOPIC)

    parsed_orders = (
        raw_orders
        .selectExpr(
            "CAST(key AS STRING) AS message_key",
            "CAST(value AS STRING) AS raw_json",
            "topic",
            "partition",
            "offset",
            "timestamp AS kafka_ingest_ts"
        )
        .select(
            col("message_key"),
            col("raw_json"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_ingest_ts"),
            from_json(col("raw_json"), order_created_schema).alias("payload")
        )
        .select(
            col("message_key"),
            col("raw_json"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_ingest_ts"),
            col("payload.order_id").alias("order_id"),
            col("payload.customer_id").alias("customer_id"),
            col("payload.event_type").alias("event_type"),
            col("payload.event_time").alias("event_time")
        )
        .filter(col("order_id").isNotNull())
        .filter(col("customer_id").isNotNull())
        .filter(col("event_type") == "order_created")
        .withColumn("event_ts", to_timestamp("event_time"))
        .filter(col("event_ts").isNotNull())
        .withWatermark("event_ts", "20 minutes")
        .dropDuplicates(["order_id", "event_ts"])
    )

    return parsed_orders


def build_payment_confirmed_stream(spark):
    raw_payments = read_kafka_topic_stream(spark, PAYMENT_CONFIRMED_TOPIC)

    parsed_payments = (
        raw_payments
        .selectExpr(
            "CAST(key AS STRING) AS message_key",
            "CAST(value AS STRING) AS raw_json",
            "topic",
            "partition",
            "offset",
            "timestamp AS kafka_ingest_ts"
        )
        .select(
            col("message_key"),
            col("raw_json"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_ingest_ts"),
            from_json(col("raw_json"), payment_confirmed_schema).alias("payload")
        )
        .select(
            col("message_key"),
            col("raw_json"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_ingest_ts"),
            col("payload.order_id").alias("order_id"),
            col("payload.payment_id").alias("payment_id"),
            col("payload.event_type").alias("event_type"),
            col("payload.event_time").alias("event_time"),
            col("payload.payment_value").alias("payment_value")
        )
        .filter(col("order_id").isNotNull())
        .filter(col("payment_id").isNotNull())
        .filter(col("event_type") == "payment_confirmed")
        .withColumn("event_ts", to_timestamp("event_time"))
        .filter(col("event_ts").isNotNull())
        .filter(col("payment_value").isNotNull())
        .filter(col("payment_value") >= 0)
        .withWatermark("event_ts", "20 minutes")
        .dropDuplicates(["payment_id", "event_ts"])
    )

    return parsed_payments


def main():
    spark = create_spark_session("week07-stream-order-metrics")

    orders_stream = build_order_created_stream(spark)
    payments_stream = build_payment_confirmed_stream(spark)

    order_count_10m = (
        orders_stream
        .groupBy(window(col("event_ts"), "10 minutes"))
        .agg(
            count("*").alias("order_created_count")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("order_created_count"),
            current_timestamp().alias("processed_at")
        )
    )

    payment_revenue_5m = (
        payments_stream
        .groupBy(window(col("event_ts"), "5 minutes"))
        .agg(
            count("*").alias("payment_confirmed_count"),
            spark_sum("payment_value").alias("payment_revenue")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("payment_confirmed_count"),
            col("payment_revenue"),
            current_timestamp().alias("processed_at")
        )
    )

    order_query = (
        order_count_10m.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", ORDER_OUTPUT_PATH)
        .option("checkpointLocation", ORDER_CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start()
    )

    payment_query = (
        payment_revenue_5m.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", PAYMENT_OUTPUT_PATH)
        .option("checkpointLocation", PAYMENT_CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()