from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    count,
    approx_count_distinct,
    sum as spark_sum,
    avg,
    current_timestamp,
)

from src.common.spark_session import create_spark_session
from src.schemas.streaming_schemas import (
    order_created_schema,
    payment_confirmed_schema,
)


BOOTSTRAP_SERVERS = "localhost:9092"

ORDER_TOPIC = "ecom.order_created.v1"
PAYMENT_TOPIC = "ecom.payment_confirmed.v1"

ORDER_OUTPUT = "data/curated/fact_realtime_orders_5m"
PAYMENT_OUTPUT = "data/curated/fact_payment_status_5m"

ORDER_CHECKPOINT = "data/checkpoints/realtime_orders_5m"
PAYMENT_CHECKPOINT = "data/checkpoints/payment_status_5m"


def read_kafka_stream(spark, topic_name: str):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


def build_order_stream(spark):
    raw_orders = read_kafka_stream(spark, ORDER_TOPIC)

    return (
        raw_orders
        .select(
            col("key").cast("string").alias("message_key"),
            col("value").cast("string").alias("raw_json"),
            col("timestamp").alias("kafka_ingest_ts"),
        )
        .select(
            col("message_key"),
            col("kafka_ingest_ts"),
            from_json(col("raw_json"), order_created_schema).alias("payload"),
        )
        .select(
            col("message_key"),
            col("kafka_ingest_ts"),
            col("payload.order_id").alias("order_id"),
            col("payload.customer_id").alias("customer_id"),
            col("payload.event_type").alias("event_type"),
            col("payload.event_time").alias("event_time"),
        )
        .filter(col("order_id").isNotNull())
        .filter(col("customer_id").isNotNull())
        .filter(col("event_type") == "order_created")
        .withColumn("event_ts", to_timestamp("event_time"))
        .filter(col("event_ts").isNotNull())
        .withWatermark("event_ts", "20 minutes")
        .dropDuplicates(["order_id", "event_ts"])
    )


def build_payment_stream(spark):
    raw_payments = read_kafka_stream(spark, PAYMENT_TOPIC)

    return (
        raw_payments
        .select(
            col("key").cast("string").alias("message_key"),
            col("value").cast("string").alias("raw_json"),
            col("timestamp").alias("kafka_ingest_ts"),
        )
        .select(
            col("message_key"),
            col("kafka_ingest_ts"),
            from_json(col("raw_json"), payment_confirmed_schema).alias("payload"),
        )
        .select(
            col("message_key"),
            col("kafka_ingest_ts"),
            col("payload.order_id").alias("order_id"),
            col("payload.payment_id").alias("payment_id"),
            col("payload.event_type").alias("event_type"),
            col("payload.event_time").alias("event_time"),
            col("payload.payment_value").alias("payment_value"),
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


def main() -> None:
    spark = create_spark_session("project08-curated-metrics")

    orders_stream = build_order_stream(spark)
    payments_stream = build_payment_stream(spark)

    fact_realtime_orders_5m = (
        orders_stream
        .groupBy(window(col("event_ts"), "5 minutes"))
        .agg(
            count("*").alias("order_created_count"),
            approx_count_distinct("customer_id").alias("unique_customer_count"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("order_created_count"),
            col("unique_customer_count"),
            current_timestamp().alias("processed_at"),
        )
    )

    fact_payment_status_5m = (
        payments_stream
        .groupBy(window(col("event_ts"), "5 minutes"))
        .agg(
            count("*").alias("payment_confirmed_count"),
            spark_sum("payment_value").alias("payment_revenue"),
            avg("payment_value").alias("avg_payment_value"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("payment_confirmed_count"),
            col("payment_revenue"),
            col("avg_payment_value"),
            current_timestamp().alias("processed_at"),
        )
    )

    order_query = (
        fact_realtime_orders_5m.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", ORDER_OUTPUT)
        .option("checkpointLocation", ORDER_CHECKPOINT)
        .trigger(processingTime="10 seconds")
        .start()
    )

    payment_query = (
        fact_payment_status_5m.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", PAYMENT_OUTPUT)
        .option("checkpointLocation", PAYMENT_CHECKPOINT)
        .trigger(processingTime="10 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()