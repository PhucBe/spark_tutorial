from pyspark.sql.functions import (
    col,
    current_timestamp,
    to_date,
    hour,
)

from src.common.spark_session import create_spark_session


BOOTSTRAP_SERVERS = "localhost:9092"

ORDER_TOPIC = "ecom.order_created.v1"
PAYMENT_TOPIC = "ecom.payment_confirmed.v1"

ORDER_RAW_PATH = "data/raw_landing/order_created"
PAYMENT_RAW_PATH = "data/raw_landing/payment_confirmed"

ORDER_CHECKPOINT = "data/checkpoints/raw_order_created"
PAYMENT_CHECKPOINT = "data/checkpoints/raw_payment_confirmed"


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


def build_raw_landing_df(kafka_df):
    return (
        kafka_df
        .select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_ingest_ts"),
            col("key").cast("string").alias("message_key"),
            col("value").cast("string").alias("raw_json"),
        )
        .withColumn("processed_at", current_timestamp())
        .withColumn("ingest_date", to_date(col("processed_at")))
        .withColumn("ingest_hour", hour(col("processed_at")))
    )


def main() -> None:
    spark = create_spark_session("project08-raw-landing")

    raw_orders = build_raw_landing_df(read_kafka_stream(spark, ORDER_TOPIC))
    raw_payments = build_raw_landing_df(read_kafka_stream(spark, PAYMENT_TOPIC))

    order_query = (
        raw_orders.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", ORDER_RAW_PATH)
        .option("checkpointLocation", ORDER_CHECKPOINT)
        .partitionBy("ingest_date", "ingest_hour")
        .trigger(processingTime="10 seconds")
        .start()
    )

    payment_query = (
        raw_payments.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", PAYMENT_RAW_PATH)
        .option("checkpointLocation", PAYMENT_CHECKPOINT)
        .partitionBy("ingest_date", "ingest_hour")
        .trigger(processingTime="10 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()