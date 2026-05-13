from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
)


events_schema = StructType([
    StructField("topic", StringType(), True),
    StructField("partition", LongType(), True),
    StructField("offset", LongType(), True),
    StructField("key", StringType(), True),
    StructField(
        "value",
        StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_time", StringType(), True),
            StructField("payment_id", StringType(), True),
            StructField("payment_value", DoubleType(), True),
            StructField("delivered_customer_date", StringType(), True),
            StructField("invalid_reason", StringType(), True),
        ]),
        True,
    ),
])