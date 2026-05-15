from pyspark.sql.types import StructType, StructField, StringType, DoubleType


order_created_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
])


payment_confirmed_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("payment_value", DoubleType(), True),
])