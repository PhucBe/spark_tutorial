from pyspark.sql.functions import (
    col,
    trim,
    lower,
    when,
)

from src.common.spark_session import create_spark_session
from src.schemas.payments_schema import payments_schema


RAW_PATH = "data/raw/olist_order_payments_dataset.csv"
OUTPUT_PATH = "data/curated/stg_payments"


VALID_PAYMENT_TYPES = [
    "credit_card",
    "boleto",
    "voucher",
    "debit_card",
    "not_defined",
]


def main():
    spark = create_spark_session("week05-transform-payments")

    df_raw = (
        spark.read
        .option("header", True)
        .schema(payments_schema)
        .csv(RAW_PATH)
    )

    df = df_raw.select(
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
    )

    df = (
        df
        .withColumn("order_id", trim(col("order_id")))
        .withColumn("payment_type", lower(trim(col("payment_type"))))
        .withColumn("payment_sequential_int", col("payment_sequential").cast("int"))
        .withColumn("payment_installments_int", col("payment_installments").cast("int"))
        .withColumn("payment_value_amount", col("payment_value").cast("double"))
    )

    df = (
        df
        .filter(col("order_id").isNotNull())
        .filter(col("payment_value_amount").isNotNull())
        .filter(col("payment_value_amount") >= 0)
    )

    df = df.withColumn(
        "payment_type_normalized",
        when(col("payment_type").isin(VALID_PAYMENT_TYPES), col("payment_type"))
        .otherwise("unknown")
    )

    df = (
        df
        .withColumn(
            "is_payment_type_valid",
            when(col("payment_type_normalized") != "unknown", 1).otherwise(0)
        )
        .withColumn(
            "is_high_value_payment",
            when(col("payment_value_amount") >= 500, 1).otherwise(0)
        )
    )

    df = df.dropDuplicates([
        "order_id",
        "payment_sequential_int",
        "payment_type_normalized",
        "payment_value_amount",
    ])

    df = df.drop(
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
    )

    print("=== STG PAYMENTS SCHEMA ===")
    df.printSchema()

    print("=== STG PAYMENTS SAMPLE ===")
    df.show(10, truncate=False)

    (
        df.write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )

    spark.stop()


if __name__ == "__main__":
    main()