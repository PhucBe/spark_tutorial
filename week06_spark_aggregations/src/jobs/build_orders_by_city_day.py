from pyspark.sql.functions import (
    col,
    countDistinct,
    year,
    month,
)

from src.common.spark_session import create_spark_session


ORDERS_PATH = "data/curated/stg_orders"
CUSTOMERS_PATH = "data/curated/stg_customers"
OUTPUT_PATH = "data/gold/orders_by_city_day"


def main() -> None:
    spark = create_spark_session("build-orders-by-city-day")

    orders = spark.read.parquet(ORDERS_PATH)
    customers = spark.read.parquet(CUSTOMERS_PATH)

    orders_by_city_day = (
        orders
        .join(
            customers.select(
                "customer_id",
                "customer_city",
                "customer_state",
            ),
            on="customer_id",
            how="left",
        )
        .fillna(
            {
                "customer_city": "unknown",
                "customer_state": "unknown",
            }
        )
        .groupBy("event_date", "customer_city", "customer_state")
        .agg(
            countDistinct("order_id").alias("order_count"),
            countDistinct("customer_id").alias("unique_customer_count"),
        )
        .withColumn("summary_year", year("event_date"))
        .withColumn("summary_month", month("event_date"))
    )

    orders_by_city_day.orderBy(
        "event_date",
        col("order_count").desc(),
    ).show(50, truncate=False)

    (
        orders_by_city_day.write
        .mode("overwrite")
        .partitionBy("summary_year", "summary_month")
        .parquet(OUTPUT_PATH)
    )

    spark.stop()


if __name__ == "__main__":
    main()