from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    countDistinct,
    sum as spark_sum,
    row_number,
    year,
    month,
    round as spark_round,
)

from src.common.spark_session import create_spark_session


ORDERS_PATH = "data/curated/stg_orders"
ORDER_ITEMS_PATH = "data/curated/stg_order_items"
OUTPUT_PATH = "data/gold/top_products_by_day"


def main() -> None:
    spark = create_spark_session("build-top-products-by-day")

    orders = spark.read.parquet(ORDERS_PATH)
    order_items = spark.read.parquet(ORDER_ITEMS_PATH)

    daily_product_sales = (
        order_items
        .join(
            orders.select("order_id", "event_date"),
            on="order_id",
            how="inner",
        )
        .groupBy("event_date", "product_id")
        .agg(
            countDistinct("order_id").alias("order_count"),
            spark_sum("price_amount").alias("product_revenue"),
        )
        .withColumn(
            "product_revenue",
            spark_round(col("product_revenue"), 2),
        )
    )

    window_spec = (
        Window
        .partitionBy("event_date")
        .orderBy(col("product_revenue").desc())
    )

    top_products_by_day = (
        daily_product_sales
        .withColumn("rank_in_day", row_number().over(window_spec))
        .filter(col("rank_in_day") <= 10)
        .withColumn("summary_year", year("event_date"))
        .withColumn("summary_month", month("event_date"))
    )

    top_products_by_day.orderBy("event_date", "rank_in_day").show(
        50,
        truncate=False,
    )

    (
        top_products_by_day.write
        .mode("overwrite")
        .partitionBy("summary_year", "summary_month")
        .parquet(OUTPUT_PATH)
    )

    spark.stop()


if __name__ == "__main__":
    main()