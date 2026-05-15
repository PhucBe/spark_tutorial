from pyspark.sql.functions import col, count

from src.common.spark_session import create_spark_session


MART_PATH = "data/serving/mart_daily_sales"


def main() -> None:
    spark = create_spark_session("check-serving-outputs")

    mart = spark.read.parquet(MART_PATH)

    row_count = mart.count()
    null_date_count = mart.filter(col("sales_date").isNull()).count()
    negative_revenue_count = mart.filter(col("total_revenue") < 0).count()

    duplicate_count = (
        mart
        .groupBy("sales_date")
        .agg(count("*").alias("cnt"))
        .filter(col("cnt") > 1)
        .count()
    )

    print("=== SERVING QUALITY CHECK ===")
    print(f"row_count = {row_count}")
    print(f"null_date_count = {null_date_count}")
    print(f"negative_revenue_count = {negative_revenue_count}")
    print(f"duplicate_sales_date_count = {duplicate_count}")

    if row_count <= 0:
        raise AssertionError("mart_daily_sales is empty")

    if null_date_count > 0:
        raise AssertionError("sales_date has null values")

    if negative_revenue_count > 0:
        raise AssertionError("total_revenue has negative values")

    if duplicate_count > 0:
        raise AssertionError("sales_date is duplicated")

    print("All checks passed.")

    spark.stop()


if __name__ == "__main__":
    main()