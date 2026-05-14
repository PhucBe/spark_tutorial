from src.common.spark_session import create_spark_session


INPUT_PATH = "data/curated/daily_orders_payments_summary"


def main():
    spark = create_spark_session("check-daily-summary")

    df = spark.read.parquet(INPUT_PATH)

    print("=== DAILY SUMMARY SCHEMA ===")
    df.printSchema()

    print("=== DAILY SUMMARY SAMPLE ===")
    df.orderBy("event_date").show(50, truncate=False)

    print("=== ROW COUNT ===")
    print(df.count())

    spark.stop()


if __name__ == "__main__":
    main()