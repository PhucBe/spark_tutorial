import os
import shutil

from src.common.spark_session import create_spark_session


INPUT_PATH = "data/gold/daily_sales_summary"

OUTPUT_BY_EVENT_DATE = "data/gold/_compare_daily_sales_by_event_date"
OUTPUT_BY_YEAR_MONTH = "data/gold/_compare_daily_sales_by_year_month"


def remove_path_if_exists(path: str) -> None:
    if os.path.exists(path):
        shutil.rmtree(path)


def count_parquet_files(path: str) -> int:
    total = 0

    for root, _, files in os.walk(path):
        for file in files:
            if file.endswith(".parquet"):
                total += 1

    return total


def count_folders(path: str) -> int:
    total = 0

    for _, dirs, _ in os.walk(path):
        total += len(dirs)

    return total


def print_tree_level_2(path: str) -> None:
    print(f"\n=== FOLDER PREVIEW: {path} ===")

    for root, dirs, files in os.walk(path):
        level = root.replace(path, "").count(os.sep)

        if level > 2:
            continue

        indent = "  " * level
        print(f"{indent}{os.path.basename(root)}/")

        sub_indent = "  " * (level + 1)
        parquet_files = [f for f in files if f.endswith(".parquet")]

        for file in parquet_files[:3]:
            print(f"{sub_indent}{file}")

        if len(parquet_files) > 3:
            print(f"{sub_indent}... {len(parquet_files) - 3} more parquet files")


def main():
    spark = create_spark_session("compare-daily-sales-partitioning")

    daily_sales_summary = spark.read.parquet(INPUT_PATH)

    print("=== INPUT DAILY SALES SUMMARY ===")
    daily_sales_summary.printSchema()
    daily_sales_summary.orderBy("event_date").show(10, truncate=False)

    remove_path_if_exists(OUTPUT_BY_EVENT_DATE)
    remove_path_if_exists(OUTPUT_BY_YEAR_MONTH)

    print("\n=== WRITE VERSION 1: partitionBy(event_date) ===")
    (
        daily_sales_summary.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(OUTPUT_BY_EVENT_DATE)
    )

    print("\n=== WRITE VERSION 2: partitionBy(summary_year, summary_month) ===")
    (
        daily_sales_summary.write
        .mode("overwrite")
        .partitionBy("summary_year", "summary_month")
        .parquet(OUTPUT_BY_YEAR_MONTH)
    )

    event_date_file_count = count_parquet_files(OUTPUT_BY_EVENT_DATE)
    event_date_folder_count = count_folders(OUTPUT_BY_EVENT_DATE)

    year_month_file_count = count_parquet_files(OUTPUT_BY_YEAR_MONTH)
    year_month_folder_count = count_folders(OUTPUT_BY_YEAR_MONTH)

    print("\n=== PARTITION COMPARISON RESULT ===")
    print(f"partitionBy(event_date):")
    print(f"- folders      = {event_date_folder_count}")
    print(f"- parquet files = {event_date_file_count}")

    print(f"\npartitionBy(summary_year, summary_month):")
    print(f"- folders      = {year_month_folder_count}")
    print(f"- parquet files = {year_month_file_count}")

    print_tree_level_2(OUTPUT_BY_EVENT_DATE)
    print_tree_level_2(OUTPUT_BY_YEAR_MONTH)

    print("\n=== CONCLUSION HINT ===")
    print(
        "Với bảng daily_sales_summary có grain 1 dòng / 1 ngày, "
        "partition theo event_date thường tạo quá nhiều thư mục nhỏ. "
        "Partition theo summary_year, summary_month thường hợp lý hơn "
        "vì nhẹ hơn và tránh small files."
    )

    spark.stop()


if __name__ == "__main__":
    main()