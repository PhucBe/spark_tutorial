from pathlib import Path
import pyarrow.parquet as pq


def read_parquet_folder(path: str, title: str):
    print(f"\n=== {title} ===")

    folder = Path(path)

    if not folder.exists():
        print(f"Path does not exist: {path}")
        return

    parquet_files = list(folder.rglob("*.parquet"))

    if not parquet_files:
        print(f"No parquet files found yet in: {path}")
        return

    table = pq.read_table(parquet_files)
    df = table.to_pandas()

    if df.empty:
        print("Parquet exists but dataframe is empty.")
        return

    print(df.sort_values("window_start").head(50).to_string(index=False))


def main():
    read_parquet_folder(
        "data/streaming_output/order_count_10m",
        "ORDER COUNT 10M"
    )

    read_parquet_folder(
        "data/streaming_output/payment_revenue_5m",
        "PAYMENT REVENUE 5M"
    )


if __name__ == "__main__":
    main()