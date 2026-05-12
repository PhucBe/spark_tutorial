import pandas as pd


RAW_PATH = "data/raw/olist_orders_dataset.csv"
OUTPUT_PATH = "data/curated/orders_pandas.parquet"


def main():
    df = pd.read_csv(RAW_PATH)

    df = df[
        [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp"
        ]
    ]

    df = df[df["order_id"].notna()]
    df = df[df["customer_id"].notna()]

    df["order_purchase_ts"] = pd.to_datetime(
        df["order_purchase_timestamp"],
        errors="coerce"
    )

    df = df[df["order_purchase_ts"].notna()]
    df["order_date"] = df["order_purchase_ts"].dt.date

    df = df.drop(columns=["order_purchase_timestamp"])

    df.to_parquet(OUTPUT_PATH, index=False)

    print(f"Pandas output written to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()