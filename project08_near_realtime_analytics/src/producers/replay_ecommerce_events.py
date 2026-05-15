import time
from pathlib import Path

import pandas as pd

from src.common.kafka_utils import build_producer


BOOTSTRAP_SERVERS = "localhost:9092"

ORDER_TOPIC = "ecom.order_created.v1"
PAYMENT_TOPIC = "ecom.payment_confirmed.v1"

ORDERS_PATH = Path("data/source/olist_orders_dataset.csv")
PAYMENTS_PATH = Path("data/source/olist_order_payments_dataset.csv")


def build_order_created_events(orders: pd.DataFrame, limit: int = 100) -> list[dict]:
    subset = (
        orders[["order_id", "customer_id", "order_purchase_timestamp"]]
        .dropna()
        .head(limit)
    )

    events = []
    for _, row in subset.iterrows():
        events.append({
            "order_id": str(row["order_id"]),
            "customer_id": str(row["customer_id"]),
            "event_type": "order_created",
            "event_time": str(row["order_purchase_timestamp"]),
        })

    return events


def build_payment_confirmed_events(payments: pd.DataFrame, limit: int = 100) -> list[dict]:
    subset = (
        payments[["order_id", "payment_sequential", "payment_value"]]
        .dropna()
        .head(limit)
    )

    events = []
    for _, row in subset.iterrows():
        order_id = str(row["order_id"])
        payment_seq = int(row["payment_sequential"])

        events.append({
            "order_id": order_id,
            "payment_id": f"{order_id}_{payment_seq}",
            "event_type": "payment_confirmed",
            "event_time": "2018-01-01 00:00:00",
            "payment_value": float(row["payment_value"]),
        })

    return events


def main() -> None:
    if not ORDERS_PATH.exists():
        raise FileNotFoundError(f"Missing file: {ORDERS_PATH}")

    if not PAYMENTS_PATH.exists():
        raise FileNotFoundError(f"Missing file: {PAYMENTS_PATH}")

    orders = pd.read_csv(ORDERS_PATH)
    payments = pd.read_csv(PAYMENTS_PATH)

    producer = build_producer(BOOTSTRAP_SERVERS)

    order_events = build_order_created_events(orders, limit=100)
    payment_events = build_payment_confirmed_events(payments, limit=100)

    for event in order_events:
        producer.send(
            ORDER_TOPIC,
            key=event["order_id"],
            value=event,
        )
        print(f"[ORDER_CREATED] sent key={event['order_id']} value={event}")
        time.sleep(0.1)

    for event in payment_events:
        producer.send(
            PAYMENT_TOPIC,
            key=event["order_id"],
            value=event,
        )
        print(f"[PAYMENT_CONFIRMED] sent key={event['order_id']} value={event}")
        time.sleep(0.1)

    producer.flush()
    producer.close()

    print("Replay finished.")


if __name__ == "__main__":
    main()