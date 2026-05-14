import json
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer


BOOTSTRAP_SERVERS = "localhost:9092"

ORDER_TOPIC = "ecom.order_created.v1"
PAYMENT_TOPIC = "ecom.payment_confirmed.v1"


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )


def main():
    producer = build_producer()

    base_time = datetime(2026, 5, 14, 10, 0, 0)

    order_events = []
    payment_events = []

    for i in range(1, 16):
        order_id = f"O{i:04d}"
        customer_id = f"C{(i % 5) + 1:03d}"
        event_time = base_time + timedelta(minutes=i * 2)

        order_events.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "event_type": "order_created",
            "event_time": event_time.isoformat()
        })

        if i % 3 != 0:
            payment_events.append({
                "order_id": order_id,
                "payment_id": f"P{i:04d}",
                "event_type": "payment_confirmed",
                "event_time": (event_time + timedelta(minutes=1)).isoformat(),
                "payment_value": float(50 + i * 10)
            })

    for event in order_events:
        producer.send(
            ORDER_TOPIC,
            key=event["order_id"],
            value=event
        )
        print(f"[ORDER] sent: {event}")
        time.sleep(0.2)

    for event in payment_events:
        producer.send(
            PAYMENT_TOPIC,
            key=event["order_id"],
            value=event
        )
        print(f"[PAYMENT] sent: {event}")
        time.sleep(0.2)

    producer.flush()
    producer.close()

    print("Finished sending test events.")


if __name__ == "__main__":
    main()