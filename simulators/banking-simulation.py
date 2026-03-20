"""
Banking Simulation
==================
Generates synthetic banking transaction events and publishes them to the
"bank-transactions" Kafka topic.

Runs continuously, emitting a new transaction every 1–3 seconds.
"""

import json
import os
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:19092")
KAFKA_TOPIC = "bank-transactions"

CUSTOMERS = [
    "Gail.Spencer@test.com",
    "Craig.Lincoln@test.com",
    "Edward.Wu@test.com",
    "Santosh.Phillips@test.com",
    "Sarah.Lincoln@test.com",
    "Sean.Howard@test.com",
    "Sarah.Clark@test.com",
    "Spencer.Davis@test.com",
    "Santosh.Fibonnaci@test.com",
    "Lyn.Davis@test.com",
]


def wait_for_kafka(broker: str, retries: int = 30) -> KafkaProducer:
    for _ in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[broker],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"[banking-sim] Connected to Kafka at {broker}")
            return producer
        except Exception as e:
            print(f"[banking-sim] Waiting for Kafka... ({e})")
            time.sleep(3)
    raise RuntimeError("Could not connect to Kafka")


def generate_transaction(customer_id: str) -> dict:
    return {
        "transactionDate": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "amount": round(random.uniform(10.0, 5000.0), 2),
        "customerId": customer_id,
        "transactionType": random.choice(["DEBIT", "CREDIT"]),
        "merchant": random.choice(
            ["Grocery Store", "Gas Station", "Online Shopping", "Restaurant", "Pharmacy"]
        ),
    }


def main():
    producer = wait_for_kafka(KAFKA_BROKER)
    print(f"[banking-sim] Publishing to topic: {KAFKA_TOPIC}")

    while True:
        customer = random.choice(CUSTOMERS)
        txn = generate_transaction(customer)
        producer.send(KAFKA_TOPIC, txn)
        producer.flush()
        print(f"[banking-sim] {txn}")
        time.sleep(random.uniform(1.0, 3.0))


if __name__ == "__main__":
    main()
