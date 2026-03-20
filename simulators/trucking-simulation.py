"""
Trucking Simulation
===================
Generates synthetic trucking telemetry events and publishes them to the
"trucking-data" Kafka topic.

Runs continuously, emitting a new event every 1–4 seconds.
"""

import json
import os
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:19092")
KAFKA_TOPIC = "trucking-data"

DRIVERS = [
    "Gail.Spencer@test.com",
    "Craig.Lincoln@test.com",
    "Edward.Wu@test.com",
    "Santosh.Phillips@test.com",
    "Sarah.Lincoln@test.com",
    "Sean.Howard@test.com",
]

EVENTS = ["normal", "unsafe following distance", "unsafe tail distance", "lane change", "overspeed"]

# Rough US lat/lon bounding box
LAT_RANGE = (25.84, 49.38)
LON_RANGE = (-124.85, -66.88)


def wait_for_kafka(broker: str, retries: int = 30) -> KafkaProducer:
    for _ in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[broker],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"[trucking-sim] Connected to Kafka at {broker}")
            return producer
        except Exception as e:
            print(f"[trucking-sim] Waiting for Kafka... ({e})")
            time.sleep(3)
    raise RuntimeError("Could not connect to Kafka")


def generate_event(driver_id: str, truck_id: str) -> dict:
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "truckId": truck_id,
        "driverId": driver_id,
        "latitude": round(random.uniform(*LAT_RANGE), 6),
        "longitude": round(random.uniform(*LON_RANGE), 6),
        "event": random.choices(EVENTS, weights=[70, 10, 10, 5, 5])[0],
        "speed": round(random.uniform(30.0, 90.0), 1),
    }


def main():
    producer = wait_for_kafka(KAFKA_BROKER)
    print(f"[trucking-sim] Publishing to topic: {KAFKA_TOPIC}")

    trucks = [str(i) for i in range(1, 11)]

    while True:
        driver = random.choice(DRIVERS)
        truck = random.choice(trucks)
        event = generate_event(driver, truck)
        producer.send(KAFKA_TOPIC, event)
        producer.flush()
        print(f"[trucking-sim] {event}")
        time.sleep(random.uniform(1.0, 4.0))


if __name__ == "__main__":
    main()
