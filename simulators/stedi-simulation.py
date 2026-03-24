"""
STEDI Application Simulator
============================
Simulates the STEDI Step Timer application by:
1. Populating Redis with Customer data (ZSortedSet "Customer")
2. Recording RapidStepTest results in Redis (ZSortedSet "RapidStepTest:<email>")
3. Calculating risk scores and publishing them to the Kafka "stedi-events" topic
   whenever a customer has 4+ completed assessments.

Redis keyspace notifications are forwarded to Kafka by the Redis Source Connector.
"""

import json
import time
import random
import base64
import os
import redis
from kafka import KafkaProducer
from datetime import datetime, timezone

# -----------------------------------------------------------------------
# Configuration (from environment variables / .env)
# -----------------------------------------------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "notreally")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:19092")
STEDI_EVENTS_TOPIC = os.getenv("KAFKA_TOPIC_STEDI_EVENTS", "stedi-events")

# -----------------------------------------------------------------------
# Sample customer profiles
# -----------------------------------------------------------------------
CUSTOMERS = [
    {"customerName": "Gail Spencer", "email": "Gail.Spencer@test.com", "phone": "8015550101", "birthDay": "1963-04-12"},
    {"customerName": "Craig Lincoln", "email": "Craig.Lincoln@test.com", "phone": "8015550102", "birthDay": "1962-07-22"},
    {"customerName": "Edward Wu", "email": "Edward.Wu@test.com", "phone": "8015550103", "birthDay": "1961-11-05"},
    {"customerName": "Santosh Phillips", "email": "Santosh.Phillips@test.com", "phone": "8015550104", "birthDay": "1960-03-18"},
    {"customerName": "Sarah Lincoln", "email": "Sarah.Lincoln@test.com", "phone": "8015550105", "birthDay": "1959-09-30"},
    {"customerName": "Sean Howard", "email": "Sean.Howard@test.com", "phone": "8015550106", "birthDay": "1958-01-14"},
    {"customerName": "Sarah Clark", "email": "Sarah.Clark@test.com", "phone": "8015550107", "birthDay": "1957-06-27"},
    {"customerName": "Spencer Davis", "email": "Spencer.Davis@test.com", "phone": "8015550108", "birthDay": "1956-12-03"},
    {"customerName": "Santosh Fibonnaci", "email": "Santosh.Fibonnaci@test.com", "phone": "8015550109", "birthDay": "1963-08-16"},
    {"customerName": "Lyn Davis", "email": "Lyn.Davis@test.com", "phone": "8015550110", "birthDay": "1955-05-20"},
    {"customerName": "Jason Miller", "email": "Jason.Miller@test.com", "phone": "8015550111", "birthDay": "1970-02-14"},
    {"customerName": "Karen White", "email": "Karen.White@test.com", "phone": "8015550112", "birthDay": "1968-09-08"},
    {"customerName": "Brian Johnson", "email": "Brian.Johnson@test.com", "phone": "8015550113", "birthDay": "1975-06-21"},
    {"customerName": "Rachel Adams", "email": "Rachel.Adams@test.com", "phone": "8015550114", "birthDay": "1972-11-30"},
    {"customerName": "Kevin Brown", "email": "Kevin.Brown@test.com", "phone": "8015550115", "birthDay": "1965-03-25"},
    {"customerName": "Nancy Wilson", "email": "Nancy.Wilson@test.com", "phone": "8015550116", "birthDay": "1980-07-17"},
    {"customerName": "Thomas Garcia", "email": "Thomas.Garcia@test.com", "phone": "8015550117", "birthDay": "1978-12-04"},
    {"customerName": "Laura Martinez", "email": "Laura.Martinez@test.com", "phone": "8015550118", "birthDay": "1983-01-22"},
    {"customerName": "Daniel Robinson", "email": "Daniel.Robinson@test.com", "phone": "8015550119", "birthDay": "1969-08-09"},
    {"customerName": "Michelle Lee", "email": "Michelle.Lee@test.com", "phone": "8015550120", "birthDay": "1974-04-15"},
    {"customerName": "Robert Taylor", "email": "Robert.Taylor@test.com", "phone": "8015550121", "birthDay": "1982-10-28"},
    {"customerName": "Jennifer Hall", "email": "Jennifer.Hall@test.com", "phone": "8015550122", "birthDay": "1967-05-11"},
    {"customerName": "William Young", "email": "William.Young@test.com", "phone": "8015550123", "birthDay": "1976-02-03"},
    {"customerName": "Amanda King", "email": "Amanda.King@test.com", "phone": "8015550124", "birthDay": "1985-09-19"},
    {"customerName": "Christopher Wright", "email": "Christopher.Wright@test.com", "phone": "8015550125", "birthDay": "1971-07-07"},
    {"customerName": "Stephanie Lopez", "email": "Stephanie.Lopez@test.com", "phone": "8015550126", "birthDay": "1964-12-31"},
    {"customerName": "Matthew Scott", "email": "Matthew.Scott@test.com", "phone": "8015550127", "birthDay": "1979-03-14"},
    {"customerName": "Emily Green", "email": "Emily.Green@test.com", "phone": "8015550128", "birthDay": "1988-06-06"},
    {"customerName": "Andrew Baker", "email": "Andrew.Baker@test.com", "phone": "8015550129", "birthDay": "1966-11-23"},
    {"customerName": "Lisa Nelson", "email": "Lisa.Nelson@test.com", "phone": "8015550130", "birthDay": "1973-08-18"},
]

# -----------------------------------------------------------------------
# Redis & Kafka clients
# -----------------------------------------------------------------------

def wait_for_redis(host, port, password, retries=30):
    for _ in range(retries):
        try:
            r = redis.Redis(host=host, port=port, password=password, decode_responses=True)
            r.ping()
            print(f"[stedi-sim] Connected to Redis at {host}:{port}")
            return r
        except Exception as e:
            print(f"[stedi-sim] Waiting for Redis... ({e})")
            time.sleep(2)
    raise RuntimeError("Could not connect to Redis")


def wait_for_kafka(broker, retries=30):
    for _ in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[broker],
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print(f"[stedi-sim] Connected to Kafka at {broker}")
            return producer
        except Exception as e:
            print(f"[stedi-sim] Waiting for Kafka... ({e})")
            time.sleep(3)
    raise RuntimeError("Could not connect to Kafka")


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

def to_base64(data: dict) -> str:
    return base64.b64encode(json.dumps(data).encode()).decode()


def emit_redis_server_event(producer: KafkaProducer, redis_key: str, encoded_element: str, score: float = 0.0) -> None:
    """Emit a Kafka message shaped like Redis Source Connector output."""
    envelope = {
        "key": base64.b64encode(redis_key.encode()).decode(),
        "existType": "NONE",
        "Ch": False,
        "Incr": False,
        "zSetEntries": [
            {
                "element": encoded_element,
                "Score": float(score),
            }
        ],
    }
    producer.send(os.getenv("KAFKA_TOPIC_REDIS", "redis-server"), envelope)
    producer.flush()


def register_customer(r: redis.Redis, customer: dict) -> str:
    """Write customer record into the Redis Customer sorted set."""
    encoded = to_base64(customer)
    r.zadd("Customer", {encoded: 0.0})
    return encoded


def record_step_test(r: redis.Redis, email: str, start_time: int, stop_time: int) -> str:
    """Write a RapidStepTest entry into Redis."""
    test = {"email": email, "startTime": start_time, "stopTime": stop_time}
    encoded = to_base64(test)
    r.zadd(f"RapidStepTest:{email}", {encoded: float(start_time)})
    return encoded


def calculate_risk(durations: list) -> float:
    """Java risk score logic ported to Python (requires >= 4 durations)."""
    current_avg = (durations[-1] + durations[-2]) / 2.0
    previous_avg = (durations[-3] + durations[-4]) / 2.0
    return round((previous_avg - current_avg) / 1000.0, 2)


def publish_risk(producer: KafkaProducer, email: str, score: float) -> None:
    msg = {
        "customer": email,
        "score": score,
        "riskDate": datetime.now(timezone.utc).isoformat()
    }
    producer.send(STEDI_EVENTS_TOPIC, msg)
    producer.flush()
    print(f"[stedi-sim] Published risk score for {email}: {score}")


# -----------------------------------------------------------------------
# Main simulation loop
# -----------------------------------------------------------------------

def main():
    r = wait_for_redis(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)
    producer = wait_for_kafka(KAFKA_BROKER)

    # Enable keyspace notifications so the Kafka Connect Redis Source picks them up
    r.config_set("notify-keyspace-events", "KEA")

    # Register all customers in Redis
    print("[stedi-sim] Registering customers in Redis...")
    for customer in CUSTOMERS:
        encoded_customer = register_customer(r, customer)
        emit_redis_server_event(producer, "Customer", encoded_customer, 0.0)

    # Simulate step tests over time
    print("[stedi-sim] Starting step test simulation loop...")
    step_counts: dict = {c["email"]: [] for c in CUSTOMERS}

    while True:
        customer = random.choice(CUSTOMERS)
        email = customer["email"]

        # Simulate a 30-step test: random duration between 20–90 seconds
        now_ms = int(time.time() * 1000)
        duration_ms = random.randint(20_000, 90_000)
        start_time = now_ms
        stop_time = now_ms + duration_ms

        encoded_step_test = record_step_test(r, email, start_time, stop_time)
        emit_redis_server_event(producer, f"RapidStepTest:{email}", encoded_step_test, float(start_time))
        step_counts[email].append(duration_ms)

        print(f"[stedi-sim] Recorded step test for {email}  "
              f"(duration={duration_ms/1000:.1f}s, "
              f"tests_so_far={len(step_counts[email])})")

        # Publish risk score once the customer has 4+ tests
        if len(step_counts[email]) >= 4:
            durations = step_counts[email][-4:]
            score = calculate_risk(durations)
            publish_risk(producer, email, score)

        # Pause between 3–8 seconds before the next simulated test
        time.sleep(random.uniform(3, 8))


if __name__ == "__main__":
    main()
