#!/bin/bash
# ===========================================================================
# kafka-create-topics.sh
# Creates all Kafka topics required by the STEDI streaming project.
# Runs as a one-shot init container after the broker is healthy.
# ===========================================================================

set -e

BROKER="${KAFKA_BROKER:-kafka:19092}"
REPLICATION=1
PARTITIONS=1

echo "[init-topics] Waiting for Kafka broker at $BROKER..."
until rpk topic list --brokers "$BROKER" > /dev/null 2>&1; do
  echo "[init-topics] Broker not ready – retrying in 5s..."
  sleep 5
done
echo "[init-topics] Kafka is ready."

create_topic() {
  local topic=$1
  rpk topic create "$topic" --brokers "$BROKER" -p $PARTITIONS -r $REPLICATION || true
  echo "[init-topics] Topic ready: $topic"
}

create_topic redis-server
create_topic stedi-events
create_topic customer-risk
create_topic rapid-step-risk
create_topic bank-transactions
create_topic trucking-data

echo "[init-topics] All topics created successfully."
