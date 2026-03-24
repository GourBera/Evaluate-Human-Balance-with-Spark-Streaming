#!/bin/bash
# ---------------------------------------------------------------------------
# Submit sparkpykafkajoin.py to the Spark cluster.
# Works in both the Udacity workspace (/data/spark) and Docker (bitnami).
# ---------------------------------------------------------------------------
set -e

# Detect Spark home
if [ -d "/data/spark" ]; then
  export SPARK_HOME=/data/spark
elif [ -d "/opt/bitnami/spark" ]; then
  export SPARK_HOME=/opt/bitnami/spark
elif [ -d "/spark" ]; then
  export SPARK_HOME=/spark
else
  echo "ERROR: SPARK_HOME not found" && exit 1
fi

mkdir -p /home/workspace/spark/logs

$SPARK_HOME/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 \
  --conf spark.sql.shuffle.partitions=2 \
  --conf spark.executor.heartbeatInterval=20s \
  --conf spark.network.timeout=300s \
  --conf spark.executor.extraJavaOptions="-XX:+UseG1GC" \
  /home/workspace/sparkpykafkajoin.py \
  2>&1 | tee /home/workspace/spark/logs/kafkajoin.log
