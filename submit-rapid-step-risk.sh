# ---------------------------------------------------------------------------
# Submit sparkpyrapidstepriskscore.py to the Spark cluster.
# Standout feature: calculates risk score from RapidStepTest data and
# compares it against the official STEDI risk score side-by-side.
# ---------------------------------------------------------------------------
set -e

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
  /home/workspace/sparkpyrapidstepriskscore.py \
  2>&1 | tee /home/workspace/spark/logs/rapid-step-risk.log
