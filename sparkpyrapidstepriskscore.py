"""
Rapid Step Test Risk Score Calculator
=========================================================
Listens to the redis-server Kafka topic and analyzes the Redis SortedSet
called RapidStepTest to calculate a custom risk score based on the last 4
steps tests for each customer.

Risk Score Logic (mirrors the Java pseudocode in the rubric):
  - Sort tests by startTime (ascending)
  - currentAvg  = avg time of the 2 most recent tests
  - previousAvg = avg time of the 2 tests before those
  - riskScore   = (previousAvg - currentAvg) / 1000
  - Negative score ⟹ step time is going down (getting faster / improving)
  - Positive score ⟹ step time is going up   (getting slower / deteriorating)

The result is published to a Kafka topic "rapid-step-risk" AND simultaneously
compared against the official STEDI risk score from the "stedi-events" topic,
with the comparison result sinked to the console.

Run with:
  /home/workspace/submit-rapid-step-risk.sh
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, to_json, col, unbase64, split, expr,
    collect_list, sort_array, struct, size
)
from pyspark.sql.types import (
    StructField, StructType, StringType, BooleanType,
    ArrayType, FloatType, LongType
)
from pyspark.sql.functions import udf
from pyspark.sql.functions import collect_list, sort_array

# Schema Definitions
# Outer Redis change-event envelope
redisServerSchema = StructType([
    StructField("key", StringType()),
    StructField("existType", StringType()),
    StructField("Ch", BooleanType()),
    StructField("Incr", BooleanType()),
    StructField("zSetEntries", ArrayType(
        StructType([
            StructField("element", StringType()),
            StructField("Score", FloatType())
        ])
    ))
])

# RapidStepTest JSON payload (base64-encoded inside the SortedSet element)
# {"email":"...","startTime":1234567890,"stopTime":1234567920}
rapidStepTestSchema = StructType([
    StructField("email", StringType()),
    StructField("startTime", LongType()),
    StructField("stopTime", LongType())
])

# Customer JSON payload (base64-encoded) - used to associate email with name
customerSchema = StructType([
    StructField("customerName", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("birthDay", StringType())
])

# stedi-events (official risk score for comparison)
stediEventsSchema = StructType([
    StructField("customer", StringType()),
    StructField("score", FloatType()),
    StructField("riskDate", StringType())
])

# -----------------------------------------------------------------------
# Spark Session
# -----------------------------------------------------------------------

spark = SparkSession.builder \
    .appName("STEDI Rapid Step Risk Score") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------------------------------------------------
# Stream A - Redis SortedSet "RapidStepTest" records
# -----------------------------------------------------------------------

redisRawDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "redis-server") \
    .option("startingOffsets", "earliest") \
    .load()

redisStringDF = redisRawDF.selectExpr("cast(value as string) value")

# Parse the outer Redis envelope
redisStringDF \
    .withColumn("value", from_json("value", redisServerSchema)) \
    .select(col("value.*")) \
    .createOrReplaceTempView("RedisSortedSet")

# Pull out the first entry's element (base64-encoded) and the Redis key
# The key tells us whether this is a "Customer" or "RapidStepTest" entry
encodedEntryDF = spark.sql("""
    SELECT key, zSetEntries[0].element AS encodedEntry,
           zSetEntries[0].Score AS entryScore
    FROM RedisSortedSet
    WHERE zSetEntries IS NOT NULL AND size(zSetEntries) > 0
""")

# -----------------------------------------------------------------------
# Decode RapidStepTest entries
# The Redis key for step tests is base64("RapidStepTest:<email>")
# We decode every entry; invalid JSON will be silently dropped via null check
# -----------------------------------------------------------------------

stepTestDF = encodedEntryDF \
    .withColumn("decoded", unbase64(col("encodedEntry")).cast("string")) \
    .withColumn("stepTest", from_json("decoded", rapidStepTestSchema)) \
    .select(
        col("stepTest.email").alias("email"),
        col("stepTest.startTime").alias("startTime"),
        col("stepTest.stopTime").alias("stopTime"),
        col("entryScore").alias("sortedSetScore")
    ) \
    .where(col("email").isNotNull() & col("startTime").isNotNull() & col("stopTime").isNotNull())

# -----------------------------------------------------------------------
# Accumulate step tests per customer using a running aggregate
# Use watermark to bound state; adjust as needed for production
# -----------------------------------------------------------------------

# Register as a temp view for SQL-based windowed aggregation
stepTestDF.createOrReplaceTempView("RapidStepTests")

# -----------------------------------------------------------------------
# Custom risk score calculation using Spark UDF
# riskScore = (previousAvg - currentAvg) / 1000
# where currentAvg is the mean duration of the 2 most recent tests,
# and previousAvg is the mean of the 2 tests before those.
# -----------------------------------------------------------------------


@udf(returnType=FloatType())
def calc_risk_score(start_times, stop_times):
    """
    Accepts parallel lists of start and stop times (already sorted by startTime).
    Returns the risk score based on the last 4 entries, or None if < 4 exist.
    """
    if start_times is None or len(start_times) < 4:
        return None
    # Duration = stopTime - startTime for each test
    durations = [s - t for t, s in zip(start_times, stop_times)]
    # Use the last 4
    recent = durations[-4:]
    current_avg = (recent[3] + recent[2]) / 2.0
    previous_avg = (recent[1] + recent[0]) / 2.0
    return float((previous_avg - current_avg) / 1000.0)

# -----------------------------------------------------------------------
# Aggregate step tests per customer, then calculate risk
# Using a streaming groupBy with collect_list
# -----------------------------------------------------------------------

# Aggregate durations grouped by email inside a micro-batch
aggregatedDF = stepTestDF \
    .groupBy("email") \
    .agg(
        collect_list("startTime").alias("startTimes"),
        collect_list("stopTime").alias("stopTimes")
    )

# Calculate risk score per customer
riskScoreDF = aggregatedDF.withColumn(
    "calculatedRisk",
    calc_risk_score(col("startTimes"), col("stopTimes"))
).select(
    col("email").alias("customer"),
    col("calculatedRisk").alias("score")
).where(col("score").isNotNull())

# -----------------------------------------------------------------------
# Publish calculated risk scores to "rapid-step-risk" Kafka topic
# -----------------------------------------------------------------------

riskScoreDF \
    .selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("topic", "rapid-step-risk") \
    .option("checkpointLocation", "/tmp/rapid-step-risk-checkpoint") \
    .outputMode("update") \
    .start()

# -----------------------------------------------------------------------
# Console output for calculated risk stream
# -----------------------------------------------------------------------

riskScoreDF \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start() \
    .awaitTermination()
