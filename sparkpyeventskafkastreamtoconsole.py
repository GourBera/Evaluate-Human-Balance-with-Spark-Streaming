from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType


# Create a SparkSession, with the name: STEDI Event Stream
spark = SparkSession.builder \
    .appName("STEDI Event Stream") \
    .getOrCreate()

# Set log level to WARN to reduce noise
spark.sparkContext.setLogLevel("WARN")


# Define the schema for the stedi-events topic JSON
stediEventsSchema = StructType([
    StructField("customer", StringType()),
    StructField("score", FloatType()),
    StructField("riskDate", StringType())
])

# Read a streaming dataframe from the Kafka topic stedi-events as the source
# startingOffsets=earliest ensures all historical events are read too
stediEventsRawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "stedi-events") \
    .option("startingOffsets", "earliest") \
    .load()

# Cast the value column in the streaming dataframe as a STRING
stediEventsStreamingDF = stediEventsRawStreamingDF.selectExpr("cast(value as string) value")

# +------------+
# | value      |
# +------------+
# |{"custom"...|
# +------------+

# Parse the JSON from the single "value" column using the defined schema
# Store result in a temporary view called CustomerRisk
stediEventsStreamingDF \
    .withColumn("value", from_json("value", stediEventsSchema)) \
    .select(col("value.*")) \
    .createOrReplaceTempView("CustomerRisk")

# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+

# Execute a SQL statement against CustomerRisk temporary view,
# selecting customer and score
customerRiskStreamingDF = spark.sql("SELECT customer, score FROM CustomerRisk")

# Sink the customerRiskStreamingDF dataframe to the console in append mode
customerRiskStreamingDF \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()

# The output will look like:
# +--------------------+-----
# |customer           |score|
# +--------------------+-----+
# |Spencer.Davis@tes...| 8.0|
# +--------------------+-----
# Run the python script by running the command from the terminal:
# /home/workspace/submit-event-kafka-streaming.sh
# Verify the data looks correct 