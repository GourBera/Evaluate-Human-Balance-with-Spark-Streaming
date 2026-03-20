from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr, struct
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType


# Schema Definitions

# Schema for the Kafka redis-server topic
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

# Schema for the Customer
customerSchema = StructType([
    StructField("customerName", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("birthDay", StringType())
])

# Schema for the stedi-events Kafka topic (Customer Risk)
stediEventsSchema = StructType([
    StructField("customer", StringType()),
    StructField("score", FloatType()),
    StructField("riskDate", StringType())
])


#Create a spark application object
spark = SparkSession.builder \
    .appName("STEDI Kafka Join") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

#Set the spark log level to WARN
spark.sparkContext.setLogLevel("WARN")


# -----------------------------------------------------------------------
# Stream 1 - Redis -> Customer Birthday Information
# -----------------------------------------------------------------------

# Read raw events from the redis-server Kafka topic (startingOffsets=earliest)
redisServerRawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "redis-server") \
    .option("startingOffsets", "earliest") \
    .load()

# Cast the value column in the streaming dataframe as a STRING
redisServerStreamingDF = redisServerRawStreamingDF.selectExpr("cast(value as string) value")

# Parse the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"key":"Q3..|
# +------------+
#
# with this JSON format: {"key":"Q3VzdG9tZXI=",
# "existType":"NONE",
# "Ch":false,
# "Incr":false,
# "zSetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "Score":0.0
# }],
# "zsetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "score":0.0
# }]
# }
# 
# (Note: The Redis Source for Kafka has redundant fields zSetEntries and zsetentries, only one should be parsed)
#
# and create separated fields like this:
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |         key|value|expiredType|expiredValue|existType|   ch| incr|      zSetEntries|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |U29ydGVkU2V0| null|       null|        null|     NONE|false|false|[[dGVzdDI=, 0.0]]|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
#
# storing them in a temporary view called RedisSortedSet
redisServerStreamingDF \
    .withColumn("value", from_json("value", redisServerSchema)) \
    .select(col("value.*")) \
    .createOrReplaceTempView("RedisSortedSet")

# Extract the base64-encoded Customer element from position 0 of zSetEntries
encodedCustomerStreamingDF = spark.sql(
    "SELECT zSetEntries[0].element AS encodedCustomer FROM RedisSortedSet"
)

# Take the encodedCustomer column which is base64 encoded at first like this:
# +--------------------+
# |            customer|
# +--------------------+
# |[7B 22 73 74 61 7...|
# +--------------------+

# and convert it to clear json like this:
# +--------------------+
# |            customer|
# +--------------------+
# |{"customerName":"...|
#+--------------------+
#
# with this JSON format: {"customerName":"Sam Test","email":"sam.test@test.com","phone":"8015551212","birthDay":"2001-01-03"}

# Base64-decode to raw Customer JSON, parse into struct, store as temp view
encodedCustomerStreamingDF \
    .withColumn("customer", unbase64(col("encodedCustomer")).cast("string")) \
    .withColumn("customer", from_json("customer", customerSchema)) \
    .select(col("customer.*")) \
    .createOrReplaceTempView("CustomerRecords")

# Filter out null rows; only keep email and birthDay
emailAndBirthDayStreamingDF = spark.sql(
    "SELECT email, birthDay FROM CustomerRecords WHERE email IS NOT NULL AND birthDay IS NOT NULL"
)

# Split birthDay (format: YYYY-MM-DD) to isolate the year
emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF \
    .withColumn("birthYear", split(col("birthDay"), "-").getItem(0)) \
    .select(col("email"), col("birthYear"))


# -----------------------------------------------------------------------
# Stream 2 - stedi-events -> Customer Risk Scores
# -----------------------------------------------------------------------

# Read raw events from the stedi-events Kafka topic (startingOffsets=earliest)
stediEventsRawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "stedi-events") \
    .option("startingOffsets", "earliest") \
    .load()

# Cast the value column in the streaming dataframe as a STRING
stediEventsStreamingDF = stediEventsRawStreamingDF.selectExpr("cast(value as string) value")

# Parse the JSON from the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"custom"...|
# +------------+
#
# and create separated fields like this:
# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+
#
# storing them in a temporary view called CustomerRisk
# Parse JSON and store in a temp view called CustomerRisk
stediEventsStreamingDF \
    .withColumn("value", from_json("value", stediEventsSchema)) \
    .select(col("value.*")) \
    .createOrReplaceTempView("CustomerRisk")

# Execute a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF
# Select customer (email) and risk score
customerRiskStreamingDF = spark.sql("SELECT customer, score FROM CustomerRisk")

# Join the streaming dataframes on the email address to get the risk score and the birth year in the same dataframe
riskScoreByBirthYear = customerRiskStreamingDF \
    .join(emailAndBirthYearStreamingDF, expr("customer = email"))

# Sink the joined dataframes to a new kafka topic to send the data to the STEDI graph application
# +--------------------+-----+--------------------+---------+
# |            customer|score|               email|birthYear|
# +--------------------+-----+--------------------+---------+
# |Santosh.Phillips@...| -0.5|Santosh.Phillips@...|     1960|
# |Sean.Howard@test.com| -3.0|Sean.Howard@test.com|     1958|
# |Suresh.Clark@test...| -5.0|Suresh.Clark@test...|     1956|
# |  Lyn.Davis@test.com| -4.0|  Lyn.Davis@test.com|     1955|
# |Sarah.Lincoln@tes...| -2.0|Sarah.Lincoln@tes...|     1959|
# |Sarah.Clark@test.com| -4.0|Sarah.Clark@test.com|     1957|
# +--------------------+-----+--------------------+---------+
#
# In this JSON Format {"customer":"Santosh.Fibonnaci@test.com","score":"28.5","email":"Santosh.Fibonnaci@test.com","birthYear":"1963"}
kafkaQuery = riskScoreByBirthYear \
    .selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("topic", "customer-risk") \
    .option("checkpointLocation", "/tmp/kafkajoin-checkpoint") \
    .outputMode("append") \
    .start()

consoleQuery = riskScoreByBirthYear \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("numRows", "50") \
    .option("checkpointLocation", "/tmp/kafkajoin-console-checkpoint") \
    .start()

kafkaQuery.awaitTermination()