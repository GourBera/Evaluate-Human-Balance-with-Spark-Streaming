from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType


# Schema Definitions

# Schema for the Kafka redis-server topic - every Redis change event
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

# Schema for the Customer JSON payload stored (base64-encoded) inside Redis
customerSchema = StructType([
    StructField("customerName", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("birthDay", StringType())
])


# Schema for the Kafka stedi-events topic - Customer Risk JSON
stediEventsSchema = StructType([
    StructField("customer", StringType()),
    StructField("score", FloatType()),
    StructField("riskDate", StringType())
])

#Create a spark application object
spark = SparkSession.builder \
    .appName("STEDI Redis-Kafka Stream to Console") \
    .getOrCreate()

#Set the spark log level to WARN
spark.sparkContext.setLogLevel("WARN")

# -----------------------------------------------------------------------
# Read from Kafka redis-server topic
# -----------------------------------------------------------------------
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

# Execute a sql statement against a temporary view, which statement takes the element field from the 0th element in the array of structs and create a column called encodedCustomer
# the reason we do it this way is that the syntax available select against a view is different than a dataframe, and it makes it easy to select the nth element of an array in a sql column

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
# Take the element field from position 0 in the zSetEntries array
encodedCustomerStreamingDF = spark.sql(
    "SELECT zSetEntries[0].element AS encodedCustomer FROM RedisSortedSet"
)

# Parse the JSON in the Customer record and store in a temporary view called CustomerRecords
decodedCustomerStreamingDF = encodedCustomerStreamingDF \
    .withColumn("customer", unbase64(col("encodedCustomer")).cast("string"))

decodedCustomerStreamingDF \
    .withColumn("customer", from_json("customer", customerSchema)) \
    .select(col("customer.*")) \
    .createOrReplaceTempView("CustomerRecords")

# Select only email + birthDay where both are not null
emailAndBirthDayStreamingDF = spark.sql(
    "SELECT email, birthDay FROM CustomerRecords WHERE email IS NOT NULL AND birthDay IS NOT NULL"
)
# From the emailAndBirthDayStreamingDF dataframe select the email and the birth year (using the split function)

# Split the birth year as a separate field from the birthday
# Select only the birth year and email fields as a new streaming data frame called emailAndBirthYearStreamingDF
emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF \
    .withColumn("birthYear", split(col("birthDay"), "-").getItem(0)) \
    .select(col("email"), col("birthYear"))

# Sink the emailAndBirthYearStreamingDF dataframe to the console in append mode
# 
# The output should look like this:
# +--------------------+-----               
# | email         |birthYear|
# +--------------------+-----
# |Gail.Spencer@test...|1963|
# |Craig.Lincoln@tes...|1962|
# |  Edward.Wu@test.com|1961|
# |Santosh.Phillips@...|1960|
# |Sarah.Lincoln@tes...|1959|
# |Sean.Howard@test.com|1958|
# |Sarah.Clark@test.com|1957|
# +--------------------+-----
emailAndBirthYearStreamingDF \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()

# Run the python script by running the command from the terminal:
# /home/workspace/submit-redis-kafka-streaming.sh
# Verify the data looks correct 