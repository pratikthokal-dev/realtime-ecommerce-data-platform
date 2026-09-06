from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType
)

spark = (
    SparkSession.builder
    .appName("EcommerceOrderStreaming")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Kafka → Spark
orders_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "ecommerce.orders")
    .option("startingOffsets", "earliest")
    .load()
)


# Define JSON schema
order_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("order_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("timestamp", StringType(), True)
])


# Convert Kafka binary value → JSON string
json_df = orders_df.selectExpr(
    "CAST(value AS STRING) AS json"
)


# Parse JSON
parsed_df = json_df.select(
    from_json(col("json"), order_schema).alias("data")
)


# Select structured columns
orders_structured_df = parsed_df.select(
    "data.event_type",
    "data.order_id",
    "data.customer_id",
    "data.amount",
    "data.status",
    "data.timestamp"
)


# Display structured streaming data
query = (
    orders_structured_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option( "path","/opt/project/data/bronze/orders")
    .option("checkpointLocation", "/opt/project/data/spark-checkpoints/orders")
    .start()
)

query.awaitTermination()