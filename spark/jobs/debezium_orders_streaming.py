from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DecimalType,
)


# ---------------------------------------------------------
# 1. Create Spark session
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("DebeziumOrdersStreaming")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------------------------
# 2. Read CDC events from Kafka
# ---------------------------------------------------------

cdc_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option(
        "subscribe",
        "ecommerce.ecommerce_platform.orders_v2"
    )
    .option("startingOffsets", "earliest")
    .load()
)


# ---------------------------------------------------------
# 3. Convert Kafka binary value → JSON string
# ---------------------------------------------------------

json_df = cdc_df.select(
    col("value").cast("string").alias("json"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp")
)


# ---------------------------------------------------------
# 4. Define the Debezium 'after' schema
# ---------------------------------------------------------

order_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("order_status", StringType(), True),
    StructField("total_amount", StringType(), True),
    StructField("order_timestamp", StringType(), True),
    StructField("updated_at", StringType(), True),
])


# ---------------------------------------------------------
# 5. Define the Debezium payload schema
# ---------------------------------------------------------

payload_schema = StructType([
    StructField(
        "before",
        order_schema,
        True
    ),
    StructField(
        "after",
        order_schema,
        True
    ),
    StructField(
        "op",
        StringType(),
        True
    ),
    StructField(
        "ts_ms",
        LongType(),
        True
    ),
])


# ---------------------------------------------------------
# 6. Define complete Debezium message schema
# ---------------------------------------------------------

debezium_schema = StructType([
    StructField(
        "payload",
        payload_schema,
        True
    )
])


# ---------------------------------------------------------
# 7. Parse Debezium JSON
# ---------------------------------------------------------

parsed_df = json_df.select(
    from_json(
        col("json"),
        debezium_schema
    ).alias("data"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp")
)


# ---------------------------------------------------------
# 8. Extract CDC fields
# ---------------------------------------------------------

cdc_structured_df = parsed_df.select(
    col("data.payload.before").alias("before"),
    col("data.payload.after").alias("after"),
    col("data.payload.op").alias("operation"),
    col("data.payload.ts_ms").alias("event_timestamp"),

    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp")
)


# ---------------------------------------------------------
# 9. Create Bronze CDC output
# ---------------------------------------------------------

bronze_df = cdc_structured_df.select(
    col("operation"),
    col("event_timestamp"),

    # Kafka metadata
    col("topic"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
    col("kafka_timestamp"),

    # Previous state
    col("before.order_id").alias("before_order_id"),
    col("before.customer_id").alias("before_customer_id"),
    col("before.order_status").alias("before_order_status"),
    col("before.total_amount").alias("before_total_amount"),

    # Current state
    col("after.order_id").alias("order_id"),
    col("after.customer_id").alias("customer_id"),
    col("after.order_status").alias("order_status"),
    col("after.total_amount").cast(
        DecimalType(12, 2)
    ).alias("total_amount"),
    col("after.order_timestamp").alias("order_timestamp"),
    col("after.updated_at").alias("updated_at"),
)

# ---------------------------------------------------------
# 10. Write Bronze CDC data
# ---------------------------------------------------------

query = (
    bronze_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        "/opt/project/data/bronze/debezium_orders"
    )
    .option(
        "checkpointLocation",
        "/opt/project/data/spark-checkpoints/debezium_orders"
    )
    .start()
)


# ---------------------------------------------------------
# 11. Keep streaming query alive
# ---------------------------------------------------------

query.awaitTermination()