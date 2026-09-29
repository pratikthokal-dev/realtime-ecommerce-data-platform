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
    col("json").alias("raw_json"),

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
# 8. Extract CDC fields and identify invalid records
# ---------------------------------------------------------

parsed_with_status_df = parsed_df.select(
    col("raw_json"),
    col("data"),

    col("data.payload.before").alias("before"),
    col("data.payload.after").alias("after"),
    col("data.payload.op").alias("operation"),
    col("data.payload.ts_ms").alias("event_timestamp"),

    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp")
)

valid_cdc_df = parsed_with_status_df.filter(
    col("data").isNotNull()
)

invalid_cdc_df = parsed_with_status_df.filter(
    col("data").isNull()
)

# ---------------------------------------------------------
# 9. Write invalid CDC records to DLQ
# ---------------------------------------------------------

dlq_df = invalid_cdc_df.select(
    col("raw_json").alias("value"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("kafka_timestamp")
)

dlq_query = (
    dlq_df
    .selectExpr(
        "CAST(NULL AS STRING) AS key",
        "value"
    )
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("topic", "ecommerce.orders.dlq")
    .option(
        "checkpointLocation",
        "/opt/project/data/spark-checkpoints/debezium_orders_dlq"
    )
    .outputMode("append")
    .start()
)


# ---------------------------------------------------------
# 10. Create Bronze CDC output
# ---------------------------------------------------------

bronze_df = valid_cdc_df.select(
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
# 11. Write Bronze CDC data
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
# 12. Keep streaming query alive
# ---------------------------------------------------------

# ---------------------------------------------------------
# 12. Keep streaming queries alive
# ---------------------------------------------------------

print("DLQ query started:", dlq_query.id, flush=True)
print("Bronze query started:", query.id, flush=True)

print("DLQ query active:", dlq_query.isActive, flush=True)
print("Bronze query active:", query.isActive, flush=True)

print("DLQ query status:", dlq_query.status, flush=True)
print("Bronze query status:", query.status, flush=True)

# Diagnostic monitoring
while True:
    print("\n========== STREAMING STATUS ==========", flush=True)

    print("DLQ active:", dlq_query.isActive, flush=True)
    print("Bronze active:", query.isActive, flush=True)

    print("DLQ status:", dlq_query.status, flush=True)
    print("Bronze status:", query.status, flush=True)

    print("DLQ exception:", dlq_query.exception(), flush=True)
    print("Bronze exception:", query.exception(), flush=True)

    print("DLQ last progress:", dlq_query.lastProgress, flush=True)
    print("Bronze last progress:", query.lastProgress, flush=True)

    import time
    time.sleep(10)