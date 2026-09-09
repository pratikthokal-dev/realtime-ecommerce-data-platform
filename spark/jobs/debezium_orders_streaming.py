from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, get_json_object
from pyspark.sql.types import StructType, StructField, StringType


spark = (
    SparkSession.builder
    .appName("DebeziumOrdersStreaming")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# 1. Read CDC events from Kafka
cdc_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option(
        "subscribe",
        "ecommerce.ecommerce_platform.orders"
    )
    .option("startingOffsets", "earliest")
    .load()
)


# 2. Convert Kafka binary value to JSON string
json_df = cdc_df.selectExpr(
    "CAST(value AS STRING) AS json"
)


# 3. Extract Debezium envelope
cdc_schema = StructType([
    StructField("before", StringType(), True),
    StructField("after", StringType(), True),
    StructField("op", StringType(), True),
    StructField("ts_ms", StringType(), True),
])

parsed_df = json_df.select(
    from_json(col("json"), cdc_schema).alias("data")
)


cdc_structured_df = parsed_df.select(
    "data.before",
    "data.after",
    "data.op",
    "data.ts_ms"
)


# 4. Write raw CDC events to Bronze
query = (
    cdc_structured_df.writeStream
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

query.awaitTermination()