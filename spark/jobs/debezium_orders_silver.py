from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    to_timestamp,
    to_date,
)


spark = (
    SparkSession.builder
    .appName("DebeziumOrdersSilver")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------------------------
# 1. Read CDC Bronze
# ---------------------------------------------------------

bronze_df = (
    spark.read
    .parquet("/opt/project/data/bronze/debezium_orders")
)


# ---------------------------------------------------------
# 2. Normalize CDC records
# ---------------------------------------------------------

silver_df = (
    bronze_df
    .withColumn(
        "order_id",
        coalesce(
            col("order_id"),
            col("before_order_id")
        )
    )
    .withColumn(
        "customer_id",
        coalesce(
            col("customer_id"),
            col("before_customer_id")
        )
    )
    .withColumn(
        "order_status",
        coalesce(
            col("order_status"),
            col("before_order_status")
        )
    )
    .withColumn(
        "total_amount",
        col("total_amount")
    )
    .withColumn(
        "order_timestamp",
        to_timestamp("order_timestamp")
    )
    .withColumn(
        "event_timestamp",
        to_timestamp(
            col("event_timestamp") / 1000
        )
    )
    .withColumn(
        "event_date",
        to_date("order_timestamp")
    )
)


# 3. Select Silver columns
# ---------------------------------------------------------

silver_df = silver_df.select(
    "order_id",
    "customer_id",
    "order_status",
    "total_amount",
    "order_timestamp",
    "event_timestamp",
    "event_date",
    "operation",
    col("topic").alias("kafka_topic"),
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
)

# ---------------------------------------------------------
# 4. Basic data-quality filters
# ---------------------------------------------------------

silver_df = silver_df.filter(
    col("order_id").isNotNull()
)


# ---------------------------------------------------------
# 5. Write normalized CDC Silver
# ---------------------------------------------------------

(
    silver_df.write
    .mode("overwrite")
    .partitionBy("event_date")
    .parquet(
        "/opt/project/data/silver/debezium_orders"
    )
)


print("CDC Silver records:", silver_df.count())

print("\nCDC operations:")
silver_df.groupBy("operation").count().orderBy("operation").show()

print("\nCDC Silver sample:")
silver_df.orderBy("kafka_offset").show(
    20,
    truncate=False
)


spark.stop()