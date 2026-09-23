from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("DebeziumOrdersIcebergIncremental")
    .config(
        "spark.sql.catalog.local",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(
        "spark.sql.catalog.local.type",
        "hadoop"
    )
    .config(
        "spark.sql.catalog.local.warehouse",
        "/opt/project/data/iceberg"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# Configuration
# ============================================================

SILVER_PATH = "/opt/project/data/silver/debezium_orders"

ICEBERG_TABLE = "local.ecommerce.orders_current"

STATE_TABLE = "local.ecommerce.cdc_pipeline_state"

KAFKA_TOPIC = "ecommerce.ecommerce_platform.orders_v2"

PIPELINE_NAME = "orders_cdc"

KAFKA_PARTITION = 0


# ============================================================
# Create Iceberg namespace
# ============================================================

spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS local.ecommerce
""")


# ============================================================
# Create current-state Iceberg table
# ============================================================

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE} (
        order_id BIGINT,
        customer_id BIGINT,
        order_status STRING,
        total_amount DECIMAL(12,2),
        order_timestamp TIMESTAMP,
        event_timestamp TIMESTAMP,
        event_date DATE,
        operation STRING,
        kafka_topic STRING,
        kafka_partition INT,
        kafka_offset BIGINT
    )
    USING iceberg
    PARTITIONED BY (event_date)
""")


# ============================================================
# Create CDC pipeline state table
# ============================================================

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
        pipeline_name STRING,
        kafka_topic STRING,
        kafka_partition INT,
        last_processed_offset BIGINT,
        updated_at TIMESTAMP
    )
    USING iceberg
""")


# ============================================================
# Read last processed Kafka offset
# ============================================================

state_df = spark.sql(f"""
    SELECT last_processed_offset
    FROM {STATE_TABLE}
    WHERE pipeline_name = '{PIPELINE_NAME}'
      AND kafka_topic = '{KAFKA_TOPIC}'
      AND kafka_partition = {KAFKA_PARTITION}
""")

state_rows = state_df.collect()


if state_rows:
    max_offset = state_rows[0]["last_processed_offset"]
else:
    max_offset = -1


print("=" * 70)
print("Debezium CDC → Iceberg Incremental Pipeline")
print("=" * 70)

print("Last processed Kafka offset:", max_offset)


# ============================================================
# Read Silver CDC history
# ============================================================

silver_df = spark.read.parquet(SILVER_PATH)


# ============================================================
# Get only new CDC records
# ============================================================

changes_df = silver_df.filter(
    col("kafka_offset") > max_offset
)


# ============================================================
# Check whether new records exist
# ============================================================

new_record_count = changes_df.count()


if new_record_count == 0:

    print("No new CDC records found.")
    print("Iceberg table is already up to date.")

else:

    print("New CDC record count:", new_record_count)


    # ========================================================
    # CDC operation summary
    # ========================================================

    print("CDC operation summary:")

    (
        changes_df
        .groupBy("operation")
        .count()
        .orderBy("operation")
        .show()
    )


    # ========================================================
    # Show sample records
    # ========================================================

    print("Sample CDC records:")

    (
        changes_df
        .orderBy("kafka_offset")
        .limit(20)
        .show(
            truncate=False
        )
    )


    # ========================================================
    # Deduplicate CDC events
    #
    # If the same order has multiple events in the same batch,
    # keep only the latest event based on Kafka offset.
    #
    # Example:
    #
    # order 503
    #   c offset 505
    #   u offset 506
    #
    # Only offset 506 is sent to MERGE.
    # ========================================================

    window = (
        Window
        .partitionBy("order_id")
        .orderBy(
            col("kafka_offset").desc()
        )
    )


    latest_changes_df = (
        changes_df
        .withColumn(
            "rn",
            row_number().over(window)
        )
        .filter(
            col("rn") == 1
        )
        .drop("rn")
    )


    print("Latest CDC records used for MERGE:")

    (
        latest_changes_df
        .orderBy("kafka_offset")
        .limit(20)
        .show(
            truncate=False
        )
    )


    # ========================================================
    # Create temporary view
    # ========================================================

    latest_changes_df.createOrReplaceTempView(
        "cdc_changes"
    )


    # ========================================================
    # MERGE CDC changes into current-state Iceberg table
    # ========================================================

    print("Applying CDC changes to Iceberg...")


    spark.sql(f"""
        MERGE INTO {ICEBERG_TABLE} AS target

        USING cdc_changes AS source

        ON target.order_id = source.order_id

        WHEN MATCHED
             AND source.operation = 'd'
        THEN DELETE

        WHEN MATCHED
             AND source.operation IN ('u', 'c', 'r')
        THEN UPDATE SET
            target.customer_id = source.customer_id,
            target.order_status = source.order_status,
            target.total_amount = source.total_amount,
            target.order_timestamp = source.order_timestamp,
            target.event_timestamp = source.event_timestamp,
            target.event_date = source.event_date,
            target.operation = source.operation,
            target.kafka_topic = source.kafka_topic,
            target.kafka_partition = source.kafka_partition,
            target.kafka_offset = source.kafka_offset

        WHEN NOT MATCHED
             AND source.operation IN ('c', 'r')
        THEN INSERT (
            order_id,
            customer_id,
            order_status,
            total_amount,
            order_timestamp,
            event_timestamp,
            event_date,
            operation,
            kafka_topic,
            kafka_partition,
            kafka_offset
        )
        VALUES (
            source.order_id,
            source.customer_id,
            source.order_status,
            source.total_amount,
            source.order_timestamp,
            source.event_timestamp,
            source.event_date,
            source.operation,
            source.kafka_topic,
            source.kafka_partition,
            source.kafka_offset
        )
    """)


    print("CDC MERGE completed successfully.")


    # ========================================================
    # Calculate new watermark
    #
    # IMPORTANT:
    # This is inside the `else` block.
    # Therefore it will NEVER execute when there are
    # zero new records.
    # ========================================================

    max_processed_offset = (
        changes_df
        .agg({
            "kafka_offset": "max"
        })
        .collect()[0][0]
    )


    print(
        "Updating watermark to:",
        max_processed_offset
    )


    # ========================================================
    # Update CDC pipeline state
    # ========================================================

    spark.sql(f"""
        MERGE INTO {STATE_TABLE} AS target

        USING (
            SELECT
                '{PIPELINE_NAME}' AS pipeline_name,
                '{KAFKA_TOPIC}' AS kafka_topic,
                {KAFKA_PARTITION} AS kafka_partition,
                {max_processed_offset} AS last_processed_offset,
                current_timestamp() AS updated_at
        ) AS source

        ON target.pipeline_name = source.pipeline_name
        AND target.kafka_topic = source.kafka_topic
        AND target.kafka_partition = source.kafka_partition

        WHEN MATCHED THEN UPDATE SET
            target.last_processed_offset =
                source.last_processed_offset,
            target.updated_at =
                source.updated_at

        WHEN NOT MATCHED THEN INSERT (
            pipeline_name,
            kafka_topic,
            kafka_partition,
            last_processed_offset,
            updated_at
        )
        VALUES (
            source.pipeline_name,
            source.kafka_topic,
            source.kafka_partition,
            source.last_processed_offset,
            source.updated_at
        )
    """)


    print("Watermark updated successfully.")


# ============================================================
# Display current Iceberg state
# ============================================================

print()
print("=" * 70)
print("Current Iceberg Orders")
print("=" * 70)


current_df = spark.sql(f"""
    SELECT
        order_id,
        customer_id,
        order_status,
        total_amount,
        order_timestamp,
        event_timestamp,
        event_date,
        kafka_offset
    FROM {ICEBERG_TABLE}
    ORDER BY order_id
""")


current_df.show(
    20,
    truncate=False
)


# ============================================================
# Current order count
# ============================================================

current_count = current_df.count()

print()
print("Current order count:")
print(current_count)


# ============================================================
# Current watermark
# ============================================================

print()
print("Current CDC watermark:")

spark.sql(f"""
    SELECT
        pipeline_name,
        kafka_topic,
        kafka_partition,
        last_processed_offset,
        updated_at
    FROM {STATE_TABLE}
    WHERE pipeline_name = '{PIPELINE_NAME}'
      AND kafka_topic = '{KAFKA_TOPIC}'
      AND kafka_partition = {KAFKA_PARTITION}
""").show(
    truncate=False
)


# ============================================================
# Finish
# ============================================================

print()
print("=" * 70)
print("CDC incremental pipeline completed.")
print("=" * 70)


spark.stop()