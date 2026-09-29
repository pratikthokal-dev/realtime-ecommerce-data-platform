from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    countDistinct,
    round,
)


spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("GoldDailySales")
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
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.default.parallelism", "2")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Reading Gold intermediate data...")

gold_input_df = spark.read.parquet(
    "file:///opt/project/data/gold/intermediate_orders"
)

print("Running daily sales aggregation...")

gold_df = (
    gold_input_df
    .groupBy("sales_date")
    .agg(
        count("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("unique_customers"),
        round(sum("total_amount"), 2).alias("total_revenue"),
        round(avg("total_amount"), 2).alias("average_order_value"),
    )
    .orderBy("sales_date")
)

print("Writing Gold Iceberg table...")

spark.sql("""
CREATE NAMESPACE IF NOT EXISTS local.ecommerce
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS local.ecommerce.daily_sales (
    sales_date DATE,
    total_orders BIGINT,
    unique_customers BIGINT,
    total_revenue DECIMAL(18,2),
    average_order_value DECIMAL(18,2)
)
USING iceberg
PARTITIONED BY (sales_date)
""")

gold_df.writeTo(
    "local.ecommerce.daily_sales"
).overwritePartitions()

print("Gold Iceberg table updated successfully.")

spark.stop()