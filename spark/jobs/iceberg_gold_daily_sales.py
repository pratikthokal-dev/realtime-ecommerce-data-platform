from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    count,
    sum,
    avg,
    countDistinct,
    round,
)


spark = (
    SparkSession.builder
    .appName("Iceberg Gold Daily Sales")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "/opt/project/data/iceberg")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Reading Iceberg current-state orders...")

orders_df = spark.table("local.ecommerce.orders_current")

gold_df = (
    orders_df
    .filter(col("order_status") != "cancelled")
    .withColumn("sales_date", to_date(col("order_timestamp")))
    .groupBy("sales_date")
    .agg(
        count("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("unique_customers"),
        round(sum("total_amount"), 2).alias("total_revenue"),
        round(avg("total_amount"), 2).alias("average_order_value"),
    )
    .orderBy("sales_date")
)

print("Gold daily sales:")
gold_df.show(50, truncate=False)

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

# Replace the current Gold snapshot.
gold_df.writeTo(
    "local.ecommerce.daily_sales"
).overwritePartitions()

print("Gold Iceberg table updated successfully.")

print("Current Gold table:")
spark.table("local.ecommerce.daily_sales") \
    .orderBy("sales_date") \
    .show(50, truncate=False)

spark.stop()