from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    round
)

spark = (
    SparkSession.builder
    .appName("GoldDailySales")
    .master("local[*]")
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


# Read Silver data
silver_df = spark.read.parquet(
    "/opt/project/data/silver/orders"
)


# Create daily sales aggregation
gold_daily_sales = (
    silver_df
    .groupBy("event_date")
    .agg(
        count("order_id").alias("total_orders"),
        round(sum("amount"), 2).alias("total_revenue"),
        round(avg("amount"), 2).alias("average_order_value")
    )
    .orderBy("event_date")
)


print("\n=== GOLD DAILY SALES ===")

gold_daily_sales.show(
    truncate=False
)


print("\n=== GOLD RECORD COUNT ===")

print(gold_daily_sales.count())


# Write Gold data
gold_daily_sales.write.mode("overwrite").parquet(
    "/opt/project/data/gold/daily_sales"
)


spark.stop()