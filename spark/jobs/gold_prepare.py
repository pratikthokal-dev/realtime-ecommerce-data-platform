from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date


spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("GoldPrepare")
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

print("Reading Iceberg current-state orders...")

orders_df = spark.table("local.ecommerce.orders_current")

print("Preparing Gold input...")

gold_input_df = (
    orders_df
    .filter(col("order_status") != "cancelled")
    .withColumn(
        "sales_date",
        to_date(col("order_timestamp"))
    )
    .select(
        "order_id",
        "customer_id",
        "total_amount",
        "sales_date"
    )
)

print("Writing Gold intermediate data...")

gold_input_df.coalesce(1).write.mode("overwrite").parquet(
    "/opt/project/data/gold/intermediate_orders"
)

print("Gold preparation completed successfully.")

spark.stop()