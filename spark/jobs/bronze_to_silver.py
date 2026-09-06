from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date

spark = (
    SparkSession.builder
    .appName("BronzeToSilverOrders")
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

# Read Bronze
bronze_df = spark.read.parquet(
    "/opt/project/data/bronze/orders"
)

# Transform Bronze → Silver
silver_df = (
    bronze_df
    .withColumn(
        "timestamp",
        to_timestamp(col("timestamp"))
    )
    .withColumn(
        "event_date",
        to_date(col("timestamp"))
    )
    
    .filter(col("order_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    .filter(col("amount").isNotNull())
    .filter(col("amount") >= 0)
    .filter(
        col("status").isin(
            "PLACED",
            "CONFIRMED",
            "SHIPPED",
            "DELIVERED",
            "CANCELLED"
        )
    ).dropDuplicates(["order_id"])
)

print("\n=== Silver Schema ===")
silver_df.printSchema()

print("\n=== Silver Sample Data ===")
silver_df.show(10, truncate=False)

bronze_count = bronze_df.count()
silver_count = silver_df.count()
records_removed = bronze_count - silver_count

print("\n=== Data Quality Metrics ===")
print(f"Bronze Record Count : {bronze_count}")
print(f"Silver Record Count : {silver_count}")
print(f"Records Removed     : {records_removed}")


silver_df.write.mode("overwrite").partitionBy("event_date").parquet(
    "/opt/project/data/silver/orders"
)
spark.stop()