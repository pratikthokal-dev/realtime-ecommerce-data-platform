from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ReadGoldDailySales")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

gold_df = spark.read.parquet(
    "/opt/project/data/gold/daily_sales"
)

print("\n=== GOLD SCHEMA ===")
gold_df.printSchema()

print("\n=== GOLD DAILY SALES ===")
gold_df.show(50, truncate=False)

print("\n=== GOLD RECORD COUNT ===")
print(gold_df.count())

print("\n=== TOTAL ORDERS ===")
print(gold_df.agg({"total_orders": "sum"}).collect()[0][0])

print("\n=== TOTAL REVENUE ===")
print(gold_df.agg({"total_revenue": "sum"}).collect()[0][0])

spark.stop()