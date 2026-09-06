from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ReadBronzeOrders")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

bronze_df = spark.read.parquet(
    "/opt/project/data/bronze/orders"
)

print("\n=== Bronze Schema ===")
bronze_df.printSchema()

print("\n=== Bronze Sample Data ===")
bronze_df.show(10, truncate=False)

print("\n=== Bronze Record Count ===")
print(bronze_df.count())

spark.stop()