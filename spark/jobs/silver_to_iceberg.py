from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("SilverToIceberg")
    .master("spark://spark:7077")
    .config(
        "spark.jars",
        "/opt/airflow/jars/iceberg-spark-runtime-4.0_2.13-1.10.1.jar"
    )
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


# Read existing Silver Parquet data
silver_df = spark.read.parquet(
    "/opt/project/data/silver/orders"
)


print("\n=== Silver Input Count ===")
print(silver_df.count())


# Create Iceberg namespace
spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS local.ecommerce
""")


# Create Iceberg table
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.ecommerce.orders
    USING iceberg
    PARTITIONED BY (event_date)
    AS SELECT *
    FROM parquet.`/opt/project/data/silver/orders`
""")


print("\n=== Iceberg Table Created ===")

spark.sql("""
    SELECT *
    FROM local.ecommerce.orders
    LIMIT 10
""").show(truncate=False)


print("\n=== Iceberg Record Count ===")

count = spark.sql("""
    SELECT COUNT(*)
    FROM local.ecommerce.orders
""").collect()[0][0]

print(count)


spark.stop()