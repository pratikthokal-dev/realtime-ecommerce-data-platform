from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ResetCDCPipelineWatermark")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "/opt/project/data/iceberg")
    .getOrCreate()
)

spark.sql("""
UPDATE local.ecommerce.cdc_pipeline_state
SET last_processed_offset = -1,
    updated_at = current_timestamp()
WHERE pipeline_name = 'orders_cdc'
  AND kafka_topic = 'ecommerce.ecommerce_platform.orders_v2'
  AND kafka_partition = 0
""")

print("=== CURRENT CDC STATE ===")

spark.sql("""
SELECT *
FROM local.ecommerce.cdc_pipeline_state
WHERE pipeline_name = 'orders_cdc'
""").show(truncate=False)

spark.stop()
