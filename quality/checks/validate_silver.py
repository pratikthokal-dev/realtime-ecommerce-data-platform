from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    upper
)


SILVER_PATH = "/opt/project/data/silver/debezium_orders"


def main():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("ValidateSilverData")
        .config("spark.driver.memory", "512m")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
)

    silver_df = spark.read.parquet(SILVER_PATH)

    total_records = silver_df.count()

    print("\n" + "=" * 60)
    print("SILVER DATA QUALITY VALIDATION")
    print("=" * 60)

    print(f"Total Silver records: {total_records}")

    if total_records == 0:
        raise ValueError("Silver layer contains no records.")

    # ---------------------------------------------------------
    # 1. Required-field validation
    # ---------------------------------------------------------

    required_columns = [
        "order_id",
        "customer_id",
        "order_status",
        "operation",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
    ]

    print("\n[1] Required-field validation")

    for column_name in required_columns:
        null_count = silver_df.filter(
            col(column_name).isNull()
        ).count()

        print(f"{column_name}: {null_count} null records")

        if null_count > 0:
            raise ValueError(
                f"Data quality failed: {column_name} contains NULL values."
            )

    # ---------------------------------------------------------
    # 2. Order amount validation
    # ---------------------------------------------------------

    print("\n[2] Amount validation")

    invalid_amounts = silver_df.filter(
    (col("operation") != "d")
    & (
        col("total_amount").isNull()
        | (col("total_amount") < 0)
    )
    ).count()

    print(f"Invalid amounts: {invalid_amounts}")

    if invalid_amounts > 0:
        raise ValueError(
            "Data quality failed: invalid total_amount values found."
        )

    # ---------------------------------------------------------
    # 3. Order status validation
    # ---------------------------------------------------------

    print("\n[3] Order status validation")

    valid_statuses = [
        "PLACED",
        "CONFIRMED",
        "SHIPPED",
        "DELIVERED",
        "CANCELLED",
    ]

    invalid_statuses = silver_df.filter(
        ~upper(col("order_status")).isin(valid_statuses)
    ).count()

    print(f"Invalid order statuses: {invalid_statuses}")

    if invalid_statuses > 0:
        raise ValueError(
            "Data quality failed: invalid order_status values found."
        )

    # ---------------------------------------------------------
    # 4. CDC operation validation
    # ---------------------------------------------------------

    print("\n[4] CDC operation validation")

    valid_operations = ["c", "u", "d", "r"]

    invalid_operations = silver_df.filter(
        ~col("operation").isin(valid_operations)
    ).count()

    print(f"Invalid CDC operations: {invalid_operations}")

    if invalid_operations > 0:
        raise ValueError(
            "Data quality failed: invalid CDC operation values found."
        )

    # ---------------------------------------------------------
    # 5. Duplicate CDC event validation
    # ---------------------------------------------------------

    print("\n[5] Duplicate CDC event validation")

    duplicate_events = (
        silver_df
        .groupBy(
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
        )
        .agg(
            count("*").alias("record_count")
        )
        .filter(col("record_count") > 1)
        .count()
    )

    print(f"Duplicate Kafka events: {duplicate_events}")

    if duplicate_events > 0:
        raise ValueError(
            "Data quality failed: duplicate Kafka events detected."
        )

    # ---------------------------------------------------------
    # 6. CDC operation summary
    # ---------------------------------------------------------

    print("\n[6] CDC operation summary")

    (
        silver_df
        .groupBy("operation")
        .count()
        .orderBy("operation")
        .show()
    )

    # ---------------------------------------------------------
    # 7. Data quality metrics
    # ---------------------------------------------------------

    print("\n[7] Data quality metrics")

    quality_metrics = silver_df.select(
        count("*").alias("total_records"),
        spark_sum(
            col("order_id").isNull().cast("int")
        ).alias("null_order_ids"),
        spark_sum(
            col("customer_id").isNull().cast("int")
        ).alias("null_customer_ids"),
    ).collect()[0]

    print(f"Total records     : {quality_metrics['total_records']}")
    print(f"Null order IDs    : {quality_metrics['null_order_ids']}")
    print(f"Null customer IDs : {quality_metrics['null_customer_ids']}")

    print("\n" + "=" * 60)
    print("DATA QUALITY VALIDATION PASSED")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()