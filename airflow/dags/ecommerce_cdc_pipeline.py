from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    dag_id="ecommerce_cdc_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ecommerce", "cdc", "spark", "iceberg"],
) as dag:

    def check_bronze_ready():
        import os

        bronze_path = "/opt/project/data/bronze/debezium_orders"

        if not os.path.exists(bronze_path):
            raise FileNotFoundError(
                f"Bronze data path does not exist: {bronze_path}"
        )

        files = [
            file
            for file in os.listdir(bronze_path)
            if file.endswith(".parquet")
        ]

        if not files:
            raise RuntimeError(
            "Bronze data path exists but contains no Parquet files."
        )

        print(f"Bronze layer ready: {len(files)} Parquet files found.")

    bronze_ready = PythonOperator(
        task_id="bronze_ready",
        python_callable=check_bronze_ready,
    )

    cdc_silver = SparkSubmitOperator(
        task_id="cdc_silver",
        application="/opt/project/spark/jobs/debezium_orders_silver.py",
        conn_id="spark_default",
        name="cdc_silver",
        driver_memory="512m",
        executor_memory="1g",
        executor_cores=2,
        num_executors=1,
        verbose=True,
    )

    def validate_cdc_silver():
        import subprocess

        subprocess.run(
            [
                "python",
                "/opt/project/quality/checks/validate_silver.py",
            ],
            check=True,
        )

    cdc_quality = PythonOperator(
        task_id="cdc_quality_check",
        python_callable=validate_cdc_silver,
    )

    cdc_iceberg = SparkSubmitOperator(
        task_id="cdc_iceberg_merge",
        application="/opt/project/spark/jobs/debezium_orders_iceberg_incremental.py",
        conn_id="spark_default",
        name="cdc_iceberg_merge",
        jars="/opt/airflow/jars/iceberg-spark-runtime-4.0_2.13-1.10.1.jar",
        driver_memory="512m",
        executor_memory="1g",
        executor_cores=2,
        num_executors=1,
        verbose=True,
    )


    gold_prepare = SparkSubmitOperator(
        task_id="gold_prepare",
        application="/opt/project/spark/jobs/gold_prepare.py",
        conn_id="spark_default",
        name="gold_prepare",
        jars="/opt/airflow/jars/iceberg-spark-runtime-4.0_2.13-1.10.1.jar",
        driver_memory="512m",
        verbose=True,

)

    gold_daily_sales = SparkSubmitOperator(
        task_id="gold_daily_sales",
        application="/opt/project/spark/jobs/gold_daily_sales.py",
        conn_id="spark_default",
        name="gold_daily_sales",
        jars="/opt/airflow/jars/iceberg-spark-runtime-4.0_2.13-1.10.1.jar",
        driver_memory="512m",
        verbose=True,
)


    bronze_ready >> cdc_silver >> cdc_quality >> cdc_iceberg >> gold_prepare >> gold_daily_sales
