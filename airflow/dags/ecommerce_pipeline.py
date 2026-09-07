from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 

with DAG(
    dag_id="ecommerce_data_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ecommerce", "data-engineering"],
) as dag:

    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application="/opt/project/spark/jobs/bronze_to_silver.py",
        conn_id="spark_default",
        name="bronze_to_silver",
        verbose=True,
    )

    silver_quality = SparkSubmitOperator(
        task_id="silver_quality_check",
        application="/opt/project/quality/checks/validate_silver_quality.py",
        conn_id="spark_default",
        name="silver_quality_check",
        verbose=True,
    )

    silver_to_iceberg = SparkSubmitOperator(
        task_id="silver_to_iceberg",
        application="/opt/project/spark/jobs/silver_to_iceberg.py",
        conn_id="spark_default",
        name="silver_to_iceberg",
        verbose=True,
    )

    create_gold = SparkSubmitOperator(
        task_id="create_gold_daily_sales",
        application="/opt/project/spark/jobs/gold_daily_sales.py",
        conn_id="spark_default",
        name="create_gold_daily_sales",
        verbose=True,
    )

    gold_quality = SparkSubmitOperator(
        task_id="gold_quality_check",
        application="/opt/project/quality/checks/gold_quality.py",
        conn_id="spark_default",
        name="gold_quality_check",
        verbose=True,
    )

    bronze_to_silver >> silver_quality
    silver_quality >> silver_to_iceberg
    silver_quality >> create_gold
    create_gold >> gold_quality