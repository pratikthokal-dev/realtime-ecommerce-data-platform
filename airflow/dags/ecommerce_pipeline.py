from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
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
    def run_silver_quality():
        import subprocess

        subprocess.run(
        [
            "python",
            "/opt/project/quality/checks/validate_silver.py"
        ],
        check=True
    )


    silver_quality = PythonOperator(
        task_id="silver_quality_check",
        python_callable=run_silver_quality,
)
    
    silver_to_iceberg = SparkSubmitOperator(
        task_id="silver_to_iceberg",
        application="/opt/project/spark/jobs/silver_to_iceberg.py",
        conn_id="spark_default",
        name="silver_to_iceberg",
        jars="/opt/airflow/jars/iceberg-spark-runtime-4.0_2.13-1.10.1.jar",
        verbose=True,
)

    create_gold = SparkSubmitOperator(
        task_id="create_gold_daily_sales",
        application="/opt/project/spark/jobs/gold_daily_sales.py",
        conn_id="spark_default",
        name="create_gold_daily_sales",
        verbose=True,
    )

    def run_gold_quality():
        import subprocess

        subprocess.run(
        [
            "python",
            "/opt/project/quality/checks/gold_quality.py"
        ],
        check=True
    )

    gold_quality = PythonOperator(
    task_id="gold_quality_check",
    python_callable=run_gold_quality,
)

    bronze_to_silver >> silver_quality
    silver_quality >> silver_to_iceberg
    silver_quality >> create_gold
    create_gold >> gold_quality