import json
import subprocess


SERVICES = [
    "ecommerce-kafka",
    "ecommerce-kafka-ui",
    "ecommerce-spark",
    "ecommerce-spark-worker",
    "ecommerce-zookeeper",
    "ecommerce-airflow",
]

DATA_PATHS = [
    "/opt/project/data/bronze/debezium_orders",
    "/opt/project/data/silver/debezium_orders",
    "/opt/project/data/iceberg",
]

KAFKA_CONTAINER = "ecommerce-kafka"

KAFKA_TOPICS = [
    "ecommerce.ecommerce_platform.orders_v2",
    "ecommerce.orders.dlq",
]

AIRFLOW_CONTAINER = "ecommerce-airflow"
AIRFLOW_DAG = "ecommerce_cdc_pipeline"

def run_command(command):
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        shell=True,
    )

    return result.returncode, result.stdout.strip(), result.stderr.strip()


def check_services():
    print("\n[1] Docker services")

    code, output, error = run_command(
    'docker ps --format "{{.Names}}|{{.Status}}"'
)

    if code != 0:
        print("Docker check: FAILED")
        print(error)
        return False

    running = {}

    for line in output.splitlines():
        if "|" in line:
            name, status = line.split("|", 1)
            running[name] = status

    healthy = True

    for service in SERVICES:
        if service in running:
            print(f"OK   {service} - {running[service]}")
        else:
            print(f"FAIL {service} - not running")
            healthy = False

    return healthy


def check_data_layers():
    print("\n[2] Data layers")

    healthy = True

    for path in DATA_PATHS:
        command = (
            f"docker exec ecommerce-spark "
            f"test -d {path}"
        )

        code, _, _ = run_command(command)

        if code == 0:
            print(f"OK   {path}")
        else:
            print(f"FAIL {path}")
            healthy = False

    return healthy

def check_kafka():
    print("\n[3] Kafka topics")

    healthy = True

    for topic in KAFKA_TOPICS:
        command = (
            f"docker exec {KAFKA_CONTAINER} "
            f"kafka-topics --bootstrap-server localhost:9092 "
            f"--describe --topic {topic}"
        )

        code, output, error = run_command(command)

        if code == 0 and "PartitionCount:" in output:
            print(f"OK   {topic}")
        else:
            print(f"FAIL {topic}")
            if error:
                print(f"     {error}")
            healthy = False

    return healthy

def check_airflow():
    print("\n[4] Airflow DAG")

    command = (
        f"docker exec {AIRFLOW_CONTAINER} "
        f"airflow dags list --output json"
    )

    code, output, error = run_command(command)

    if code != 0:
        print("FAIL Airflow DAG check")
        print(error)
        return False

    try:
        dags = json.loads(output)
    except json.JSONDecodeError:
        print("FAIL Airflow DAG check - invalid JSON response")
        return False

    for dag in dags:
        if dag.get("dag_id") == AIRFLOW_DAG:
            print(f"OK   {AIRFLOW_DAG} - DAG found")
            return True

    print(f"FAIL {AIRFLOW_DAG} - DAG not found")
    return False

def check_airflow_latest_run():
    print("\n[5] Airflow latest DAG run")

    command = (
        f"docker exec {AIRFLOW_CONTAINER} "
        f"airflow dags list-runs "
        f"--dag-id {AIRFLOW_DAG} "
        f"--output json"
    )

    code, output, error = run_command(command)

    if code != 0:
        print("FAIL Latest DAG run check")
        print(error)
        return False

    try:
        runs = json.loads(output)
    except json.JSONDecodeError:
        print("FAIL Latest DAG run check - invalid JSON response")
        return False

    if not runs:
        print(f"WARN {AIRFLOW_DAG} - no DAG runs found")
        return True

    latest_run = runs[0]
    state = latest_run.get("state")
    run_id = latest_run.get("run_id")

    if state == "success":
        print(f"OK   {AIRFLOW_DAG} - latest run: {state}")
        print(f"     Run ID: {run_id}")
        return True

    print(f"FAIL {AIRFLOW_DAG} - latest run: {state}")
    print(f"     Run ID: {run_id}")
    return False

def main():
    print("=" * 60)
    print("REAL-TIME E-COMMERCE PIPELINE HEALTH CHECK")
    print("=" * 60)

    services_ok = check_services()
    data_ok = check_data_layers()
    kafka_ok = check_kafka()
    airflow_ok = check_airflow()
    airflow_run_ok = check_airflow_latest_run()

    print("\n" + "=" * 60)

    if services_ok and data_ok and kafka_ok and airflow_ok and airflow_run_ok:
        print("PIPELINE STATUS: HEALTHY")
        return 0

    print("PIPELINE STATUS: UNHEALTHY")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())