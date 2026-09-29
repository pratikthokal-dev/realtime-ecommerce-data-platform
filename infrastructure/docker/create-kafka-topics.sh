#!/bin/bash

set -e

BOOTSTRAP_SERVER="kafka:29092"

echo "Waiting for Kafka..."

until kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --list >/dev/null 2>&1
do
    sleep 2
done

echo "Kafka is ready."

kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" \
  --create \
  --if-not-exists \
  --topic ecommerce.ecommerce_platform.orders_v2 \
  --partitions 1 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000

kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" \
  --create \
  --if-not-exists \
  --topic ecommerce.orders.dlq \
  --partitions 1 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000

kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" \
  --create \
  --if-not-exists \
  --topic debezium_connect_configs \
  --partitions 1 \
  --replication-factor 1 \
  --config cleanup.policy=compact

kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" \
  --create \
  --if-not-exists \
  --topic debezium_connect_offsets \
  --partitions 1 \
  --replication-factor 1 \
  --config cleanup.policy=compact

kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" \
  --create \
  --if-not-exists \
  --topic debezium_connect_status \
  --partitions 1 \
  --replication-factor 1 \
  --config cleanup.policy=compact

echo "Kafka topics initialized successfully."

