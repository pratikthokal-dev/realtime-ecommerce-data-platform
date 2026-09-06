import json
from kafka import KafkaProducer


class JsonSerializer:

    def __call__(self, data):

        return json.dumps(
            data,
            default=str
        ).encode("utf-8")


def get_kafka_producer():

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=JsonSerializer()
    )

    return producer