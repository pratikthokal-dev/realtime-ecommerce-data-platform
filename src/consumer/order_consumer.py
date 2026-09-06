from kafka import KafkaConsumer
from kafka import KafkaProducer
import json

from src.database.mysql_connection import get_mysql_connection


# MySQL Connection

connection = get_mysql_connection()

cursor = connection.cursor()

dlq_producer = KafkaProducer(

    bootstrap_servers="localhost:9092",

    value_serializer=lambda data:
        json.dumps(data, default=str).encode("utf-8")

)
# Kafka Consumer

consumer = KafkaConsumer(

    "ecommerce.orders",

    bootstrap_servers="localhost:9092",

    auto_offset_reset="earliest",

    enable_auto_commit=True,

    group_id="order-analytics-group",

    value_deserializer=lambda data:
        json.loads(data.decode("utf-8"))

)


print("Order consumer started...")


for message in consumer:

    order = message.value

    print("\nNew Order Event:")
    print(order)


    try:

        if "order_id" not in order:
            raise Exception("Missing order_id")


        order_id = order["order_id"]

        customer_id = order["customer_id"]

        amount = float(order["amount"])

        status = order["status"]


        cursor.execute(
            """
            INSERT INTO order_analytics
            (
            order_id,
            customer_id,
            amount,
            status
            )
            VALUES (%s,%s,%s,%s)
            """,
            (
                order_id,
                customer_id,
                amount,
                status
            )
        )


        connection.commit()

        print("Stored in analytics table ✅")


    except Exception as error:

        print("Error processing event:", error)

        dlq_producer.send(
            "ecommerce.orders.dlq",
            {
                "original_event": order,
                "error": str(error)
            }
        )

        dlq_producer.flush()

        print("Sent to DLQ ❌")