from kafka import KafkaProducer
import json
import time


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)


order = {
    "order_id": 1001,
    "customer_id": 15,
    "product": "Wireless Headphones",
    "amount": 3000,
    "status": "PLACED"
}


producer.send(
    "ecommerce.orders",
    order
)

producer.flush()

print("Order sent successfully!")