import json
from datetime import datetime, timezone
from kafka import KafkaProducer

KAFKA_HOST = "localhost"
KAFKA_TOPIC_NAME = "service_backend_feed"

class MessageProducer:
    broker = None
    topic = None
    producer = None

    def __init__(self, broker, topic):
        self.broker = broker
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=self.broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries = 3
        )

    def send_msg(self, msg):
        future = self.producer.send(self.topic, msg)
        self.producer.flush()
        # Куда именно легло сообщение: партиция и offset внутри неё
        return future.get(timeout=10)

kafka_client = MessageProducer(
    f"{KAFKA_HOST}:9092",
    KAFKA_TOPIC_NAME
)

meta = kafka_client.send_msg({
    "created_at": datetime.now(timezone.utc).isoformat(),
    "model_name": "__model",
    "method": "__method",
    "image_input": "__uuid.__fileExt",
    "image_output": "__output"
})
print(f">>> Sent: topic={meta.topic} partition={meta.partition} offset={meta.offset}")
