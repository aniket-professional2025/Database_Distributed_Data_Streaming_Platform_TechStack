# Importing Required Libraries
from kafka import KafkaProducer
import json
from app_settings import KAFKA_BOOTSTRAP

# Function to Send Notification Messages to Kafka
def send_notification(payload: dict):
    p = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        linger_ms=10
    )
    p.send('notifications', value=payload)
    p.flush()
    p.close()