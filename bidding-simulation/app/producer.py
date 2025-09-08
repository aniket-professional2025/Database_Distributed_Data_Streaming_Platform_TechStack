# Importing Required Libraries
from kafka import KafkaProducer, errors
import json, time
from app_settings import KAFKA_BOOTSTRAP

# Function to Get Kafka Producer Instance
def get_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        linger_ms=10
    )

# Function to Publish Bid Messages to Kafka
def publish_bid(producer, bid):
    # bid is a dict with keys: id, user_id, item_number, amount
    producer.send('bids', value=bid)
    producer.flush()