# Importing Required Packages
from kafka import KafkaProducer, KafkaConsumer
import json

# Kafka Client Class
class KafkaClient:
    def __init__(self, bootstrap_servers, topic):
        self.bootstrap = bootstrap_servers
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=self.bootstrap, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def send(self, message):
        self.producer.send(self.topic, message)
        self.producer.flush()

    def consumer(self, group_id='upi_group'):
        return KafkaConsumer(self.topic, bootstrap_servers=self.bootstrap, group_id=group_id, value_deserializer=lambda m: json.loads(m.decode('utf-8')))
