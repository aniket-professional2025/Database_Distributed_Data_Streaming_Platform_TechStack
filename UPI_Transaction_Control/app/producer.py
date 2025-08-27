# Importing Required Packages
import time
import random
from kafka_client import KafkaClient
from config import KAFKA_BOOTSTRAP, KAFKA_TOPIC
from utils import gen_tx_id
from kafka.errors import NoBrokersAvailable

# Utility Functions: Getting a random user
def random_user(i):
    return f'user_{i}'

# Define a function to Connect the Kafka
def connect_kafka(bootstrap, topic, retries = 50, delay = 5):
    for attempt in range(retries):
        try:
            print(f"Connecting to Kafka... attempt {attempt+1}/{retries}")
            return KafkaClient(bootstrap, topic)
        except NoBrokersAvailable:
            print("Kafka not ready, retrying in", delay, "seconds...")
            time.sleep(delay)

    raise Exception("Failed to connect to Kafka after multiple attempts")

# Simulating Transactions
def run_simulator(num_users = 5, interval = 0.5, total = 800, log_every = 100):
    kc = connect_kafka(KAFKA_BOOTSTRAP, KAFKA_TOPIC)
    print("Starting Producer")
    for i in range(total):
        payer = random_user(random.randint(1, num_users))
        payee = random_user(random.randint(1, num_users))
        if payer == payee:
            continue
        amount = random.randint(1, 200000)
        msg = {
            'tx_id': gen_tx_id(),
            'payer_id': payer,
            'payee_id': payee,
            'amount': amount,
            'currency': 'INR',
            'created_at': None
        }
        
        kc.send(msg)

        if (i+1) % log_every == 0:
            print(f"Produced {i+1} messages so far... (latest tx_id={msg['tx_id']})")

        time.sleep(interval)

if __name__ == '__main__':
    run_simulator()