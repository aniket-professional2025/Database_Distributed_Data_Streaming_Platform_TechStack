# Importing Required Packages
import time
import random
from kafka_client import KafkaClient
from config import KAFKA_BOOTSTRAP, KAFKA_TOPIC
from utils import gen_tx_id

# Utility Functions: Getting a random user
def random_user(i):
    return f'user_{i}'

# Simulating Transactions
def run_simulator(num_users = 5, interval = 0.5, total = 5000):
    kc = KafkaClient(KAFKA_BOOTSTRAP, KAFKA_TOPIC)
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
        print('Producing', msg)
        kc.send(msg)
        time.sleep(interval)

if __name__ == '__main__':
    run_simulator()