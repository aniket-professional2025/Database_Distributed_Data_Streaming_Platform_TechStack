# Importing Required Libraries
import os
from dotenv import load_dotenv
load_dotenv()

# Setting Kafka Parameters
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "upi_transactions")

# Setting PostgreSQL Parameters
POSTGRES = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'db': os.getenv('POSTGRES_DB', 'upi_db'),
    'user': os.getenv('POSTGRES_USER', 'upi_user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'upi_pass')
}

# Setting Redis Parameters
REDIS = {
    'host': os.getenv('REDIS_HOST', 'localhost'),
    'port': int(os.getenv('REDIS_PORT', 6379))
}

# Setting Cassandra Parameters
CASSANDRA = {
    'hosts': [os.getenv('CASSANDRA_HOST', 'cassandra')]
}

# Setting Application Limits
MAX_TXN_AMOUNT = int(os.getenv('MAX_TXN_AMOUNT', 150000))
DAILY_TXN_LIMIT = int(os.getenv('DAILY_TXN_LIMIT', 500000))