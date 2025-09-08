# Importing Required Libraries
import os

# Setting up environment variables with defaults
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
POSTGRES_DSN = os.environ.get("POSTGRES_DSN", "postgresql://postgres:postgres@postgres:5432/bidding")
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
PROM_METRICS_PORT = int(os.environ.get("PROM_METRICS_PORT", 8000))