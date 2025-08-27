# settings.py
import os
from dotenv import load_dotenv

load_dotenv()

def _get_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except ValueError:
        return default

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "coldchain.readings")
KAFKA_CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "coldchain-producer")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "coldchain-consumer-group")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "coldchain")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")

TEMP_MIN_C = _get_float("TEMP_MIN_C", 2.0)
TEMP_MAX_C = _get_float("TEMP_MAX_C", 8.0)

PRODUCER_DEVICE_COUNT = int(os.getenv("PRODUCER_DEVICE_COUNT", "5"))
PRODUCER_SEND_INTERVAL_SEC = _get_float("PRODUCER_SEND_INTERVAL_SEC", 1.5)