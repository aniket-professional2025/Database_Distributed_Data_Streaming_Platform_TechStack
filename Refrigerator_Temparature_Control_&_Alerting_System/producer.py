# producer.py
import logging
import random
import signal
import sys
import time
from typing import List

from kafka  import KafkaProducer
import settings
import common

logger = logging.getLogger("producer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

_running = True

def _handle_sig(*_):
    global _running
    logger.info("Shutting down producer...")
    _running = False

signal.signal(signal.SIGINT, _handle_sig)
signal.signal(signal.SIGTERM, _handle_sig)

def build_producer() -> KafkaProducer:
    kwargs = {
        "bootstrap_servers": settings.KAFKA_BOOTSTRAP_SERVERS.split(","),
        "client_id": settings.KAFKA_CLIENT_ID,
        "acks": "all",
        "value_serializer": lambda v: v,  # already bytes
        "key_serializer": lambda v: v,    # bytes
        "retries": 5,
        "linger_ms": 10,
        "max_in_flight_requests_per_connection": 1,
    }
    # Optional SASL settings for secured clusters
    if settings.KAFKA_SECURITY_PROTOCOL and settings.KAFKA_SECURITY_PROTOCOL != "PLAINTEXT":
        kwargs.update({
            "security_protocol": settings.KAFKA_SECURITY_PROTOCOL,
            "sasl_mechanism": settings.KAFKA_SASL_MECHANISM or None,
            "sasl_plain_username": settings.KAFKA_SASL_USERNAME or None,
            "sasl_plain_password": settings.KAFKA_SASL_PASSWORD or None,
        })
    return KafkaProducer(**kwargs)

def _device_ids(n: int) -> List[str]:
    return [f"dev-{i+1:03d}" for i in range(n)]

def _package_id_for_device(device_id: str) -> str:
    # deterministic-ish mapping for demo
    return f"pkg-{device_id[-3:]}"

def main():
    producer = build_producer()
    topic = settings.KAFKA_TOPIC
    devices = _device_ids(settings.PRODUCER_DEVICE_COUNT)
    logger.info("Producer started. Topic=%s, devices=%s", topic, devices)

    try:
        while _running:
            device_id = random.choice(devices)
            package_id = _package_id_for_device(device_id)
            event = common.make_reading_event(device_id, package_id)

            key = device_id.encode("utf-8")
            value = common.dumps(event)

            future = producer.send(topic, key=key, value=value)
            # Optional: block for delivery report
            metadata = future.get(timeout=10)
            logger.info("Sent event %s to %s [%d] @ offset %d",
                        event["event_id"], metadata.topic, metadata.partition, metadata.offset)

            producer.flush()
            time.sleep(settings.PRODUCER_SEND_INTERVAL_SEC)
    except Exception as e:
        logger.exception("Producer error: %s", e)
    finally:
        try:
            producer.flush(10)
        except Exception:
            pass
        producer.close(10)
        logger.info("Producer closed.")

if __name__ == "__main__":
    main()