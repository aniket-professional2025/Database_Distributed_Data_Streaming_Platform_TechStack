# consumer.py
import logging
from datetime import datetime
from typing import Dict, Any

from kafka import KafkaConsumer
import settings
import common
import db

logger = logging.getLogger("consumer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

def build_consumer() -> KafkaConsumer:
    kwargs = {
        "bootstrap_servers": settings.KAFKA_BOOTSTRAP_SERVERS.split(","),
        "group_id": settings.KAFKA_GROUP_ID,
        "enable_auto_commit": False,  # we'll commit after successful DB write
        "auto_offset_reset": "earliest",
        "value_deserializer": lambda b: b,  # raw bytes, we parse manually
        "key_deserializer": lambda b: b,
        "consumer_timeout_ms": 1000,
        "max_poll_records": 50,
    }
    if settings.KAFKA_SECURITY_PROTOCOL and settings.KAFKA_SECURITY_PROTOCOL != "PLAINTEXT":
        kwargs.update({
            "security_protocol": settings.KAFKA_SECURITY_PROTOCOL,
            "sasl_mechanism": settings.KAFKA_SASL_MECHANISM or None,
            "sasl_plain_username": settings.KAFKA_SASL_USERNAME or None,
            "sasl_plain_password": settings.KAFKA_SASL_PASSWORD or None,
        })
    return KafkaConsumer(settings.KAFKA_TOPIC, **kwargs)

def _validate_event(evt: Dict[str, Any]) -> None:
    required = ["event_id", "type", "device_id", "package_id", "temperature_c", "latitude", "longitude", "event_ts"]
    missing = [k for k in required if k not in evt]
    if missing:
        raise ValueError(f"Missing fields: {missing}")
    if not isinstance(evt["temperature_c"], (int, float)):
        raise ValueError("temperature_c must be a number")

def _maybe_alert(reading_id: int, evt: Dict[str, Any]):
    temp = float(evt["temperature_c"])
    if temp < settings.TEMP_MIN_C or temp > settings.TEMP_MAX_C:
        reason = f"Temperature out of range [{settings.TEMP_MIN_C}C - {settings.TEMP_MAX_C}C]"
        alert_id = db.insert_alert(
            device_id=evt["device_id"],
            package_id=evt["package_id"],
            temperature_c=temp,
            threshold_min=settings.TEMP_MIN_C,
            threshold_max=settings.TEMP_MAX_C,
            reason=reason,
            reading_id=reading_id,
        )
        logger.warning("ALERT %s for reading %s: %s (%.2fC)", alert_id, reading_id, reason, temp)

def main():
    db.init_db()
    consumer = build_consumer()
    logger.info("Consumer started. Subscribed to %s", settings.KAFKA_TOPIC)

    try:
        while True:
            batch = consumer.poll(timeout_ms=1000)
            if not batch:
                continue

            for tp, messages in batch.items():
                for msg in messages:
                    try:
                        evt = common.loads(msg.value)
                        _validate_event(evt)

                        event_ts = common.from_iso(evt["event_ts"])
                        reading_id = db.insert_reading(
                            device_id=evt["device_id"],
                            package_id=evt["package_id"],
                            temperature_c=float(evt["temperature_c"]),
                            latitude=float(evt["latitude"]),
                            longitude=float(evt["longitude"]),
                            event_ts=event_ts,
                        )
                        _maybe_alert(reading_id, evt)
                        consumer.commit()  # commit after successful insert (simple at-least-once)
                        logger.info("Stored reading %s offset=%d partition=%d", reading_id, msg.offset, msg.partition)
                    except Exception as e:
                        logger.exception("Failed to process message at offset=%d: %s", msg.offset, e)
                        # Skip commit so message can be retried; optionally send to a dead-letter topic here.
    except KeyboardInterrupt:
        logger.info("Consumer interrupted, closing...")
    finally:
        consumer.close()
        logger.info("Consumer closed.")

if __name__ == "__main__":
    main()