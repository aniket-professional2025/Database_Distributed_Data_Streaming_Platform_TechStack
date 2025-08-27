# common.py
import json
import time
import uuid
import random
from typing import Dict, Any
from datetime import datetime, timezone

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def to_iso(ts: datetime) -> str:
    return ts.isoformat()

def from_iso(s: str) -> datetime:
    return datetime.fromisoformat(s)

def make_reading_event(device_id: str, package_id: str) -> Dict[str, Any]:
    # Random-ish coordinates (somewhere around Europe/India corridor for demo)
    base_lat, base_lon = 30.0, 20.0
    lat_jitter = random.uniform(-15.0, 15.0)
    lon_jitter = random.uniform(-20.0, 20.0)
    # Safe range center +/- some variance; occasionally produce out-of-range
    temp = round(random.normalvariate(5.0, 2.0), 2)
    # 10% chance to push out of range strongly
    if random.random() < 0.10:
        temp += random.choice([-6.0, +6.0])

    event = {
        "event_id": str(uuid.uuid4()),
        "type": "sensor_reading.v1",
        "device_id": device_id,
        "package_id": package_id,
        "temperature_c": float(temp),
        "latitude": round(base_lat + lat_jitter, 6),
        "longitude": round(base_lon + lon_jitter, 6),
        "event_ts": to_iso(now_utc()),
        "producer_ts": to_iso(now_utc())
    }
    return event

def dumps(event: Dict[str, Any]) -> bytes:
    return json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

def loads(b: bytes) -> Dict[str, Any]:
    return json.loads(b.decode("utf-8"))    