# Importing Required Packages
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from contextlib import contextmanager
from typing import Optional
import datetime
import logging
import settings

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

_POOL: Optional[ThreadedConnectionPool] = None

def get_pool() -> ThreadedConnectionPool:
    global _POOL
    if _POOL is None:
        dsn = f"host={settings.PG_HOST} port={settings.PG_PORT} dbname={settings.PG_DB} user={settings.PG_USER} password={settings.PG_PASSWORD}"
        _POOL = ThreadedConnectionPool(minconn=1, maxconn=10, dsn=dsn)
        logger.info("Initialized PostgreSQL connection pool.")
    return _POOL

@contextmanager
def get_conn():
    pool = get_pool()
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)

def init_db():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS sensor_readings (
            id BIGSERIAL PRIMARY KEY,
            device_id TEXT NOT NULL,
            package_id TEXT NOT NULL,
            temperature_c DOUBLE PRECISION NOT NULL,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            event_ts TIMESTAMPTZ NOT NULL
        );
        """)
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_sensor_readings_device_ts
        ON sensor_readings (device_id, event_ts DESC);
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id BIGSERIAL PRIMARY KEY,
            device_id TEXT NOT NULL,
            package_id TEXT NOT NULL,
            temperature_c DOUBLE PRECISION NOT NULL,
            threshold_min DOUBLE PRECISION NOT NULL,
            threshold_max DOUBLE PRECISION NOT NULL,
            reason TEXT NOT NULL,
            reading_id BIGINT REFERENCES sensor_readings(id) ON DELETE CASCADE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)
        conn.commit()
        logger.info("Ensured tables exist.")

def insert_reading(
    device_id: str,
    package_id: str,
    temperature_c: float,
    latitude: float,
    longitude: float,
    event_ts: datetime.datetime
) -> int:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO sensor_readings (device_id, package_id, temperature_c, latitude, longitude, event_ts)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id;
        """, (device_id, package_id, temperature_c, latitude, longitude, event_ts))
        reading_id = cur.fetchone()[0]
        conn.commit()
        return reading_id

def insert_alert(
    device_id: str,
    package_id: str,
    temperature_c: float,
    threshold_min: float,
    threshold_max: float,
    reason: str,
    reading_id: Optional[int]
) -> int:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO alerts (device_id, package_id, temperature_c, threshold_min, threshold_max, reason, reading_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
        """, (device_id, package_id, temperature_c, threshold_min, threshold_max, reason, reading_id))
        alert_id = cur.fetchone()[0]
        conn.commit()
        return alert_id
