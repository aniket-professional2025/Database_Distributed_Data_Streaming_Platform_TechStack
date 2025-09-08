# Importing Required Libraries
import time
import psycopg2
from cassandra.cluster import Cluster, ConsistencyLevel
from cassandra.query import SimpleStatement
import redis
from sqlalchemy import create_engine, text
from app_settings import POSTGRES_DSN, CASSANDRA_HOST, REDIS_HOST

# Function to Wait for Services to be Ready
RETRIES = 30
SLEEP = 5

# Define a function to wait for Postgres
def wait_for_postgres():
    last_exc = None
    for i in range(RETRIES):
        try:
            engine = create_engine(POSTGRES_DSN, connect_args={})
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print(f"[DB] Postgres connected successfully after {i+1} retries.")
            return engine
        except Exception as e:
            last_exc = e
            print(f"[DB] Attempt {i+1}/{RETRIES}: Postgres not ready, retrying in {SLEEP}s...")
            time.sleep(SLEEP)
    raise last_exc

# Define a function to wait for Cassandra
def wait_for_cassandra():
    last_exc = None
    for i in range(RETRIES):
        try:
            cluster = Cluster([CASSANDRA_HOST])
            session = cluster.connect()
            # Create keyspace and table if they don't exist
            session.execute("""
                CREATE KEYSPACE IF NOT EXISTS bidding WITH replication = {
                    'class': 'SimpleStrategy',
                    'replication_factor': '1'
                }
            """)
            session.set_keyspace('bidding')

            session.execute("""
                CREATE TABLE IF NOT EXISTS bid_logs (
                    bid_id text,
                    user_id text,
                    item_number text,
                    amount float,
                    status text,
                    reason text,
                    created_at timestamp,
                    PRIMARY KEY (bid_id)
                )
            """)
            print(f"[DB] Cassandra connected successfully after {i+1} retries.")
            return session
        except Exception as e:
            last_exc = e
            print(f"[DB] Attempt {i+1}/{RETRIES}: Cassandra not ready, retrying in {SLEEP}s...")
            time.sleep(SLEEP)
    raise last_exc

# Define a function to wait for Redis
def wait_for_redis():
    last_exc = None
    for i in range(RETRIES):
        try:
            r = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
            r.ping()
            print(f"[DB] Redis connected successfully after {i+1} retries.")
            return r
        except Exception as e:
            last_exc = e
            print(f"[DB] Attempt {i+1}/{RETRIES}: Redis not ready, retrying in {SLEEP}s...")
            time.sleep(SLEEP)
    raise last_exc