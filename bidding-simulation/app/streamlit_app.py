# Importing Required Libraries
import streamlit as st
import time
import threading
import pandas as pd
from producer import get_producer, publish_bid
from utils import gen_bid_id, now_iso
from db import wait_for_postgres, wait_for_redis, wait_for_cassandra
from bid_validate import run_consumer
from track import start_tracker_in_thread
from kafka import KafkaConsumer
import json
from app_settings import KAFKA_BOOTSTRAP
from collections import deque
from streamlit_autorefresh import st_autorefresh

# Page setup
st.set_page_config(page_title="Bid Simulation", layout="wide")
st.title("Bid Simulation Control Panel")

# Auto refresh every 5 seconds
st_autorefresh(interval=5000, limit=None, key="refresh")

# Background services: Start tracker thread (reads redis and updates gauges)
start_tracker_in_thread()

# Start bid consumer in background thread
def start_consumer_thread():
    t = threading.Thread(target=run_consumer, daemon=True)
    t.start()
    return t

# Start background consumer thread only once
start_consumer_thread()

# Kafka producer
producer = get_producer()

# Kafka Notifications Consumer (using deque)
if "notif_buffer" not in st.session_state:
    st.session_state.notif_buffer = deque(maxlen=20)

# Function to consume notifications in background
def consume_notifications(buffer):
    consumer = KafkaConsumer(
        "notifications",
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="streamlit-ui"
    )
    for msg in consumer:
        buffer.append(msg.value)

# Start background thread only once
if "notif_thread" not in st.session_state:
    t = threading.Thread(target=consume_notifications, args=(st.session_state.notif_buffer,), daemon=True)
    t.start()
    st.session_state.notif_thread = t

# Sidebar: Bid submission
with st.sidebar:
    st.header("New Bid")
    user_id = st.text_input("User ID", value="Enter User ID")
    item_number = st.text_input("Item Number", value="Enter Item Number")
    amount = st.number_input("Amount", min_value=0.0, step=1.0)
    submitted = st.button("Submit Bid")

# Log container
log_container = st.empty()

# On bid submission
if submitted:
    bid = {
        "id": gen_bid_id(),
        "user_id": user_id,
        "item_number": item_number,
        "amount": float(amount),
        "ts": now_iso()
    }
    publish_bid(producer, bid)
    log_container.info(f"Published bid {bid['id']} for item {item_number} amount {amount}")

# Database loaders: Postgres
def load_postgres_table():
    try:
        engine = wait_for_postgres()
        with engine.connect() as conn:
            users_df = pd.read_sql("SELECT * FROM users ORDER BY created_at DESC LIMIT 50", conn)
            bids_df = pd.read_sql("SELECT * FROM bids ORDER BY created_at DESC LIMIT 50", conn)
        return users_df, bids_df
    except Exception:
        return None, None

# Database loaders: Redis
def load_redis_snapshot():
    try:
        r = wait_for_redis()
        items = {}
        for key in r.scan_iter(match='item:*'):
            item = key.split(':', 1)[1]
            vals = r.zrange(key, 0, -1, withscores=True)
            items[item] = vals[-5:]  # show top 5
        return items
    except Exception:
        return {}

# Database loaders: Cassandra
def load_cassandra_audit():
    try:
        session = wait_for_cassandra()
        rows = session.execute("SELECT * FROM bid_logs LIMIT 50")
        return pd.DataFrame(rows)
    except Exception:
        return None

# Display database snapshots (row-wise)
st.header("Databases snapshot")

# Postgres Users & Bids
st.subheader("Postgres - Users & Bids")
u_df, b_df = load_postgres_table()
if u_df is not None:
    st.write("Users")
    st.dataframe(u_df)
    st.write("Bids")
    st.dataframe(b_df)
else:
    st.info("Postgres not ready or empty")

# Redis - Current Highest Bids
st.subheader("Redis - Current Highest Bids")
r_snapshot = load_redis_snapshot()
if r_snapshot:
    for item, vals in r_snapshot.items():
        st.write(f"Item {item}: {vals}")
else:
    st.info("Redis empty or not ready")

# Kafka Notifications
st.subheader("Kafka Notifications")
if st.session_state.notif_buffer:
    st.dataframe(list(st.session_state.notif_buffer))
else:
    st.info("No notifications yet")

# Cassandra Audit Log
st.subheader("Cassandra - Audit Log")
c_df = load_cassandra_audit()
if c_df is not None and not c_df.empty:
    st.dataframe(c_df)
else:
    st.info("Cassandra empty or not ready")