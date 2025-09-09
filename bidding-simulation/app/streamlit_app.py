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
from prometheus_metrics import start_metrics_server, BIDS_TOTAL, BIDS_SUCCESS, BIDS_FAILED, HIGHEST_BID, BID_PROCESSING_TIME

# Use st.session_state to ensure the metrics server is started only once
if "metrics_server_started" not in st.session_state:
    try:
        start_metrics_server()
        st.session_state.metrics_server_started = True
        print("Prometheus metrics server started successfully.")
    except OSError as e:
        if "Address already in use" in str(e):
            print("Prometheus metrics server is already running.")
            st.session_state.metrics_server_started = True
        else:
            raise e

# Page setup
st.set_page_config(page_title="Bid Simulation", layout="wide")
st.markdown("<h1>Bid Simulation Control Panel</h1>", unsafe_allow_html=True)

# Initialize state with a counter for log steps
if "log_step" not in st.session_state:
    st.session_state.log_step = -1 # -1 means no logging is active
if "log_messages" not in st.session_state:
    st.session_state.log_messages = []

# Log messages template
log_messages_template = [
    "Bid submitted...",
    "Activating Kafka producer...",
    "Consumed by Kafka consumer...",
    "Stored in Postgres 'users' and 'bids' tables...",
    "Bid validation check...",
    "Audit logs are stored in Cassandra...",
    "Bids tracking in process...",
    "Successfully updated Redis with key-value pair...",
    "Data flow completed. All databases updated."
]

# The st_autorefresh is the key to this solution
refresh_interval = 3000 # 3 seconds
if st.session_state.log_step >= 0 and st.session_state.log_step < len(log_messages_template):
    st_autorefresh(interval=refresh_interval, key=f"refresh_log_{st.session_state.log_step}")
else:
    st_autorefresh(interval=5000, key="refresh_tables")

# Live Logs section
st.markdown("<h3 style='margin-top: 0px;'>Live Bid Process Logs</h3>", unsafe_allow_html=True)
log_placeholder = st.empty()

# Background services: Start tracker thread (reads redis and updates gauges)
start_tracker_in_thread()

# Start bid consumer in background thread
def start_consumer_thread():
    t = threading.Thread(target=run_consumer, daemon=True)
    t.start()
    return t

# Start background consumer thread only once
if "consumer_thread" not in st.session_state:
    st.session_state.consumer_thread = start_consumer_thread()

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
    st.markdown("<h3>New Bid</h3>", unsafe_allow_html=True)
    user_id = st.text_input("User ID", value="Enter User ID")
    item_number = st.text_input("Item Number", value="Enter Item Number")
    amount = st.number_input("Amount", min_value=0.0, step=1.0)
    submitted = st.button("Submit Bid")

# On bid submission
if submitted:
    # Reset the log state
    st.session_state.log_step = 0
    st.session_state.log_messages = []
    
    # Publish the bid to Kafka
    bid = {
        "id": gen_bid_id(),
        "user_id": user_id,
        "item_number": item_number,
        "amount": float(amount),
        "ts": now_iso()
    }
    publish_bid(producer, bid)
    
    # Add the first log message and trigger the first refresh
    st.session_state.log_messages.append(f"{now_iso()} - {log_messages_template[0]}")
    st.rerun()

# Display live logs based on the current step
if st.session_state.log_step >= 0 and st.session_state.log_step < len(log_messages_template):
    # This block will re-run every 3 seconds
    if len(st.session_state.log_messages) <= st.session_state.log_step:
        st.session_state.log_messages.append(f"{now_iso()} - {log_messages_template[st.session_state.log_step]}")
        
    with log_placeholder.container():
        for log_msg in st.session_state.log_messages:
            st.write(log_msg)
            
    # Increment the step for the next refresh
    st.session_state.log_step += 1

# Database loaders: Postgres
def load_postgres_table():
    try:
        print("[Streamlit] Loading data from Postgres...")
        engine = wait_for_postgres()
        with engine.connect() as conn:
            users_df = pd.read_sql("SELECT * FROM users ORDER BY created_at DESC LIMIT 50", conn)
            bids_df = pd.read_sql("SELECT * FROM bids ORDER BY created_at DESC LIMIT 50", conn)
        return users_df, bids_df
    except Exception as e:
        print(f"[Streamlit] Postgres connection failed: {e}")
        return None, None

# Database loaders: Redis
def load_redis_snapshot():
    try:
        print("[Streamlit] Loading data from Redis...")
        r = wait_for_redis()
        items = {}
        for key in r.scan_iter(match='item:*'):
            item = key.split(':', 1)[1]
            vals = r.zrange(key, 0, -1, withscores=True)
            items[item] = vals[-5:]  # show top 5
        return items
    except Exception as e:
        print(f"[Streamlit] Redis connection failed: {e}")
        return {}
    
# Database loaders: Cassandra
def load_cassandra_audit():
    try:
        print("[Streamlit] Loading data from Cassandra...")
        session = wait_for_cassandra()
        rows = session.execute("SELECT * FROM bid_logs LIMIT 50")
        return pd.DataFrame(rows)
    except Exception as e:
        print(f"[Streamlit] Cassandra connection failed: {e}")
        return None

# Display database snapshots after the log animation is finished
if st.session_state.log_step >= len(log_messages_template):
    # This block now renders the final state of the logs and the tables
    with log_placeholder.container():
        for log_msg in st.session_state.log_messages:
            st.write(log_msg)
            
    st.markdown("<h2 style='margin-bottom: 0px;'>Databases snapshot</h2>", unsafe_allow_html=True)

    # Postgres Users & Bids
    st.markdown("<h4 style='margin-bottom: 0px;'>Postgres - Users & Bids</h4>", unsafe_allow_html=True)
    u_df, b_df = load_postgres_table()
    if u_df is not None:
        st.write("Users")
        st.dataframe(u_df)
        st.write("Bids")
        st.dataframe(b_df)
    else:
        st.info("Postgres not ready or empty")

    # Cassandra Audit Log
    st.markdown("<h4 style='margin-bottom: 0px;'>Cassandra - Audit Log</h4>", unsafe_allow_html=True)
    c_df = load_cassandra_audit()
    if c_df is not None and not c_df.empty:
        st.dataframe(c_df)
    else:
        st.info("Cassandra empty or not ready")
    
    # Redis - Current Highest Bids
    st.markdown("<h4 style='margin-bottom: 0px;'>Redis - Current Highest Bids</h4>", unsafe_allow_html=True)
    r_snapshot = load_redis_snapshot()
    if r_snapshot:
        for item, vals in r_snapshot.items():
            st.write(f"Item {item}: {vals}")
    else:
        st.info("Redis empty or not ready")

    # Kafka Notifications
    st.markdown("<h4 style='margin-bottom: 0px;'>Kafka Notifications</h4>", unsafe_allow_html=True)
    if st.session_state.notif_buffer:
        st.dataframe(list(st.session_state.notif_buffer))
    else:
        st.info("No notifications yet")

# Initial state: hide database tables
else:
    if not st.session_state.log_messages:
      st.markdown("<h2 style='margin-bottom: 0px;'>Databases snapshot</h2>", unsafe_allow_html=True)
      st.info("Database snapshots will appear here after a bid is submitted and the data flow is complete.")