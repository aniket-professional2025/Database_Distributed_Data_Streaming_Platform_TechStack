#### DESCRIPTION:
# This streamlit application simulates a UPI payment system, allowing users to create and manage transactions.
# Importing Required Packages
import os
import subprocess
from pathlib import Path
import streamlit as st
import pandas as pd
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
import redis
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

# -------------------------
# Setup & environment
# -------------------------
load_dotenv(dotenv_path=Path(".env"))
PROJECT_DIR = Path(__file__).parent.parent.resolve()

# -------------------------
# docker-compose detection
# -------------------------
def docker_compose_cmd() -> list[str]:
    for cmd in (["docker", "compose"], ["docker-compose"]):
        try:
            subprocess.run(
                cmd + ["version"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=True
            )
            return cmd
        except Exception:
            continue
    st.error("Docker Compose is not installed or not in PATH.")
    st.stop()

DC = docker_compose_cmd()

# -------------------------
# session state init
# -------------------------
if "stack_running" not in st.session_state:
    st.session_state.stack_running = False

# -------------------------
# Stack control helpers
# -------------------------
def start_stack():
    res = subprocess.run(
        DC + ["up", "-d", "--build"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace"
    )
    if res.returncode != 0:
        raise subprocess.CalledProcessError(
            res.returncode, res.args, output=res.stdout, stderr=res.stderr
        )
    st.session_state.stack_running = True

def stop_stack():
    subprocess.run(
        DC + ["down"],
        cwd=PROJECT_DIR,
        check=False,
        encoding="utf-8",
        errors="replace"
    )
    st.session_state.stack_running = False

# -------------------------
# DB helpers
# -------------------------
def pg_conn():
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    db   = os.getenv("POSTGRES_DB", "upi_db")
    user = os.getenv("POSTGRES_USER", "upi_user")
    pw   = os.getenv("POSTGRES_PASSWORD", "upi_pass")
    if host in ("postgres", "db"):
        host = "127.0.0.1"
    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pw)

def fetch_postgres_safe():
    try:
        with pg_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM users ORDER BY id;")
                users = cur.fetchall()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM transactions ORDER BY created_at DESC LIMIT 200;")
                txns = cur.fetchall()
        return pd.DataFrame(users), pd.DataFrame(txns)
    except Exception:
        return pd.DataFrame(), pd.DataFrame()

def redis_client():
    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", "6379"))
    if host == "redis":
        host = "127.0.0.1"
    return redis.Redis(host=host, port=port, decode_responses=True)

def fetch_redis_safe():
    try:
        r = redis_client()
        out = []
        for key in r.scan_iter(match="balance:*", count=500):
            out.append({"key": key, "value": r.get(key)})
        for key in r.scan_iter(match="daily:*", count=500):
            out.append({"key": key, "value": r.get(key)})
        return pd.DataFrame(out)
    except Exception:
        return pd.DataFrame()

def cass_session():
    host = os.getenv("CASSANDRA_HOST", "localhost")
    if host == "cassandra":
        host = "127.0.0.1"
    cluster = Cluster([host])
    sess = cluster.connect()
    sess.execute("""
        CREATE KEYSPACE IF NOT EXISTS upi
        WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1};
    """)
    sess.set_keyspace("upi")
    sess.row_factory = dict_factory
    return cluster, sess

def fetch_cassandra_safe(limit=50):
    try:
        cluster, sess = cass_session()
        try:
            rows = list(sess.execute(f"SELECT * FROM audit_logs LIMIT {limit};"))
            return pd.DataFrame(rows)
        finally:
            cluster.shutdown()
    except Exception:
        return pd.DataFrame()

# -------------------------
# Streamlit UI
# -------------------------
st.set_page_config(page_title="UPI Simulator Control Panel", layout="wide")
st.title("UPI Simulator-Orchestration & Monitor")

# Layout
left, right = st.columns([0.6, 1.4], gap="small")

with left:
    st.subheader("Controls")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Start the Tech Stack", type="primary"):
            try:
                start_stack()
                st.success("Stack started.")
            except subprocess.CalledProcessError:
                st.error("Failed to start stack. Check Docker logs.")
    with c2:
        if st.button("Stop the Tech Stack"):
            stop_stack()
            st.warning("Stack stopped.")

with right:
    st.subheader("Data Views")
    tab_users, tab_txns, tab_cass, tab_redis = st.tabs(
        ["Postgres: Users", "Postgres: Transactions", "Cassandra: audit_logs", "Redis: cache"]
    )

    with tab_users:
        if st.button("Refresh Users", key="refresh_users"):
            pass
        if st.session_state.stack_running:
            users_df, _ = fetch_postgres_safe()
            if not users_df.empty:
                st.dataframe(users_df, use_container_width=True)

    with tab_txns:
        if st.button("Refresh Transactions", key="refresh_txns"):
            pass
        if st.session_state.stack_running:
            _, tx_df = fetch_postgres_safe()
            if not tx_df.empty:
                st.dataframe(tx_df, use_container_width=True)

    with tab_cass:
        if st.button("Refresh Cassandra", key="refresh_cass"):
            pass
        if st.session_state.stack_running:
            cdf = fetch_cassandra_safe(limit=100)
            if not cdf.empty and len(cdf) >= 10:
                st.dataframe(cdf, use_container_width=True)

    with tab_redis:
        if st.button("Refresh Redis", key="refresh_redis"):
            pass
        if st.session_state.stack_running:
            rdf = fetch_redis_safe()
            if not rdf.empty:
                st.dataframe(rdf, use_container_width=True)

st.divider()