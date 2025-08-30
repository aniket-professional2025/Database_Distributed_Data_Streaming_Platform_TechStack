# Importing required Packages
import os
import uuid
import subprocess
import datetime
import time
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import streamlit as st
import pandas as pd
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
import redis
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from prometheus_client import start_http_server, Gauge, Counter, Histogram, REGISTRY

# Configuration and Limits
SINGLE_TXN_LIMIT = 150000
DAILY_LIMIT = 500000

# Prometheus Metrics (labels: payer_id, payee_id, status)
def clear_existing_metrics():
    collectors_to_unregister = []
    for collector in list(REGISTRY._collector_to_names.keys()):
        if "upi_" in collector.__class__.__name__.lower():  # crude filter
            collectors_to_unregister.append(collector)
    for c in collectors_to_unregister:
        try:
            REGISTRY.unregister(c)
        except KeyError:
            pass

if "metrics_initialized" not in st.session_state:
    clear_existing_metrics()
    # Total Transactions
    MET_TXS_TOTAL = Counter(
        "upi_tx_total",
        "Total number of UPI transactions processed",
        ["payer_id", "payee_id", "status"]
    )
    # Total Amount sum per payer
    MET_TX_AMOUNT_SUM = Counter(
        "upi_tx_amount_sum",
        "Sum of transaction amounts (per payer)",
        ["payer_id"]
    )
    # Count per Payer
    MET_TX_AMOUNT_COUNT = Counter(
        "upi_tx_amount_count",
        "Count of UPI transactions (per payer)",
        ["payer_id"]
    )
    # Histogram for Amounts for distribution/Average estimation
    MET_TX_AMOUNT_HIST = Histogram(
        "upi_tx_amount_hist",
        "Histogram of transaction amounts (per payer)",
        buckets = (1, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, 200000),
    )
    # Gauge for Last Transaction Status/time
    MET_LAST_TX_STATUS = Gauge(
        "upi_last_tx_status",
        "Gauge representing last transaction status (1=success,0=fail)",
        ["tx_id", "payer_id", "payee_id"]
    )
    # Gauge for balances snapshot (best-effort)
    MET_USER_BALANCE = Gauge(
        "upi_user_balance",
        "Last known user balance (best-effort snapshot from Postgres/Redis)",
        ["user_id"],
    )
    st.session_state.metrics_initialized = True

# Setup & environment
load_dotenv(dotenv_path=Path(".env"))
PROJECT_DIR = Path(__file__).parent.resolve()

# docker-compose detection (non-fatal)
def docker_compose_cmd() -> list[str]:
    for cmd in (["docker", "compose"], ["docker-compose"]):
        try:
            subprocess.run(cmd + ["version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
            return cmd
        except Exception:
            continue
    return ["docker", "compose"]

DC = docker_compose_cmd()

# Streamlit session defaults
if "stack_running" not in st.session_state:
    st.session_state.stack_running = False
if "last_error" not in st.session_state:
    st.session_state.last_error = None
if "last_inserted_tx" not in st.session_state:
    st.session_state.last_inserted_tx = None
if "logs" not in st.session_state:
    st.session_state.logs = []

def set_error(msg: str):
    st.session_state.last_error = str(msg)

def clear_error():
    st.session_state.last_error = None

def add_log(message: str):
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    st.session_state.logs.append(f"[{ts}] {message}")

def clear_logs():
    st.session_state.logs = []

# Postgres Helpers
def pg_conn():
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    db   = os.getenv("POSTGRES_DB", "upi_db")
    user = os.getenv("POSTGRES_USER", "upi_user")
    pw   = os.getenv("POSTGRES_PASSWORD", "upi_pass")
    if host in ("postgres", "db"):
        host = "127.0.0.1"
    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pw)

def ensure_postgres_schema():
    try:
        with pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id TEXT PRIMARY KEY,
                        name TEXT,
                        balance BIGINT DEFAULT 0
                    );
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS transactions (
                        tx_id TEXT PRIMARY KEY,
                        payer_id TEXT,
                        payee_id TEXT,
                        amount BIGINT,
                        currency TEXT,
                        status TEXT,
                        reason TEXT,
                        created_at TIMESTAMP DEFAULT NOW()
                    );
                """)
            conn.commit()
        clear_error()
    except Exception as e:
        set_error(f"Failed to ensure Postgres schema: {e}")

def fetch_postgres_safe(tx_id: Optional[str] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    try:
        with pg_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM users ORDER BY id;")
                users = cur.fetchall()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if tx_id:
                    cur.execute("SELECT * FROM transactions WHERE tx_id = %s;", (tx_id,))
                else:
                    cur.execute("SELECT * FROM transactions ORDER BY created_at DESC LIMIT 200;")
                txns = cur.fetchall()
        clear_error()
        return pd.DataFrame(users), pd.DataFrame(txns)
    except Exception as e:
        set_error(f"Postgres error: {e}")
        return pd.DataFrame(), pd.DataFrame()

# Cassandra helpers
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
    sess.execute("""
        CREATE TABLE IF NOT EXISTS audit_logs (
            tx_id text PRIMARY KEY,
            payer_id text,
            payee_id text,
            amount bigint,
            status text,
            created_at timestamp
        );
    """)
    return cluster, sess

def fetch_cassandra_safe(limit: int = 200) -> pd.DataFrame:
    try:
        cluster, sess = cass_session()
        rows = list(sess.execute(f"SELECT * FROM audit_logs LIMIT {limit};"))
        try:
            cluster.shutdown()
        except Exception:
            pass
        clear_error()
        return pd.DataFrame(rows)
    except Exception as e:
        set_error(f"Cassandra error: {e}")
        return pd.DataFrame()

# Redis Helpers
def redis_client():
    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", "6379"))
    if host == "redis":
        host = "127.0.0.1"
    return redis.Redis(host=host, port=port, decode_responses=True)

def fetch_redis_safe() -> pd.DataFrame:
    try:
        r = redis_client()
        out = []
        for key in r.scan_iter(match="balance:*", count=500):
            out.append({"key": key, "value": r.get(key)})
        for key in r.scan_iter(match="daily:*", count=500):
            out.append({"key": key, "value": r.get(key)})
        clear_error()
        return pd.DataFrame(out)
    except Exception as e:
        set_error(f"Redis error: {e}")
        return pd.DataFrame()
      
# Prometheus server runner
def start_metrics_server(port: int = 8000):
    """Start Prometheus metrics HTTP server in separate thread (non-blocking)."""
    try:
        # start_http_server starts an HTTP server that serves /metrics
        start_http_server(port)
        add_log(f"Prometheus metrics exposed on :{port}/metrics")
    except Exception as e:
        add_log(f"Failed to start Prometheus metrics server: {e}")

# start metrics server in a daemon thread so Streamlit can continue
metrics_thread = threading.Thread(target=start_metrics_server, args=(8000,), daemon=True)
metrics_thread.start()

# Transaction Pipeline with Logs (simulated Kafka) + metrics updates
def insert_transaction_pipeline(tx_id: str,payer_id: str,payee_id: str,amount: int,currency: str,reason: str,created_at: datetime.datetime,) -> Dict[str, Any]:
    results: Dict[str, Any] = {"postgres": None, "cassandra": None, "redis": None}
    amt = int(amount)

    # add initial logs
    add_log("Manual entry submitted.")
    time.sleep(0.3)
    add_log("Sent to Kafka topic 'transactions'.")
    time.sleep(0.3)
    add_log("Kafka consumer received transaction event.")
    time.sleep(0.3)

    # --- Business rules: compute final_status ---
    final_status = "SUCCESS"
    fail_reason = None

    # 1) single txn limit
    if amt > SINGLE_TXN_LIMIT:
        final_status = "FAILED"
        fail_reason = f"single_txn_limit_exceeded: {amt} > {SINGLE_TXN_LIMIT}"
        add_log(f"Rule check: {fail_reason}")

    # 2) daily limit per payer (only if still success so far)
    if final_status == "SUCCESS":
        try:
            r = redis_client()
            today = datetime.date.today().isoformat()
            daily_key = f"daily:{payer_id}:{today}"
            current_daily = r.get(daily_key)
            current_daily_val = int(current_daily) if current_daily else 0
        except Exception as e:
            # if Redis fails, default to 0 but log the problem
            current_daily_val = 0
            add_log(f"Redis read failed for daily totals: {e}")

        if (current_daily_val + amt) > DAILY_LIMIT:
            final_status = "FAILED"
            fail_reason = f"daily_limit_exceeded: current {current_daily_val} + {amt} > {DAILY_LIMIT}"
            add_log(f"Rule check: {fail_reason}")
        else:
            add_log(f"Rule check: daily total OK (current={current_daily_val}, +{amt} => {current_daily_val+amt})")

    time.sleep(0.3)

    # --- Postgres: ensure users exist and insert transaction + update balances if SUCCESS ---
    try:
        with pg_conn() as conn:
            with conn.cursor() as cur:
                # Ensure both users exist (if missing create with balance 0)
                cur.execute(
                    """
                    INSERT INTO users (id, name, balance)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (id) DO NOTHING;
                    """,
                    (payer_id, payer_id, 0),
                )
                cur.execute(
                    """
                    INSERT INTO users (id, name, balance)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (id) DO NOTHING;
                    """,
                    (payee_id, payee_id, 0),
                )

                # Insert transaction row with final_status
                cur.execute(
                    """
                    INSERT INTO transactions
                        (tx_id, payer_id, payee_id, amount, currency, status, reason, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tx_id) DO NOTHING
                    RETURNING tx_id;
                    """,
                    (tx_id, payer_id, payee_id, amt, currency, final_status, fail_reason or reason, created_at),
                )
                returned = cur.fetchone()
                txn_id = returned[0] if returned else tx_id

                # Update balances only if SUCCESS
                if final_status == "SUCCESS":
                    # Decrement payer, increment payee
                    cur.execute("UPDATE users SET balance = balance - %s WHERE id = %s;", (amt, payer_id))
                    cur.execute("UPDATE users SET balance = balance + %s WHERE id = %s;", (amt, payee_id))

            conn.commit()

        results["postgres"] = {"ok": True, "tx_id": txn_id, "final_status": final_status}
        add_log(f"Transaction written to Postgres (status={final_status}).")
    except Exception as e:
        results["postgres"] = {"ok": False, "error": str(e)}
        add_log(f"Postgres insert failed: {e}")
        # if Postgres fails, bail out early (no audit/redis updates)
        # update metrics for failure
        MET_TXS_TOTAL.labels(payer_id=payer_id, payee_id=payee_id, status="FAILED").inc()
        MET_LAST_TX_STATUS.labels(tx_id=tx_id, payer_id=payer_id, payee_id=payee_id).set(0)
        return results

    time.sleep(0.3)

    # --- Cassandra: write audit (always write audit with final_status) ---
    try:
        cluster, sess = cass_session()
        sess.execute(
            """
            INSERT INTO audit_logs (tx_id, payer_id, payee_id, amount, status, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (tx_id, payer_id, payee_id, amt, final_status, created_at),
        )
        try:
            cluster.shutdown()
        except Exception:
            pass
        results["cassandra"] = {"ok": True, "tx_id": tx_id}
        add_log(f"Audit log written to Cassandra (status={final_status}).")
    except Exception as e:
        results["cassandra"] = {"ok": False, "error": str(e)}
        add_log(f"Cassandra insert failed: {e}")

    time.sleep(0.3)

    # --- Redis: update balances & daily totals only on SUCCESS ---
    try:
        if final_status == "SUCCESS":
            r = redis_client()
            today = datetime.date.today().isoformat()
            payer_balance_key = f"balance:{payer_id}"
            payee_balance_key = f"balance:{payee_id}"
            payer_daily_key = f"daily:{payer_id}:{today}"

            pipe = r.pipeline()
            # Use integer INCRBY (we used ints in DB)
            pipe.incrby(payer_balance_key, -amt)
            pipe.incrby(payee_balance_key, amt)
            pipe.incrby(payer_daily_key, amt)
            pipe.execute()

            results["redis"] = {"ok": True, "payer_balance_key": payer_balance_key, "payer_daily_key": payer_daily_key}
            add_log("Redis balances & daily totals updated.")
        else:
            # For failed txns we do not update balances or daily totals
            results["redis"] = {"ok": True, "note": "no Redis updates for failed txn"}
            add_log("No Redis updates because transaction FAILED.")
    except Exception as e:
        results["redis"] = {"ok": False, "error": str(e)}
        add_log(f"Redis update failed: {e}")

    # --- Update Prometheus metrics ---
    try:
        # tx counts by (payer,payee,status)
        MET_TXS_TOTAL.labels(payer_id=payer_id, payee_id=payee_id, status=final_status).inc()

        if final_status == "SUCCESS":
            # per-payer sums / counts for average calculation
            MET_TX_AMOUNT_SUM.labels(payer_id=payer_id).inc(amt)
            MET_TX_AMOUNT_COUNT.labels(payer_id=payer_id).inc()
            MET_TX_AMOUNT_HIST.observe(amt)

            # set last tx status gauge to 1
            MET_LAST_TX_STATUS.labels(tx_id=tx_id, payer_id=payer_id, payee_id=payee_id).set(1)
        else:
            MET_LAST_TX_STATUS.labels(tx_id=tx_id, payer_id=payer_id, payee_id=payee_id).set(0)

        # Try to snapshot balances from Postgres for metrics (best-effort)
        try:
            users_df, _ = fetch_postgres_safe()
            if not users_df.empty:
                for _, row in users_df.iterrows():
                    try:
                        MET_USER_BALANCE.labels(user_id=row["id"]).set(int(row.get("balance", 0)))
                    except Exception:
                        pass
        except Exception:
            pass

        add_log("Prometheus metrics updated.")
    except Exception as e:
        add_log(f"Failed to update Prometheus metrics: {e}")

    # Final log
    if final_status == "SUCCESS":
        add_log("Transaction pipeline completed successfully.")
    else:
        add_log(f"Transaction pipeline completed (FAILED): {fail_reason}")

    # include status & reason in the results for UI
    results["final_status"] = final_status
    results["fail_reason"] = fail_reason
    return results

# Streamlit UI
st.set_page_config(page_title="UPI Simulator — Orchestration & Monitor (Kafka-style pipeline)", layout="wide")
st.title("UPI Simulator — Orchestration & Monitor (Kafka-style pipeline)")

# Ensure Postgres schema at startup
ensure_postgres_schema()

if st.session_state.get("last_error"):
    st.error(st.session_state["last_error"])

# --- Top row: Manual entry + Live logs side by side ---
col1, col2 = st.columns([0.7, 1.3], gap="large")

with col1:
    st.subheader("Manual Transaction Entry")

    with st.form("manual_txn_form", clear_on_submit=True):
        payer_id = st.text_input("Payer ID", value="user_1")
        payee_id = st.text_input("Payee ID", value="user_2")
        amount = st.number_input("Amount", min_value=0.0, step=1.0, format="%.2f", value=100.00)
        currency = st.selectbox("Currency", ["INR", "USD", "EUR"])
        reason = st.text_input("Reason", value="Manual Entry")
        submitted = st.form_submit_button("Submit Transaction")

    if submitted:
        clear_logs()
        tx_id = str(uuid.uuid4())
        created_at = datetime.datetime.now()
        with st.spinner("Simulating Kafka pipeline..."):
            res = insert_transaction_pipeline(tx_id, payer_id, payee_id, int(amount), currency, reason, created_at)
            st.session_state.last_inserted_tx = tx_id
        if res.get("final_status") == "SUCCESS":
            st.success("Pipeline simulation finished (SUCCESS).")
        else:
            st.warning(f"Pipeline finished (FAILED): {res.get('fail_reason')}")

with col2:
    st.subheader("Live Data Flow Logs")
    log_box = st.empty()

    if st.session_state.logs:
        log_box.text("\n".join(st.session_state.logs))
    else:
        log_box.info("No logs yet. Submit a transaction to see flow.")

# --- Bottom row: Database contents ---
st.markdown("---")
st.subheader("Database Views")

tab_users, tab_txns, tab_cass, tab_redis = st.tabs(
    ["Postgres: Users", "Postgres: Transactions", "Cassandra: audit_logs", "Redis: cache"]
)

with tab_users:
    users_df, _ = fetch_postgres_safe()
    st.dataframe(users_df if not users_df.empty else pd.DataFrame(), use_container_width=True)

with tab_txns:
    _, txns_df = fetch_postgres_safe()
    st.dataframe(txns_df if not txns_df.empty else pd.DataFrame(), use_container_width=True)

with tab_cass:
    cdf = fetch_cassandra_safe(limit=200)
    st.dataframe(cdf if not cdf.empty else pd.DataFrame(), use_container_width=True)

with tab_redis:
    rdf = fetch_redis_safe()
    st.dataframe(rdf if not rdf.empty else pd.DataFrame(), use_container_width=True)

st.markdown("---")
st.caption("Simulated Kafka → Postgres → Cassandra → Redis pipeline with step-by-step live logs. Prometheus metrics available at :8000/metrics for Prometheus -> Grafana dashboards.")