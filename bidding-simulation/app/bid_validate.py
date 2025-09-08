# Importing Required Libraries
import json
import time
from kafka import KafkaConsumer
from app_settings import KAFKA_BOOTSTRAP, REDIS_HOST
from db import wait_for_postgres, wait_for_redis, wait_for_cassandra
from utils import now_iso
from prometheus_metrics import BIDS_TOTAL, BIDS_SUCCESS, BIDS_FAILED, HIGHEST_BID, BID_PROCESSING_TIME
from notification_producer import send_notification
from sqlalchemy import text
import decimal
from notification_producer import send_notification
import datetime

# Calculating the Current Highest Bid from Redis
def current_highest_from_redis(r, item):
    vals = r.zrange(f"item:{item}", 0, -1, withscores=True)
    if not vals:
        return None
    member, score = vals[-1]
    return float(score)

# Main function to run the Kafka Consumer
def run_consumer():
    consumer = KafkaConsumer(
        'bids',
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='bid-validators'
    )
    print("[Validator] Waiting for Postgres...")
    engine = wait_for_postgres()
    print("[Validator] Waiting for Redis...")
    r = wait_for_redis()
    print("[Validator] Waiting for Cassandra...")
    session = wait_for_cassandra() # <-- CONNECT TO CASSANDRA

    # # Create the keyspace and table if they don't exist
    # session.execute("""
    #     CREATE KEYSPACE IF NOT EXISTS bidding WITH replication = {
    #         'class': 'SimpleStrategy',
    #         'replication_factor': '1'
    #     }
    # """)
    # session.set_keyspace('bidding')

    # session.execute("""
    #     CREATE TABLE IF NOT EXISTS bid_logs (
    #         bid_id text,
    #         user_id text,
    #         item_number text,
    #         amount float,
    #         status text,
    #         reason text,
    #         created_at timestamp,
    #         PRIMARY KEY (bid_id)
    #     )
    # """)

    prepared_stmt = session.prepare("INSERT INTO bid_logs (bid_id, user_id, item_number, amount, status, reason, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)") # <-- PREPARE STATEMENT
    
    print("[Validator] All database connections established. Starting Kafka consumer loop.")
    for msg in consumer:
        bid = msg.value
        BIDS_TOTAL.inc()
        start = time.time()
        bid_id = bid.get('id')
        user_id = bid.get('user_id')
        item = bid.get('item_number')
        try:
            amount = float(bid.get('amount'))
        except Exception:
            amount = None

        status = 'FAILED'
        reason = None

        # Validation logic:
        if not bid_id or not user_id or not item or amount is None:
            reason = "Missing fields or invalid amount"
        elif amount <= 0:
            reason = "Amount must be positive"
        else:
            highest = current_highest_from_redis(r, item)
            if highest is not None and amount <= highest:
                reason = f"Amount {amount} <= current highest {highest}"
            else:
                status = 'SUCCESS'

        # Write audit to Postgres
        try:
            with engine.begin() as conn:
                conn.execute(
                    text("INSERT INTO bids (bid_id, user_id, item_number, amount, created_at) VALUES (:id,:user_id,:item_number,:amount, now())"),
                    {"id": bid_id, "user_id": user_id, "item_number": item, "amount": decimal.Decimal(amount if amount is not None else 0)}
                )
                conn.execute(text("""
                    INSERT INTO users(user_id, last_item_number, last_amount, created_at)
                    VALUES (:user_id, :item_number, :amount, now())
                    ON CONFLICT (user_id) DO UPDATE 
                        SET last_item_number = EXCLUDED.last_item_number, 
                            last_amount = EXCLUDED.last_amount
                """), {"user_id": user_id, "item_number": item, "amount": decimal.Decimal(amount if amount is not None else 0)})
        except Exception as e:
            reason = f"DB error: {e}"
            status = 'FAILED'

        # Write audit to Cassandra # <-- ADD THIS BLOCK
        try:
            session.execute(prepared_stmt, (bid_id, user_id, item, amount, status, reason, datetime.datetime.now()))
        except Exception as e:
            print(f"Cassandra write failed: {e}")

        if status == 'SUCCESS':
            try:
                r.zadd(f"item:{item}", {f"{bid_id}:{user_id}": amount})
            except Exception:
                pass
            BIDS_SUCCESS.inc()
            HIGHEST_BID.labels(item_number=item).set(amount)
            send_notification({"type": "BID_SUCCESS", "bid_id": bid_id, "user_id": user_id, "item_number": item, "amount": amount, "status": "SUCCESS" ,  "ts": now_iso()})
        else:
            BIDS_FAILED.inc()
            # Send FAILED notification
            send_notification({"type": "BID_FAILED", "bid_id": bid_id, "user_id": user_id, "item_number": item, "amount": amount, "status": "FAILED", "reason": reason, "ts": now_iso()})

        BID_PROCESSING_TIME.observe(time.time() - start)

# Make Inference when the script is run directly
if __name__ == "__main__":
    while True:
        try:
            print("[Validator] Starting consumer...")
            run_consumer()
        except Exception as e:
            print(f"[Validator] Error: {e}, retrying in 5s...")
            time.sleep(5)