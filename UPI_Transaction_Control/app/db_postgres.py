# Importing Required Packages
import psycopg2
from psycopg2.extras import RealDictCursor
from config import POSTGRES
import time

# Creating Users Table
CREATE_USERS = '''
CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    name TEXT,
    balance BIGINT DEFAULT 0
);
'''

# Creating Transactions Table
CREATE_TXNS = '''
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
'''

# PostgresDB Class
class PostgresDB:
    def __init__(self):
        cfg = POSTGRES
        max_retries = 10
        retry_delay = 5
        for i in range(max_retries):
            try:
                self.conn = psycopg2.connect(host='postgres', port=5432, dbname=cfg['db'], user=cfg['user'], password=cfg['password'])
                print("PostgresDB connection established")
                break
            except psycopg2.OperationalError as e:
                if i < max_retries - 1:
                    print(f"Error connecting to PostgreSQL: {e}. Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    print(f"Max retries reached. Failed to connect to PostgreSQL. {e}")
                    raise # Re-raise the exception if all retries fail
        else:
                    print(f"Max retries reached. Failed to connect to PostgreSQL. {e}")
                    raise # Re-raise the exception if all retries fail
        
        self._ensure_schema()

    def _ensure_schema(self):
        with self.conn.cursor() as cur:
            cur.execute(CREATE_USERS)
            cur.execute(CREATE_TXNS)
            self.conn.commit()

        # Pre-seed users if not already there
        with self.conn.cursor() as cur:
            for i in range(1, 6):  # create 5 users
                cur.execute(
                "INSERT INTO users (id, name, balance) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING",
                (f"user_{i}", f"user_{i}", 1000000)  # start with 1M balance each
            )
            self.conn.commit()
            
    def create_user(self, user_id, name, balance=0):
        with self.conn.cursor() as cur:
            cur.execute("INSERT INTO users (id, name, balance) VALUES (%s,%s,%s) ON CONFLICT (id) DO NOTHING", (user_id, name, balance))
            self.conn.commit()

    def get_balance(self, user_id):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT balance FROM users WHERE id=%s", (user_id,))
            row = cur.fetchone()
            return row['balance'] if row else None

    def update_balance(self, user_id, new_balance):
        with self.conn.cursor() as cur:
            cur.execute("UPDATE users SET balance=%s WHERE id=%s", (new_balance, user_id))
            self.conn.commit()

    def insert_transaction(self, tx):
        with self.conn.cursor() as cur:
            cur.execute("INSERT INTO transactions (tx_id,payer_id,payee_id,amount,currency,status,reason) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                        (tx.tx_id, tx.payer_id, tx.payee_id, tx.amount, tx.currency, tx.status, tx.reason))

            self.conn.commit()
