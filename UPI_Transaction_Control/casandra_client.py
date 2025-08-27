# casandra_client.py
from cassandra.cluster import Cluster, NoHostAvailable, DriverException
import time

class CassandraClient:
    def __init__(self, hosts):
        self.session = None  # Initialize session to None
        
        max_retries = 50
        retry_delay = 5  # seconds
        
        for i in range(max_retries):
            try:
                print(f"Attempting to connect to Cassandra... (Attempt {i+1}/{max_retries})")
                
                # Recreate the cluster object on each retry
                self.cluster = Cluster(hosts)
                self.session = self.cluster.connect()
                
                print("Successfully connected to Cassandra.")
                break
            except (NoHostAvailable, DriverException) as e:
                print(f"Error connecting to Cassandra: {e}. Retrying in {retry_delay}s...")
                
                # Close the cluster to prevent subsequent 'already shut down' errors
                if hasattr(self, 'cluster'):
                    self.cluster.shutdown()

                if i == max_retries - 1:
                    print(f"Max retries reached. Failed to connect to Cassandra. {e}")
                    raise # Re-raise the exception if all retries fail
                
                time.sleep(retry_delay)
        
        self._ensure_keyspace()

    def _ensure_keyspace(self):
        self.session.execute("""
        CREATE KEYSPACE IF NOT EXISTS upi WITH replication = {'class':'SimpleStrategy','replication_factor':1};
        """)
        self.session.set_keyspace('upi')
        self.session.execute("""
        CREATE TABLE IF NOT EXISTS audit_logs (
            tx_id text PRIMARY KEY,
            payer_id text,
            payee_id text,
            amount bigint,
            status text,
            created_at timestamp
        )
        """)

    def insert_audit(self, tx_id, payer_id, payee_id, amount, status, created_at):
        self.session.execute("INSERT INTO audit_logs (tx_id,payer_id,payee_id,amount,status,created_at) VALUES (%s,%s,%s,%s,%s,%s)",
                              (tx_id,payer_id,payee_id,amount,status,created_at))