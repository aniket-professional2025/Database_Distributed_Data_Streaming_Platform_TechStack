# Importing Required Packages
from prometheus_client import Counter, Gauge, start_http_server

# Defining Metrics
TX_TOTAL = Counter('upi_transactions_total', 'Total transactions processed', ['status'])
TX_AMOUNT = Counter('upi_transactions_amount_total', 'Sum of transaction amounts', [])
BALANCE_GAUGE = Gauge('upi_user_balance', 'User balance', ['user_id'])

# Starting Metrics Server
def start_metrics(port=8000):
    start_http_server(port)