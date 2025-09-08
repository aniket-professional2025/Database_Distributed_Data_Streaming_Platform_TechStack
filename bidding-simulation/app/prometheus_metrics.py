# Importing Required Libraries
from prometheus_client import Counter, Gauge, Summary, start_http_server
from app_settings import PROM_METRICS_PORT

# Counters
BIDS_TOTAL = Counter('bids_total', 'Total number of bids processed')
BIDS_SUCCESS = Counter('bids_success_total', 'Total successful bids')
BIDS_FAILED = Counter('bids_failed_total', 'Total failed bids')
# Gauges
HIGHEST_BID = Gauge('highest_bid_amount', 'Current highest bid amount', ['item_number'])
# Summary
BID_PROCESSING_TIME = Summary('bid_processing_seconds', 'Time spent processing bids')

# Function to Start Prometheus Metrics Server
def start_metrics_server():
    # start the HTTP server to expose /metrics
    start_http_server(PROM_METRICS_PORT)