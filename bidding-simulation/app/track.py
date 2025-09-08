# Importing necessary libraries
import time
from db import wait_for_redis
from prometheus_metrics import HIGHEST_BID
from prometheus_metrics import BIDS_TOTAL
from app_settings import PROM_METRICS_PORT
import threading

# Function to periodically track highest bids from Redis and update Prometheus metrics
def track_loop(poll_interval=5):
    r = wait_for_redis()
    while True:
        try:
            keys = [k for k in r.scan_iter(match='item:*')]
            for key in keys:
                item = key.split(':', 1)[1]
                # high score is last element
                vals = r.zrange(key, 0, -1, withscores=True)
                if vals:
                    member, score = vals[-1]
                    HIGHEST_BID.labels(item_number=item).set(float(score))
        except Exception:
            pass
        time.sleep(poll_interval)

# Function to start the tracker in a separate thread
def start_tracker_in_thread():
    t = threading.Thread(target=track_loop, daemon=True)
    t.start()
    return t

# Main execution
if __name__ == "__main__":
    print("Starting bid-tracker service...")
    start_tracker_in_thread()

    # keep process alive
    while True:
        time.sleep(60)