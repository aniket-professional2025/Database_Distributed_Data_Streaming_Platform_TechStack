# UPI Transaction Simulator
Run a local simulated UPI transaction system that uses Kafka for events, Postgres for storage, Cassandra for audit logs, Redis for caching, and Prometheus for metrics. Docker is used to create the container to host all these tech stack services in a single cell. 

## Project File Structure and Details
This project simulates **UPI transactions** between users using multiple technologies (PostgreSQL, Cassandra, Redis, Kafka, Prometheus, Docker). Below is a breakdown of each file and its role:

### 1. `.env`
- Stores environment variables (database URLs, Kafka configs, Redis host, etc.).
- Keeps sensitive credentials/configs separate from the code.

---

### 2. `docker-compose.yml`
- Orchestrates all services with one command (`docker-compose up`).
- Runs:
  - PostgreSQL (transaction persistence)
  - Cassandra (historical storage)
  - Redis (cache for balances)
  - Kafka + Zookeeper (message queue for transactions)
  - Prometheus (metrics monitoring)
  - The application container itself.

---

### 3. `Dockerfile`
- Defines how the application container is built.
- Uses Python as base, installs dependencies from `requirements.txt`, copies the app code, and starts the application.

---

### 4. `prometheus.yml`
- Prometheus configuration file.
- Defines scraping rules to pull metrics from the app (exposed via `/metrics` endpoint in `metrics.py`).

---

### 5. `requirements.txt`
- Lists all Python dependencies (e.g., `psycopg2`, `cassandra-driver`, `redis`, `kafka-python`, `prometheus_client`).
- Ensures consistent environment setup inside Docker.

---

### 6. `app/cache_redis.py`
- Handles **Redis caching**.
- Provides functions like:
  - `set_balance(user_id, balance)` → cache user balance.
  - `get_balance(user_id)` → retrieve cached balance quickly.
- Helps reduce load on PostgreSQL.

---

### 7. `app/casandra_client.py`
- Manages **Cassandra connection**.
- Stores historical transaction logs for fast analytical queries.
- Provides:
  - `insert_transaction(...)` → store transaction details.
  - `get_transactions(user_id)` → fetch user’s transaction history.

---

### 8. `app/config.py`
- Centralized configuration file.
- Reads values from `.env` and provides them to other modules.
- Keeps DB credentials, Kafka topics, Redis host, etc. in one place.

---

### 9. `app/consumer.py`
- Kafka **consumer** service.
- Listens to the Kafka topic for transaction messages.
- Processes transactions by:
  1. Validating limits (transaction > ₹1,50,000 fails).
  2. Updating PostgreSQL & Cassandra.
  3. Updating Redis cache.
  4. Exposing Prometheus metrics.

---

### 10. `app/db_postgres.py`
- PostgreSQL client module.
- Provides:
  - `create_tables()` → initializes schema for users & transactions.
  - `insert_transaction(...)` → record transactions.
  - `update_balance(...)` → update user balance safely.
- Ensures relational integrity of account balances.

---

### 11. `app/kafka_client.py`
- Kafka producer & consumer setup utilities.
- Provides:
  - `get_producer()` → returns Kafka producer instance.
  - `get_consumer()` → returns Kafka consumer instance.
- Used by both `producer.py` and `consumer.py`.

---

### 12. `app/metrics.py`
- Defines **Prometheus metrics**.
- Exposes metrics like:
  - `transactions_total` (Counter) → number of processed transactions.
  - `transaction_failures_total` (Counter) → number of failed transactions.
  - `transaction_amount` (Histogram) → distribution of transaction amounts.
- Integrated with `consumer.py` for monitoring.

---

### 13. `app/models.py`
- Defines **data models** (likely as Python classes or dicts).
- Example:
  - `Transaction(user_from, user_to, amount, status, timestamp)`
- Acts as a data schema between producer → Kafka → consumer.

---

### 14. `app/producer.py`
- Kafka **producer** service.
- Simulates random transactions between users.
- Publishes transaction messages to Kafka for processing.
- Uses `models.py` for transaction structure.

---

### 15. `app/utils.py`
- Helper utility functions.
- Examples:
  - `generate_transaction_id()`
  - `validate_amount(amount)`
  - `get_current_timestamp()`
- Keeps common logic separate from core modules.

---

## How to run
1. Create `.env` (already provided).
2. Build and start services:
   ```bash
   docker-compose up --build