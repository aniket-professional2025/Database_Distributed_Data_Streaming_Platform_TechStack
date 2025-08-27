# Cold-Chain Kafka + PostgreSQL (Python)

A minimal end-to-end example:

- **producer.py** simulates refrigerated package temperature readings and publishes to Kafka.
- **consumer.py** consumes events, writes them into Postgres, and raises alerts when temperature drifts out of 2–8°C.

## Prerequisites

- Python 3.10+
- Kafka broker (e.g., local Apache Kafka or Redpanda)
- PostgreSQL (local or remote)

## Setup

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux/Mac: source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
