# Importing Required Packages
from kafka_client import KafkaClient
from config import KAFKA_BOOTSTRAP, KAFKA_TOPIC, MAX_TXN_AMOUNT, DAILY_TXN_LIMIT
from db_postgres import PostgresDB
from cache_redis import CacheRedis
from casandra_client import CassandraClient
from metrics import TX_TOTAL, TX_AMOUNT, BALANCE_GAUGE, start_metrics
from models import Transaction
from utils import now
import time

# Message Processing
def process_message(msg, db, cache, cass):
    tx = Transaction(tx_id=msg['tx_id'], payer_id=msg['payer_id'], payee_id=msg['payee_id'], amount=int(msg['amount']), created_at=now())

    # 1) Single txn limit
    if tx.amount > MAX_TXN_AMOUNT:
        tx.status = 'FAILED'
        tx.reason = f'Amount exceeds single transaction limit of {MAX_TXN_AMOUNT}'
        TX_TOTAL.labels(status='FAILED').inc()
        db.insert_transaction(tx)
        cass.insert_audit(tx.tx_id, tx.payer_id, tx.payee_id, tx.amount, tx.status, tx.created_at)
        return tx

    # 2) Daily total check
    daily_total = cache.get_daily_total(tx.payer_id)
    if (daily_total + tx.amount) > DAILY_TXN_LIMIT:
        tx.status = 'FAILED'
        tx.reason = f'Daily limit exceeded: current {daily_total} + {tx.amount} > {DAILY_TXN_LIMIT}'
        TX_TOTAL.labels(status='FAILED').inc()
        db.insert_transaction(tx)
        cass.insert_audit(tx.tx_id, tx.payer_id, tx.payee_id, tx.amount, tx.status, tx.created_at)
        return tx

    # 3) Check payer balance
    bal = cache.get_balance(tx.payer_id)
    if bal is None:
        bal = db.get_balance(tx.payer_id)
        if bal is None:
            db.create_user(tx.payer_id, tx.payer_id, balance=1000000)
            bal = 1000000
        cache.set_balance(tx.payer_id, bal)

    if bal < tx.amount:
        tx.status = 'FAILED'
        tx.reason = 'Insufficient balance'
        TX_TOTAL.labels(status='FAILED').inc()
        db.insert_transaction(tx)
        cass.insert_audit(tx.tx_id, tx.payer_id, tx.payee_id, tx.amount, tx.status, tx.created_at)
        return tx

    # 4) All checks passed – perform transfer
    new_payer_bal = bal - tx.amount
    cache.set_balance(tx.payer_id, new_payer_bal)

    payee_bal = cache.get_balance(tx.payee_id)
    if payee_bal is None:
        payee_bal = db.get_balance(tx.payee_id) or 0
    new_payee_bal = payee_bal + tx.amount
    cache.set_balance(tx.payee_id, new_payee_bal)

    db.update_balance(tx.payer_id, new_payer_bal)
    db.update_balance(tx.payee_id, new_payee_bal)
    tx.status = 'SUCCESS'
    TX_TOTAL.labels(status='SUCCESS').inc()
    TX_AMOUNT.inc(tx.amount)
    BALANCE_GAUGE.labels(user_id=tx.payer_id).set(new_payer_bal)
    BALANCE_GAUGE.labels(user_id=tx.payee_id).set(new_payee_bal)

    db.insert_transaction(tx)
    cass.insert_audit(tx.tx_id, tx.payer_id, tx.payee_id, tx.amount, tx.status, tx.created_at)
    cache.incr_daily_total(tx.payer_id, tx.amount)
    return tx

# Main Function
def main():
    start_metrics(port = 8000)
    db = PostgresDB()
    cache = CacheRedis()
    cass = CassandraClient(['cassandra'])

    kc = KafkaClient(KAFKA_BOOTSTRAP, KAFKA_TOPIC)
    consumer = kc.consumer()
    print('Starting consumer...')
    for message in consumer:
        try:
            msg = message.value
            print('Consumed', msg)
            tx = process_message(msg, db, cache, cass)
            print('Processed:', tx)
        except Exception as e:
            print('Error processing message', e)
            time.sleep(1)


if __name__ == '__main__':

    main()
