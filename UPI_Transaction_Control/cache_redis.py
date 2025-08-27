# Importing Required Packages
import redis
from config import REDIS
from datetime import date

# CacheRedis Class
class CacheRedis:
    def __init__(self):
        self.r = redis.Redis(host=REDIS['host'], port=REDIS['port'], decode_responses=True)

    def get_balance(self, user_id):
        val = self.r.get(f"balance:{user_id}")
        return int(val) if val is not None else None

    def set_balance(self, user_id, amount):
        self.r.set(f"balance:{user_id}", int(amount))

    def incr_daily_total(self, user_id, amount):
        key = f"daily:{user_id}:{date.today()}"
        return self.r.incrby(key, int(amount))

    def get_daily_total(self, user_id):
        key = f"daily:{user_id}:{date.today()}"
        val = self.r.get(key)
        return int(val) if val else 0