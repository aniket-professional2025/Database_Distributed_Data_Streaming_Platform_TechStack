CREATE KEYSPACE IF NOT EXISTS bidding WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

USE bidding;

CREATE TABLE IF NOT EXISTS bid_logs (
  bid_id text PRIMARY KEY,
  user_id text,
  item_number text,
  amount decimal,
  status text,
  reason text, 
  created_at timestamp
);