-- SQL script to initialize the database schema
CREATE TABLE IF NOT EXISTS users (
  user_id TEXT PRIMARY KEY,
  last_item_number TEXT,
  last_amount NUMERIC,
  created_at TIMESTAMP DEFAULT now()
);

-- Table to store bid records
CREATE TABLE IF NOT EXISTS bids (
  bid_id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  item_number TEXT NOT NULL,
  amount NUMERIC NOT NULL,
  created_at TIMESTAMP DEFAULT now()
);