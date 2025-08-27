# Importing Required Libraries
from dataclasses import dataclass
from datetime import datetime

# Creating Transaction Data Class
@dataclass
class Transaction:
    tx_id: str
    payer_id: str
    payee_id: str
    amount: int
    currency: str = 'INR'
    created_at: datetime = None
    status: str = 'PENDING'
    reason: str = ''