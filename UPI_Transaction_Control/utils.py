# Importing Required Packages
import uuid
from datetime import datetime

# Utility Functions: Getting the transaction Id
def gen_tx_id():
    return str(uuid.uuid4())

# Utility Functions: Getting the current UTC time
def now():
    return datetime.utcnow()