# Importing necessary libraries
import uuid
from datetime import datetime

# Function to generate a unique bid ID
def gen_bid_id():
    full_id = uuid.uuid4()
    full_id_str = str(full_id)
    return full_id_str[:5]

# Function to get current timestamp in ISO format
def now_iso():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S") #isoformat()