"""Run this after running main.py and novatus_api/main.py to load the json to the cusotmer_transaction table"""

import datetime
import json

import requests

my_json = {
    "transaction_id": ["TX999999"],
    "timestamp": [datetime.datetime.now().isoformat()],
    "amount": [50000],
    "currency": ["USD"],
    "instrument_type": ["IRS"],
    "region": ["US"],
    "trade_type": ["Blah"],
    "status": ["Blah"],
}


if __name__ == "__main__":
    req = requests.post("http://0.0.0.0:80/customer_transaction", data=json.dumps(my_json))
