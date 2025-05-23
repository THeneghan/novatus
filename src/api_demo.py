import json
import requests
import datetime

my_json = {"transaction_id":["TX999999"],"timestamp":[datetime.datetime.now().isoformat()],
           "amount":[50000],"currency":["USD"],"instrument_type":["IRS"],"region":["US"],"trade_type":["Blah"],
           "status":["Blah"]}



req=requests.post("http://0.0.0.0:80/customer_transaction", data=json.dumps(my_json))
print(req.json())