from flask import  Flask, request, make_response, jsonify
import requests
import json
import os
import re
from flask_cors import CORS, cross_origin

base_url="https://0509.callidusondemand.com/TrueComp-SaaS/services/rest/calculate/incentiveFromTransaction"
payload_act = {

"position": "I352401",
"payeeId": "I352401",
"searchDate": "2020-01-01",
"returnCurrentValue": False,
"returnLog": False,
"showStackTrace": True,
"tracing": "internal",
"returnCalculation": True,
"salesTransactions": [
{"eventType": { "eventTypeId": "Sales Actual" }, "productId":"", "value":{ "amount":120,"unitType":"USD"}, 
"compensationDate": '2020-01-01'
}]}

total_earnings=0.0
r = requests.post(base_url,auth=('Deepan','Msd183$$'),json=payload_act)
results = json.loads(r.text)
json_resp=results['outputIncentives']
for i in json_resp:
    total_earnings+=i["value"]["amount"]
print(total_earnings)