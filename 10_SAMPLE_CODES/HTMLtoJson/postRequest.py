import requests
import json

user='Deepan'
password='Msd183$$'
base_url="https://0509.callidusondemand.com/TrueComp-SaaS/services/rest/calculate/incentiveFromTransaction"
payload = {
"position": "I352401",
"payeeId": "I352401",
"searchDate": "2020-01-01",
"returnCurrentValue": False,
"returnLog": False,
"showStackTrace": False,
"tracing": "internal",
"returnCalculation": True,
"salesTransactions": [
{"eventType": { "eventTypeId": "Sales Actual" }, "productId":"", "value":{ "amount":120,"unitType":"USD"}, 
"compensationDate": "2020-01-01"
}]}

headers={
    'Content-type':'application/json',
    'Accept' : '*/*'
}
r = requests.post(base_url,auth=(user,password),json=payload)
results = json.loads(r.text)
print(results['calculation'])
