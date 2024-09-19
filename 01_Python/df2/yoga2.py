import requests
import pandas as pd
import base64
from pandas.io.json import json_normalize
import json
# from geopy.geocoders import Nominatim
# import folium
apidomain = "https://0509.callidusondemand.com/api/v2/"
payments = "salesTransactions"
expand = "?expand=transactionAssignments,eventType&"
filter = '$filter=genericAttribute29 eq "CEB" and eventType/eventTypeId eq "Sales Actual"'
skip = "&skip=0&top=100"
#processingunit = "processingUnits"
headers = {
    'authorization': "Basic UmVzdEFkbWluOlBlcmYwcm1AMQ==",
    'content-type': "application/json",
    'Accept': "application/json",
    'cache-control': "no-cache"
    }
# def pipelinejob():
response = requests.request("GET",apidomain+payments+expand+filter,headers=headers)
response.encoding = 'utf-8'
results = json.loads(response.text)
    #print(results)
json_str = json.dumps(results["salesTransactions"])
df = pd.read_json(json_str)
df = df[['genericAttribute29','genericAttribute30','genericAttribute31','salesTransactionSeq','transactionAssignments','value',
         'eventType','compensationDate']]
# df = pd.DataFrame(j)
name = [x.get('positionName')  for d in df.transactionAssignments if d for x in d]
print(name)
