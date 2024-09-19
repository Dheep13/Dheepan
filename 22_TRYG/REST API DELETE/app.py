import requests
import json
import pandas as pd

###### Set your Envionment Variables & Modify to filter your data ###########
env = "https://<tenantid>.callidusondemand.com/api/v2/"
endpoint = "salesTransactions"
filtered = '?expand=eventType&$filter=originTypeId eq "manual" and eventType/eventTypeId eq "LicensesNF" '
size = "&skip=0&top=3"  ## only 3 records will be shown and make a change it to your need

headers = {
'authorization': "Basic eW9XXXXXXXXXXXXXXXXXXXXXXXXXXAMQ==",
'cache-control': "no-cache"
}

######## Get All SalesTransactions Seq into group ###############
response = requests.request("GET", env+endpoint+filtered+size, headers=headers)
response.encoding = 'utf-8'
response.text
transaction = json.loads(response.text)
json_str = json.dumps(transaction["salesTransactions"])

####### Parse the json object and element to get SalesTransaction###############
df = pd.read_json(json_str)
df = df['salesTransactionSeq']


######## Loop each salestransactionSeq to DELETE the record ##################
i = 0

for i in range(len(df)):
    response = requests.request("DELETE", env+endpoint+"("+str(df[i])+")", headers=headers)
    response.encoding = 'utf-8'
    if (response.status_code == 200):
        print(str(df[i]) + "---->" + response.text)
    else:
        print(str(df[i]) + "---->" + response.text)
        break
        #i = i+1

###### Break point is added - remove # to continuous loop ###########
