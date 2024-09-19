from cgitb import html
from bs4 import BeautifulSoup
import re
import requests
import json
import estimator_creds as ec
from flask import Flask, request, jsonify
import os
from flask_cors import CORS, cross_origin

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'
cf_port = os.getenv("PORT")


url = "https://0509.callidusondemand.com/"
cf_port = os.getenv("PORT")

def get_request(base_url, headers):
    r = requests.get(base_url ,headers=headers, auth=(ec.user,ec.password)) 
    data = json.loads(r.text)
    return data

@app.route('/Achievement',methods=['POST','GET'])
@cross_origin()
def Achievement():
    position = request.args.get('position')
    value = request.args.get('value')
    comp_date = request.args.get('compdate')
    base_url = "https://0509.callidusondemand.com/api/v2/payments?$filter=(position eq 4785074604087153 and period eq 2533274790396152)"
    pay_headers = {'Accept': '*/*', 'Accept': 'application/json'}
    r = requests.get(base_url, headers=pay_headers, auth=(ec.user,ec.password))
    payments = json.loads(r.text)
    payment_values=[]
    current_earnings=0.0
    payment_values=payments["payments"]
    for x in payment_values:
        current_earnings+=x["value"]["value"]
    print(current_earnings)
    print('value is :' +str(value))
    print('comp_date is :' +str(comp_date))
    print('position is :' +str(position))
    base_url = url+"/api/v2/salesTransactions?$filter=(salesOrder eq 14355223812270777)"
    headers = {'Content-Type': 'application/json', 'Accept':'application/json'}
    opp_details=get_request(base_url,headers)
    
    base_url="https://0509.callidusondemand.com/TrueComp-SaaS/services/rest/calculate/incentiveFromTransaction"
    payload_act = {

"position": position,
"payeeId": position,
"searchDate": "2020-01-01",
"returnCurrentValue": False,
"returnLog": False,
"showStackTrace": True,
"tracing": "internal",
"returnCalculation": True,
"salesTransactions": [
{"eventType": { "eventTypeId": "Sales Actual" }, "productId":"", "value":{ "amount":value,"unitType":"USD"}, 
"compensationDate": comp_date
}]}

    payload_zero = {

"position": position,
"payeeId": position,
"searchDate": "2020-01-01",
"returnCurrentValue": False,
"returnLog": False,
"showStackTrace": True,
"tracing": "internal",
"returnCalculation": True,
"salesTransactions": [
{"eventType": { "eventTypeId": "Sales Actual" }, "productId":"", "value":{ "amount":0,"unitType":"USD"}, 
"compensationDate": comp_date
}]}
    r = requests.post(base_url,auth=(ec.user,ec.password),json=payload_act)
    results = json.loads(r.text)
    r_zero = requests.post(base_url,auth=(ec.user,ec.password),json=payload_zero)
    results_zero = json.loads(r_zero.text)
    # print(results['calculation'])
    html_doc=results['calculation']
    soup = BeautifulSoup(html_doc, 'html.parser')
    json_resp=results['outputIncentives']
    json_resp_zero=results_zero['outputIncentives']
    Total_Earnings=0.0
    for i in json_resp:
        Total_Earnings+=i["value"]["amount"]
    print(Total_Earnings)
    Estimated_Comm_For_Opp=Total_Earnings - current_earnings
    print('"'+'Total_Earnings '+'"' + ":" + '"'+str(Total_Earnings)+ '"')
    print('"'+'estimatedCommissionForOpp '+'"' + ":" + '"'+str(Estimated_Comm_For_Opp)+ '"')
    est_api_total_earnings=('"'+'totalEarnings'+'"' + ":" + '"'+str(Total_Earnings)+ '"')
    estimated_for_curr=('"'+'estimatedCommissionForOpp'+'"' + ":" + '"'+str(Estimated_Comm_For_Opp)+ '"')
    existing_payment=('"'+'currEarningsInComp'+'"' + ":" + '"'+str(current_earnings)+ '"')
    
    
    result_str= "RESULT"
    warning_str= "Warning: applying overwrite"
  
    json_str=''
    #Achievement calc
    lst=(soup.find_all(string=re.compile("SMR Sales Achievement")))
    for i in lst:
        if result_str in i:
            est_str='"estimatedAchievement":'  + '"' + str(re.search('value(.*)%', i).group(1)).replace(' ','') + '"'
            
            if  not json_str:
                json_str+=est_str
            else:
                json_str=json_str+','+ est_str
        
        elif warning_str in i:
            war_str='"currentAchievement":' + '"' + str(re.search('with value(.*)%', i).group(1)).replace(' ','') + '"'
            
            if not json_str:
                json_str+=war_str
            else:
                json_str=json_str+','+ war_str
    
    final_json= "{" + existing_payment + ","+ est_api_total_earnings + "," + estimated_for_curr +  "," + json_str + "}"   
    return final_json

if __name__ == '__main__':
    if cf_port is None:
        app.run(host='localhost', port=5000, debug=True)
    else:
        app.run(host='localhost', port=5000, debug=True)
