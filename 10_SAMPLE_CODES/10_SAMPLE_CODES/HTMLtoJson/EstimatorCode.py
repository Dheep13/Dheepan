from cgitb import html
from bs4 import BeautifulSoup
import re
import requests
import json
import estimator_creds as ec
from flask import Flask, request, jsonify
import os

app = Flask(__name__)
cf_port = os.getenv("PORT")


url = "https://0509.callidusondemand.com/"
cf_port = os.getenv("PORT")

def get_request(base_url, headers):
    r = requests.get(base_url ,headers=headers, auth=(ec.user,ec.password)) 
    data = json.loads(r.text)
    # print (data)
    return data

@app.route('/Achievement',methods=['POST','GET'])
# @cross_origin()
def Achievement():
    position = request.args.get('position')
    value = request.args.get('value')
    comp_date = request.args.get('compdate')
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
}
]
}

    payload_zero = {

"position": "I352401",
"payeeId": "I352401",
"searchDate": "2020-01-01",
"returnCurrentValue": False,
"returnLog": False,
"showStackTrace": True,
"tracing": "internal",
"returnCalculation": True,
"salesTransactions": [
{"eventType": { "eventTypeId": "Sales Actual" }, "productId":"", "value":{ "amount":0,"unitType":"USD"}, 
"compensationDate": "2020-01-01"
}
]
}

    r = requests.post(base_url,auth=(ec.user,ec.password),json=payload_act)
    results = json.loads(r.text)
    r_zero = requests.post(base_url,auth=(ec.user,ec.password),json=payload_zero)
    results_zero = json.loads(r_zero.text)
    # print(results['calculation'])
    html_doc=results['calculation']
    soup = BeautifulSoup(html_doc, 'html.parser')
    json_resp=results['outputIncentives']
    json_resp_zero=results_zero['outputIncentives']
    Estimated_Total_Commission=json_resp[0]["value"]["amount"]
    Estimated_Comm_For_Opp=(json_resp[0]["value"]["amount"]) - (json_resp_zero[0]["value"]["amount"])
    print('"'+'estimatedTotalCommission '+'"' + ":" + '"'+str(Estimated_Total_Commission)+ '"')
    print('"'+'estimatedCommissionForOpp '+'"' + ":" + '"'+str(Estimated_Comm_For_Opp)+ '"')
    estimated_total=('"'+'estimatedTotalCommission'+'"' + ":" + '"'+str(Estimated_Total_Commission)+ '"')
    estimated_for_curr=('"'+'estimatedCommissionForOpp'+'"' + ":" + '"'+str(Estimated_Comm_For_Opp)+ '"')
    
    
    # print(soup)

    result_str= "RESULT"
    warning_str= "Warning: applying overwrite"
    est_comm_str="= RESULT( IR_Sales Actual"
    
    # print(result.group(1))
    # print(soup.prettify())
    # print(soup.find_all('font'))
    # print(soup.find("SMR Shipping actual"))
    # print(soup.b.contents[1])
    # print(soup.find_all(string=re.compile("SMR Sales Achievement")))
    json_str=''
    #Achievement calc
    lst=(soup.find_all(string=re.compile("SMR Sales Achievement")))
    for i in lst:
        if result_str in i:
            # print('"Estimated Achievement" :'+ '"'+i.replace('=','')+'"')
            # print(i)
            # print('"Estimated Achievement" :' + str(re.search('value(.*)%', i).group(1)))
            est_str='"estimatedAchievement":'  + '"' + str(re.search('value(.*)%', i).group(1)).replace(' ','') + '"'
            
            if  not json_str:
                json_str+=est_str
            else:
                json_str=json_str+','+ est_str
        
        elif warning_str in i:
            # print('"Estimated Achievement" :'+ '"'+i.replace('=','')+'"')
            # print(i)
            # print('"Current Achievement" :' + str(re.search('with value(.*)%', i).group(1)))
            war_str='"currentAchievement":' + '"' + str(re.search('with value(.*)%', i).group(1)).replace(' ','') + '"'
            
            if not json_str:
                json_str+=war_str
            else:
                json_str=json_str+','+ war_str
    
    # print(json_str)
    
    final_json= "{" + estimated_total + "," + estimated_for_curr +  "," + json_str + "}"   
    return final_json
    # replacers = {']':'','[':''} 
    #Estimated Commission calc
    # lst=(soup.find_all(string=re.compile("PL Sales Actual")))
    # for i in lst:
    #     # print(i)
    #     if est_comm_str in i:
    #         # print(i)
    #         #this also works
    #         # new_comm=results = str(re.search('with value (.*)U', i).group(1)).replace('=','')
    #         # new_str ='"Total Estimated Commissions" :' + '"' + new_comm + '"'
            
    # print(new_str)
    #     #     if  not json_str:
        #         json_str+=est_str
        #     else:
        #         json_str=json_str+','+ est_str
        
        # elif warning_str in i:
        #     # print('"Estimated Achievement" :'+ '"'+i.replace('=','')+'"')
        #     # print(i)
        #     # print('"Current Achievement" :' + str(re.search('with value(.*)%', i).group(1)))
        #     war_str='"Current Achievement" :' + '"' + str(re.search('with value(.*)%', i).group(1)) + '"'
            
        #     if not json_str:
        #         json_str+=war_str
        #     else:
        #         json_str=json_str+','+ war_str

    # return "{"+(json_str) + "}"
    


if __name__ == '__main__':
	if cf_port is None:
		app.run(host='0.0.0.0', port=5000, debug=True)
	else:
		app.run(host='0.0.0.0', port=int(cf_port), debug=True)

# Achievement()