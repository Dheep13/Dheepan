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


# @app.route('/Achievement',methods=['POST','GET'])
# @cross_origin()
def Achievement():
    # position = request.args.get('position')
    # value = request.args.get('value')
    base_url = url+"/api/v2/salesTransactions?$filter=(salesOrder eq 14355223812270777)"
    headers = {'Content-Type': 'application/json', 'Accept':'application/json'}
    opp_details=get_request(base_url,headers)
    
    base_url="https://0509.callidusondemand.com/TrueComp-SaaS/services/rest/calculate/incentiveFromTransaction"
    payload = {

"position": "I352401",
"payeeId": "I352401",
"searchDate": "2020-01-01",
"returnCurrentValue": False,
"returnLog": False,
"showStackTrace": True,
"tracing": "internal",
"returnCalculation": True,
"salesTransactions": [
{"eventType": { "eventTypeId": "Sales Actual" }, "productId":"", "value":{ "amount":112020,"unitType":"USD"}, 
"compensationDate": "2020-01-01"
}
]
}

    r = requests.post(base_url,auth=(ec.user,ec.password),json=payload)
    results = json.loads(r.text)
    # print(results['calculation'])
    html_doc=results['calculation']
    soup = BeautifulSoup(html_doc, 'html.parser')
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
            est_str='"Estimated Achievement" :'  + '"' + str(re.search('value(.*)%', i).group(1)) + '"'
            
            if  not json_str:
                json_str+=est_str
            else:
                json_str=json_str+','+ est_str
        
        elif warning_str in i:
            # print('"Estimated Achievement" :'+ '"'+i.replace('=','')+'"')
            # print(i)
            # print('"Current Achievement" :' + str(re.search('with value(.*)%', i).group(1)))
            war_str='"Current Achievement" :' + '"' + str(re.search('with value(.*)%', i).group(1)) + '"'
            
            if not json_str:
                json_str+=war_str
            else:
                json_str=json_str+','+ war_str
                
    replacers = {']':'','[':''} 
    #Estimated Commission calc
    lst=(soup.find_all(string=re.compile("PL Sales Actual")))
    for i in lst:
        # print(i)
        if est_comm_str in i:
            print(i)
    #         new_comm=(str(re.findall("=\s*?(\d+\.\d+|\d+)",i))).replace('[','').replace(']','').replace("'",'')
    #         new_str ='"Total Estimated Commissions" :' + '"' + new_comm + '"'
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
    


# if __name__ == '__main__':
# 	if cf_port is None:
# 		app.run(host='0.0.0.0', port=5000, debug=True)
# 	else:
# 		app.run(host='0.0.0.0', port=int(cf_port), debug=True)

Achievement()