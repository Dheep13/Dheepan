from unicodedata import name
from flask import  Flask, request, make_response, jsonify
import requests
import json
import os
from flask_cors import CORS, cross_origin
import credentials as cr

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

url = "https://0509.callidusondemand.com/"
cf_port = os.getenv("PORT")


def get_request(base_url, headers):
    r = requests.get(base_url ,headers=headers, auth=(cr.user,cr.password)) 
    data = json.loads(r.text)
    print (data)
    return data

@app.route('/webhook/<position>/<period>',methods=['GET'])
@cross_origin()
def webhook(position,period):
    # period = str(request.args.get('period'))
    if (request.method == 'GET'):
        base_url = url+"/api/v2/positions?$filter=name eq "+position
        headers = {'Content-Type': 'application/json', 'Accept':'application/json'}
        pos_data=get_request(base_url,headers)
        # r = requests.get(base_url ,headers=headers, auth=(user,password)) 
        # pos_data = json.loads(r.text)
        print(pos_data)
        ruleElementOwnerSeq =pos_data['positions'][0]['ruleElementOwnerSeq']
        #period = period.replace(" ", "")
        base_url = url+"/api/v2/periods?$filter=name eq "+ period 
        headers = {'Content-Type': 'application/json', 'Accept':'application/json'}
        # r = requests.get(base_url ,headers=headers, auth=(user,password)) 
        period_data=get_request(base_url,headers)
        # period_data = json.loads(r.text)
        periodSeq=period_data['periods'][0]['periodSeq']
        base_url = url+"/api/v2/payments?$filter=period eq "+ periodSeq + " and position eq "+ ruleElementOwnerSeq
        headers = {'Content-Type': 'application/json', 'Accept':'application/json'}
        # r = requests.get(base_url ,headers=headers, auth=(user,password)) 
        payments=get_request(base_url,headers)
        # payments = json.loads(r.text)
        final_payment=payments['payments'][0]['value']
        return jsonify(final_payment)
        #/AG00019/"January 2020"


@app.route('/positions',methods=['GET'])
@cross_origin()
def position():
    if (request.method == 'GET'):
        pos_lst =[]
        base_url = url+"/api/v2/positions?$filter=((name eq User111) or (name eq User111-1) or (name eq User111-2) or (name eq User111-3))"
        headers = {'Content-Type': 'application/json', 'Accept':'application/json'}
        r = requests.get(base_url ,headers=headers, auth=(user,password)) 
        pos_data = json.loads(r.text)
        for item in pos_data['positions']:
            pos_lst.append({'Position' +'":"' + item['name'],'FirstName' +'":"' + item['genericAttribute6'],'LastName' +'":"' + item['genericAttribute3'],'Image' +'":"' + item['genericAttribute5'],'Email' +'":"' + item['genericAttribute4']})
        print(pos_lst)
        return str((pos_lst)).replace("'",'"')

@app.route('/positionsGetOne/<position>',methods=['GET'])
@cross_origin()
def positionsGetOne(position):
    if (request.method == 'GET'):
        pos_lst =[]
        base_url = url+"/api/v2/positions?$filter=(name eq "+position + ")"
        headers = {'Content-Type': 'application/json', 'Accept':'application/json'}
        r = requests.get(base_url ,headers=headers, auth=(user,password)) 
        pos_data = json.loads(r.text)
        for item in pos_data['positions']:
            pos_lst.append({'Position' +'":"' + item['name'],'FirstName' +'":"' + item['genericAttribute6'],'LastName' +'":"' + item['genericAttribute3'],'Image' +'":"' + item['genericAttribute5'],'Email' +'":"' + item['genericAttribute4'],'Stats' +'":"' + item['genericAttribute2']})
        print(pos_lst)
        str1=str((pos_lst)).replace("'",'"')
        str1=str1.replace("[",'')
        str1=str1.replace("]",'')
        return ((str1))

@app.route('/positions/<name>',methods=['GET'])
@cross_origin()
def getPositionTitle(name):
    if (request.method == 'GET'):
        base_url = url+"/api/v2/positions?$filter=(name eq "+name +")&select=name,title&expand=title"
        headers = {'Content-Type': 'application/json', 'Accept':'application/json'}
        r = requests.get(base_url ,headers=headers, auth=(user,password)) 
        pos_data = json.loads(r.text)
        return jsonify(pos_data["positions"][0]["title"]["displayName"])

@app.route('/periods',methods=['GET'])
@cross_origin()
def period():
    if (request.method == 'GET'):
        period_lst =[]
        base_url = url+"/api/v2/periods?skip=10&top=100"
        headers = {'Content-Type': 'application/json', 'Accept':'application/json'}
        r = requests.get(base_url ,headers=headers, auth=(user,password)) 
        period_data = json.loads(r.text)
        print(period_data)
        for item in period_data['periods']:
            period_lst.append({'label' +'":"' + item['name'],'value' +'":"' + item['name']})
        return str((period_lst)).replace("'",'"')

@app.route('/getOppList',methods=['GET'])
@cross_origin()
def getOppList():
    res_str=''
    base_url = url+"/api/v2/salesTransactions?$filter=(salesOrder eq 14355223812271075) &select=salesOrder,genericAttribute3,productId,subLineNumber,value, genericAttribute4&expand=value,subLineNumber, salesOrder"
    headers = {'Content-Type': 'application/json', 'Accept':'application/json'}
    opp_details=get_request(base_url,headers)
    for i in opp_details['salesTransactions']:
        res=("{"+'"opportunityName":' + '"' + i["genericAttribute3"] + '",' '"opportunityId":' + '"' + str(i["subLineNumber"]["value"]) + '",'
            +'"productId":' + '"' + str(i["productId"]) + '",'+'"opportunityValue":' + '"' + str(i["value"]["value"]) + '",'+'"originalOrderId":' + '"' + str(i["genericAttribute4"]) 
            +'",'+'"salesOrderId":' + '"' + str(i["salesOrder"]["displayName"]) + '"'+ "}")
        if  res_str != '':
            res_str+=(","+res)
        else:
            res_str+=(res)

    res_str = "[" + res_str +"]"
    return(res_str)

@app.route('/getOppList/<oppId>',methods=['GET'])
@cross_origin()
def getOppSingle(oppId):
    res_str=''
    base_url = url+"/api/v2/salesTransactions?$filter=((salesOrder eq 14355223812271075)) &select=salesOrder,genericAttribute3,productId,subLineNumber,value, genericAttribute4&expand=value,subLineNumber, salesOrder"
    headers = {'Content-Type': 'application/json', 'Accept':'application/json'}
    opp_details=get_request(base_url,headers)
    for i in opp_details['salesTransactions']:
        if str(i["subLineNumber"]["value"]) == str(oppId):
            res=("{"+'"opportunityName":' + '"' + i["genericAttribute3"] + '",' '"opportunityId":' + '"' + str(i["subLineNumber"]["value"]) + '",'
                +'"productId":' + '"' + str(i["productId"]) + '",'+'"opportunityValue":' + '"' + str(i["value"]["value"]) + '",'+'"originalOrderId":' + '"' + str(i["genericAttribute4"]) 
                +'",'+'"salesOrderId":' + '"' + str(i["salesOrder"]["displayName"]) + '"'+ "}")
            if  res_str != '':
                res_str+=(","+res)
            else:
                res_str+=(res)
    return(res_str)

if __name__ == '__main__':
	if cf_port is None:
		app.run(host='0.0.0.0', port=5000, debug=True)
        # app.run(host='localhost', port=5000, debug=True)
	else:
        # app.run(host='localhost', port=5000, debug=True)
		app.run(host='0.0.0.0', port=int(cf_port), debug=True)
