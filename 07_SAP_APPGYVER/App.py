from flask import  Flask, request, make_response, jsonify
import requests
import json
import os
from flask_cors import CORS, cross_origin

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

url = "https://0509.callidusondemand.com/"
cf_port = os.getenv("PORT")
user = 'Deepan'
password='Msd183$$'

def get_request(base_url, headers):
    r = requests.get(base_url ,headers=headers, auth=(user,password)) 
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
        base_url = url+"/api/v2/positions?top=100"
        headers = {'Content-Type': 'application/json', 'Accept':'application/json'}
        r = requests.get(base_url ,headers=headers, auth=(user,password)) 
        pos_data = json.loads(r.text)
        print(pos_data)
        # raw_pos_data = pos_data
        # [pos_list['name'] for pos_list in raw_pos_data]
        for item in pos_data['positions']:
            pos_lst.append({'label' +'":"' + item['name'],'value' +'":"' + item['name']})
        print(pos_lst)
        # raw_pos_data=pos_data['positions'][0]['name']
        return str((pos_lst)).replace("'",'"')
        # return jsonify([pos_list['name'] for pos_list in raw_pos_data])

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
        #print(pos_lst)
        #return jsonify(period_lst)
        return str((period_lst)).replace("'",'"')
        # return jsonify([period_list['name'] for period_list in raw_period_data])

if __name__ == '__main__':
	if cf_port is None:
		# app.run(host='0.0.0.0', port=5000, debug=True)
         app.run(host='localhost', port=5000, debug=True)
	else:
         app.run(host='localhost', port=5000, debug=True)
		# app.run(host='0.0.0.0', port=int(cf_port), debug=True)
