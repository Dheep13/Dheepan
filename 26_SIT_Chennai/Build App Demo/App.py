from flask import  Flask, request, make_response, jsonify
import requests
import json
import os
from flask_cors import CORS, cross_origin
import Credentials as cr
app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

url = "https://0509.callidusondemand.com/"
cf_port = os.getenv("PORT")

def get_request(base_url, headers):
    r = requests.get(base_url ,headers=headers, auth=(cr.user,cr.password)) 
    data = json.loads(r.text)
    return data

@app.route('/webhook/<position>/<period>',methods=['GET'])
@cross_origin()
def webhook(position,period):
  
    if (request.method == 'GET'):
        base_url = url+"/api/v2/positions?$filter=name eq "+position
        headers = {'Content-Type': 'application/json', 'Accept':'application/json'}
        pos_data=get_request(base_url,headers)
        print(pos_data)
        ruleElementOwnerSeq =pos_data['positions'][0]['ruleElementOwnerSeq']
        base_url = url+"/api/v2/periods?$filter=name eq "+ period 
        headers = {'Content-Type': 'application/json', 'Accept':'application/json'}
        period_data=get_request(base_url,headers)
        periodSeq=period_data['periods'][0]['periodSeq']
        base_url = url+"/api/v2/payments?$filter=period eq "+ periodSeq + " and position eq "+ ruleElementOwnerSeq
        headers = {'Content-Type': 'application/json', 'Accept':'application/json'}
        payments=get_request(base_url,headers)
        final_payment=payments['payments'][0]['value']
        return jsonify(final_payment)
        #/AG00019/"January 2020"


if __name__ == '__main__':
	if cf_port is None:
		app.run(host='0.0.0.0', port=5000, debug=True)
	else:
		app.run(host='0.0.0.0', port=int(cf_port), debug=True)