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
@app.route('/positions',methods=['GET'])
@cross_origin()
def position():
    if (request.method == 'GET'):
        raw_pos_data =[]
        base_url = url+"/api/v2/positions?skip=10&top=100"
        headers = {'Content-Type': 'application/json', 'Accept':'application/json'}
        r = requests.get(base_url ,headers=headers, auth=('Deepan','Msd183$$')) 
        pos_data = json.loads(r.text)
        print(pos_data)
        raw_pos_data=pos_data['positions']
        #return jsonify(raw_pos_data)
        return jsonify([pos_list['name'] for pos_list in raw_pos_data])

@app.route('/periods',methods=['GET'])
@cross_origin()
def period():
    if (request.method == 'GET'):
        raw_period_data =[]
        base_url = url+"/api/v2/periods?skip=10&top=100"
        headers = {'Content-Type': 'application/json', 'Accept':'application/json'}
        r = requests.get(base_url ,headers=headers, auth=('Deepan','Msd183$$')) 
        period_data = json.loads(r.text)
        print(period_data)
        raw_period_data=period_data['periods']
        #return jsonify(raw_pos_data)
        return jsonify([period_list['name'] for period_list in raw_period_data])

        #/AG00019/"January 2020"

if __name__ == '__main__':
	if cf_port is None:
		# app.run(host='0.0.0.0', port=5000, debug=True)
         app.run(host='localhost', port=5000, debug=True)
	else:
         app.run(host='localhost', port=5000, debug=True)
		# app.run(host='0.0.0.0', port=int(cf_port), debug=True)
