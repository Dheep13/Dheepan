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
@app.route('/webhook/<position>',methods=['GET'])
@cross_origin()
def webhook(position):
    if (request.method == 'GET'):
        base_url = url+"api/v2/positions?$filter=name eq "+position+" &select=name,title&expand=title"
        headers = {'Content-Type': 'application/json', 'Accept':'application/json'}
        r = requests.get(base_url ,headers=headers, auth=('Deepan','Msd183$$'))
        print(r.text)
        pos_data = json.loads(r.text)
        print(pos_data)
        #return corsify_response(pos_data)
        #json_str= '['+str(pos_data)+']'
        json_str= pos_data['positions'][0]['title']
        print (json_str)
        return jsonify(json_str)

if __name__ == '__main__':
	if cf_port is None:
		app.run(host='0.0.0.0', port=5000, debug=True)
        #  app.run(host='localhost', port=5000, debug=True)
	else:
        #  app.run(host='localhost', port=5000, debug=True)
		app.run(host='0.0.0.0', port=int(cf_port), debug=True)