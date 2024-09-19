from unicodedata import name
from flask import  Flask, request, make_response, jsonify
import requests
import json
import os
from flask_cors import CORS, cross_origin

app = Flask(__name__)
# cors = CORS(app)
# app.config['CORS_HEADERS'] = 'Content-Type'

url = "https://0509.callidusondemand.com/"
cf_port = os.getenv("PORT")
user = 'Deepan'
password='Msd183$$'

def get_request(base_url, headers):
    r = requests.get(base_url ,headers=headers, auth=(user,password)) 
    data = json.loads(r.text)
    print (data)
    return data
    
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
            pos_lst.append({'Position' +'":"' + item['name'],'FirstName' +'":"' + item['genericAttribute6'],'LastName' +'":"' + item['genericAttribute3'],'Image' +'":"' + item['genericAttribute5'],'Email' +'":"' + item['genericAttribute4']})
        print(pos_lst)
        str1=str((pos_lst)).replace("'",'"')
        str1=str1.replace("[",'')
        str1=str1.replace("]",'')
        return ((str1))
if __name__ == '__main__':
    app.run(host='localhost', port=5000, debug=True)
