import requests
import json

def getJWT(token):
    base_url="https://sandbox.webcomcpq.com/api/rd/v1/core/GenerateJWT"
    my_headers={"Authorization" : "Bearer "+token}
    r= requests.post(base_url, headers=my_headers)
    response= json.loads(r.text)
    jwt_token=response['token']
    #print(jwt_token)
    return jwt_token

base_url="https://sandbox.webcomcpq.com/basic/api/token"
data="grant_type=password&username=deepan.s&password=Msd183$$&domain=PROFSERINDIA"
r = requests.post(base_url, data=data)
pos_data = json.loads(r.text)
api_token =pos_data['access_token']
jwtToken=getJWT(api_token)
print(jwtToken)

