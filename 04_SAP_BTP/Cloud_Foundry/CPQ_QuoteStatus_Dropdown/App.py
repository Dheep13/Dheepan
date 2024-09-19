
from dash_bootstrap_components._components.Card import Card
import pandas as pd
from collections import Counter
from dash import html,dcc
from dash.dependencies import Input, Output
import plotly.express as px
import requests
import json
import dash
import dash_bootstrap_components as dbc
from jupyter_dash import JupyterDash
from flask import request
import os

tenant_url='https://sandbox.webcomcpq.com'
cf_port = os.getenv("PORT")

data = {
    "username":'deepan.s',
    "grant_type":"password",
    "password":'Msd183$$',
    "domain":'PROFSERINDIA'

}
#Get API token using basic auth
def getApiToken():
    base_url=tenant_url+"/basic/api/token"
    #my_headers={"Authorization" : "Bearer "+token}
    r= requests.post(base_url, data=data)
    response= json.loads(r.text)
    print(response['access_token'])
    access_token=response['access_token']
    # print(jwt_token)
    return access_token

api_token=getApiToken()

#Get JWT token using api token
def getJWT(token):
    base_url=tenant_url+"/api/rd/v1/core/GenerateJWT"
    my_headers={"Authorization" : "Bearer "+token}
    r= requests.post(base_url, headers=my_headers)
    response= json.loads(r.text)
    jwt_token=response['token']
    #print(jwt_token)
    return jwt_token

jwt_token=getJWT(api_token)
print('Jwt Token is : ' +jwt_token)

#Quote_Status is the name of the custom table that was created in CPQ
#Ensure the table has data
# use the jwt token to get data from custom table
base_url = tenant_url+"/api/custom-table/v1/customTables/Quote_Status/entries"
# #my_headers = {"Authorization" : "Bearer "+jwt_token}
my_headers = {"Authorization" : jwt_token}
response = requests.get(base_url, headers=my_headers)
data = json.loads(response.text)
print(data)

cnt = Counter()
for i in data['value']:
    cnt[i['STATUS']] += 1

df=pd.DataFrame(cnt.items(),columns=['Status', 'Count'])
print(df)
app = JupyterDash(external_stylesheets=[dbc.themes.SLATE])

#----------------------------------APP LAYOUT---------------------------#
app.layout = html.Div([ 
        dbc.Card(
        [
            
        dbc.CardBody(
            [ 
        dbc.Row([
                dbc.Col([html.Label(['Select Chart Type'],style={"font-size":"200 px", "color":"white"}),
        dcc.Dropdown(
            id='my_dropdown',
            options=[
                     {'label': 'Bar Chart', 'value':'Bar Chart' },
                     {'label': 'Pie Chart', 'value': 'Pie Chart'},
                                         
            ],
            value='Bar Chart',
            multi=False,
            clearable=False,
            style={"width": "100%","color":"black"},
            

        )], width=6, align='start', style={"font-size":"200 px","height":"30px"})   ,
    
    ])
        ])]
            
        ,style={"width": "50%", "height":"100px"}, color='info', inverse=True),
    html.Br(),
    html.Div([
        dbc.Card(
        dbc.CardBody([ 
        dbc.Row([
                dbc.Col([
                    dcc.Graph(id='the_graph')
                ], width=6)      
    ])
        ]) 
        ) 
    ])
])

#----------------------------------------App Decorators--------------------------------------#

@app.callback(
    Output(component_id='the_graph', component_property='figure'),
    [Input(component_id='my_dropdown', component_property='value')]
)
def update_output_div(my_dropdown):
    if my_dropdown:
        if my_dropdown == 'Pie Chart':
            piechart=px.pie(
                        df, values=df.Count, names=df.Status, title='Quote Status'
                    ).update_layout(
                        template='ggplot2'
                    )
            return piechart

        elif my_dropdown == 'Bar Chart':
            barchart=px.bar(
                        df, x=df.Count, y=df.Status, title='Quote Status'
                    ).update_layout(
                        template='ggplot2'
                    )
            return barchart
    else:
        return  dash.no_update, dash.no_update

if __name__ == '__main__':
	if cf_port is None:
		app.run_server(host='0.0.0.0', port=5000, debug=True)
	else:
		app.run_server(host='0.0.0.0', port=int(cf_port), debug=True)