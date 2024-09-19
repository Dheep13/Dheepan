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
#import dash_auth
from flask import request


def getJWT(token):
    base_url="https://sandbox.webcomcpq.com/api/rd/v1/core/GenerateJWT"
    my_headers={"Authorization" : "Bearer "+token}
    r= requests.post(base_url, headers=my_headers)
    response= json.loads(r.text)
    jwt_token=response['token']
    #print(jwt_token)
    return jwt_token

# base_url="https://sandbox.webcomcpq.com/basic/api/token"
# data="grant_type=password&username=deepan.s&password=Msd183$$&domain=PROFSERINDIA"
# r = requests.post(base_url, data=data)
# pos_data = json.loads(r.text)
# api_token =pos_data['access_token']
# jwtToken=getJWT(api_token)
#print(jwtToken)
jwtToken="Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE2MzE3NDA3NzksInVzZXJuYW1lIjoiZGVlcGFuLnMiLCJkb21haW4iOiJQUk9GU0VSSU5ESUEifQ.43vu-8mS8FMXUu-POywAqTrXijmt81uVFDa-4knr68A"
base_url = "https://sandbox.webcomcpq.com/api/custom-table/v1/customTables/Quote_Status/entries"
#my_headers = {"Authorization" : "Bearer "+jwtToken}
my_headers = {"Authorization" : jwtToken}
response = requests.get(base_url, headers=my_headers)
#print(response)
data = json.loads(response.text)
cnt = Counter()
for i in data['value']:
    cnt[i['STATUS']] += 1

df=pd.DataFrame(cnt.items(),columns=['Status', 'Count'])

#df = pd.read_csv("Urban_Park_Ranger_Animal_Condition_Response.csv")

# you need to include __name__ in your Dash constructor if
# you plan to use a custom CSS or JavaScript in your Dash apps
app = JupyterDash(external_stylesheets=[dbc.themes.SLATE])
#port= os.getenv("PORT")
#app = dash.Dash(__name__)
# VALID_USERNAME_PASSWORD_PAIRS = [
#     ['Deepan', '1234']
# ]
# auth = dash_auth.BasicAuth(
#     app,
#     VALID_USERNAME_PASSWORD_PAIRS
# )

# header = html.Div([

#     dbc.Card(
#         [      
#         dbc.CardBody(
#             [ 
#         dbc.Row([       
#         dbc.Col([html.H2(id='show-output', children=''),
#     html.Button('Click to Authenticate', id='button')], width=5, align="start")   
#     ], justify="start")
#         ])]     
#         )
# ], className='container')

header = dbc.Card(
    [
        # dbc.CardHeader("Login Info"),
        dbc.CardBody(
            [
                html.H6(id="show-output", children='', style={"color":"#660033"}),
                html.Button('Authenticate', id='button'),
            ]
        ),
    ],
    style={"width": "18%","height":"80px","background-color": "#ccffff","position":"absolute",
  "right": "0px"}
)

#---------------------------------------------------------------
app.layout = html.Div([ header,
        dbc.Card(
        [
            
        dbc.CardBody(
            [ 
        dbc.Row([
                dbc.Col([html.Label(['Quote Status'],style={"font-size":"200 px"}),
        dcc.Dropdown(
            id='my_dropdown',
            options=[
                     {'label': 'Bar Chart', 'value':'Bar Chart' },
                     {'label': 'Pie Chart', 'value': 'Pie Chart'}
                     
            ],
            value='Bar Chart',
            multi=False,
            clearable=False,
            style={"width": "100%"}
        )], width=6, align='start', style={"font-size":"200 px","height":"30px"})   ,
        #dbc.Col([dbc.CardImg(src="/static/images/download_monkey.jpg", top=True, style={"width":"40px","height":"50px"} )], align='end')   
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

#---------------------------------------------------------------

@app.callback(
    [Output(component_id='show-output', component_property='children'), Output(component_id='the_graph', component_property='figure')],
    [Input(component_id='button', component_property='n_clicks'),Input(component_id='my_dropdown', component_property='value')]
)
def update_output_div(n_clicks, my_dropdown):
    #username = request.authorization['username']
    username = "Deepan"
    #password = request.authorization['password']
    if n_clicks:
        if my_dropdown == 'Pie Chart':
            piechart=px.pie(
                        df, values=df.Count, names=df.Status, title='Quote Status'
                    ).update_layout(
                        template='ggplot2',
                        # plot_bgcolor= 'rgba(1, 4, 0, 5)',
                        # paper_bgcolor= 'rgba(1, 4, 0, 2)',
                    )
            return 'Hello, welcome %s' % (username), piechart

        elif my_dropdown == 'Bar Chart':
            barchart=px.bar(
                        df, x=df.Count, y=df.Status, title='Quote Status'
                    ).update_layout(
                        template='ggplot2',
                        # plot_bgcolor= 'rgba(1, 4, 0, 5)',
                        # paper_bgcolor= 'rgba(1, 4, 0, 2)',
                    )
            return 'Hello, welcome %s' % (username), barchart
    else:
        return  dash.no_update, dash.no_update

if __name__ == '__main__':
    app.run_server(debug=True)
