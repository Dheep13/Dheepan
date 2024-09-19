import pandas as pd
import dash
from collections import Counter
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
import requests
import json
import dash_bootstrap_components as dbc
from jupyter_dash import JupyterDash

base_url = "https://sandbox.webcomcpq.com/api/custom-table/v1/customTables/Quote_Status/entries"
my_headers = {"Authorization" : "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE2MzExNDY4MjUsInVzZXJuYW1lIjoiZGVlcGFuLnMiLCJkb21haW4iOiJQUk9GU0VSSU5ESUEifQ.mCIiOhTxD_JDDr12lH8q8E5E52zgyhO90al1BR7MofM"}
response = requests.get(base_url, headers=my_headers)
#print(response)
data = json.loads(response.text)
#print[data]
#for i in data['value']:
    #print(i['QUOTENUMBER'])

 # Tally occurrences of words in a list
cnt = Counter()
for i in data['value']:
    cnt[i['STATUS']] += 1
#print(cnt)
#conv_list = list(cnt.items())
#print(conv_list)

df=pd.DataFrame(cnt.items(),columns=['Status', 'Count'])

#df = pd.read_csv("Urban_Park_Ranger_Animal_Condition_Response.csv")

# you need to include __name__ in your Dash constructor if
# you plan to use a custom CSS or JavaScript in your Dash apps
app = JupyterDash(external_stylesheets=[dbc.themes.SLATE])
#app = JupyterDash(__name__)


#---------------------------------------------------------------
app.layout = html.Div([
        dbc.Card(
        [
            
        dbc.CardBody(
            [ 
        dbc.Row([
                dbc.Col([html.Label(['Quote Status']),
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
        )], width=6)   ,
        dbc.Col([dbc.CardImg(src="/static/images/download_monkey.jpg", top=True)], width=1)   
    ])
        ])]
            
        ),
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
    Output(component_id='the_graph', component_property='figure'),
    [Input(component_id='my_dropdown', component_property='value')]
)

def update_graph(my_dropdown):
    dff = df
    if my_dropdown == 'Pie Chart':
        piechart=px.pie(
                        df, values=df.Count, names=df.Status, title='Quote Status'
                    ).update_layout(
                        template='plotly_dark',
                        plot_bgcolor= 'rgba(1, 4, 0, 2)',
                        paper_bgcolor= 'rgba(1, 4, 0, 2)',
                    )
        return (piechart)

    elif my_dropdown == 'Bar Chart':
        barchart=px.bar(
                        df, x=df.Count, y=df.Status, title='Quote Status'
                    ).update_layout(
                        template='plotly_dark',
                        plot_bgcolor= 'rgba(1, 4, 0, 2)',
                        paper_bgcolor= 'rgba(1, 4, 0, 2)',
                    )
        return (barchart)


if __name__ == '__main__':
    app.run_server(debug=True)