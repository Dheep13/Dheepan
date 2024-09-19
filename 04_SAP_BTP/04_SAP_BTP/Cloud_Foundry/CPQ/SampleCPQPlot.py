import pandas as pd
import requests
import json
from collections import Counter
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import os
import dash_bootstrap_components as dbc

cf_port = os.getenv("PORT")

#base_url = "https://sandbox.webcomcpq.com/setup/api/v1/"
#r = requests.get(base_url , auth=(os.environ.get('CPQ_USER'),os.environ.get('CPQ_PASS')))
#Get data from custom table
base_url = "https://sandbox.webcomcpq.com/api/custom-table/v1/customTables/Quote_Status/entries"

my_headers = {'Authorization' : 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE2MzA2MjE5NzksInVzZXJuYW1lIjoiZGVlcGFuLnMiLCJkb21haW4iOiJQUk9GU0VSSU5ESUEifQ.RRIMyD4sxgZJsQFgwAUFvQn0dhArMkI6_XqRw5oQNLw'}
response = requests.get(base_url, headers=my_headers)
data = json.loads(response.text)
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
#print(df)

trace = go.Bar(x=df.Status, y=df.Count, name='Status')
app = dash.Dash()

app.layout = html.Div(children=[
    html.H1(children='Quote Status Report'),
    #html.Div(children='''National Sales Funnel Report.'''),
    dcc.Graph(
        id='example-graph',
        figure={
            'data': [trace],
            'layout':
            go.Layout(title='Count of quotes by status', barmode='stack', height=300, width=400)
        })
])

if __name__ == '__main__':
	if cf_port is None:
		app.run_server(host='0.0.0.0', port=4000, debug=True)
	else:
		app.run_server(host='0.0.0.0', port=int(cf_port), debug=True)