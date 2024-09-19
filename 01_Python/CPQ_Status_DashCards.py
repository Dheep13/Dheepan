import plotly.express as px
from jupyter_dash import JupyterDash
from collections import Counter
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
import requests
import json
from skimage import data

img = data.chelsea()
fig = px.imshow(img)
fig.update_layout(dragmode="drawrect")
config = {
    "modeBarButtonsToAdd": [
        "drawline",
        "drawopenpath",
        "drawclosedpath",
        "drawcircle",
        "drawrect",
        "eraseshape",
    ]
}

#path = "C:\\Users\\I520292\\OneDrive - SAP SE\\Visual Studio Code\\01_Python\\download_monkey.jpg"
# Iris bar figure
def drawBarFigure():
    return  html.Div([
        dbc.Card(
            dbc.CardBody([
                dcc.Graph(
                    figure=px.bar(
                        df, x=df.Status, y=df.Count
                    ).update_layout(
                        template='plotly_dark',
                        plot_bgcolor= 'rgba(0, 0, 0, 0)',
                        paper_bgcolor= 'rgba(0, 0, 0, 0)',
                    ),
                    config={
                        'displayModeBar': False
                    }
                ) 
            ])
        ),  
    ])

# image to be embedded in Dasshboard
# image_filename = 'download_monkey.jpg' # replace with your own image
# encoded_image = base64.b64encode(open(image_filename, 'rb').read())

# Iris bar figure
def drawPieFigure():
    return  html.Div([
        dbc.Card(
            dbc.CardBody([
                dcc.Graph(
                    figure=px.pie(
                        df, values=df.Count, names=df.Status
                    ).update_layout(
                        template='plotly_dark',
                        plot_bgcolor= 'rgba(0, 0, 0, 0)',
                        paper_bgcolor= 'rgba(0, 0, 0, 0)',
                    ),
                    config={
                        'displayModeBar': False
                    }
                ) 
            ])
        ),  
    ])

# Text field
def drawText():
    return html.Div([
        dbc.Card(
            dbc.CardBody([
                html.Div([
                    html.H2("Quote Status Count"),
                ], style={'textAlign': 'center'}) 
            ])
        ),
    ])

# Data
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

# Build App
app = JupyterDash(external_stylesheets=[dbc.themes.SLATE])

app.layout = html.Div([
    dbc.Card(
        dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    drawText()
                ], width=3),
                dbc.Col([
                    drawText()
                ], width=3),
                dbc.Col([
                    drawText()
                ], width=3),
                dbc.Col([
                    drawText()
                ], width=3),
            ], align='center'), 
            html.Br(),
            dbc.Row([
                dbc.Col([
                    drawBarFigure() 
                ], width=3),
                dbc.Col([
                    drawBarFigure()
                ], width=3),
                dbc.Col([
                    drawBarFigure() 
                ], width=6),
            ], align='center'), 
            html.Br(),
            dbc.Row([
                dbc.Col([
                    drawBarFigure()
                ], width=9),
                dbc.Col([
                    drawBarFigure()
                ], width=3),
            ], align='center'), 
            dcc.Graph(figure=fig, config=config), 
            dbc.Row([
                dbc.Col([
                    drawPieFigure()
                ], width=9),
                dbc.Col([
                    drawPieFigure()
                ], width=3),
            ], align='center')    
        ]), color = 'light'
    )
])

# Run app and display result inline in the notebook
app.run_server(mode='external')