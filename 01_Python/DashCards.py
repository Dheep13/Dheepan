import dash_bootstrap_components as dbc
import dash_html_components as html
import pandas as pd
import dash
from jupyter_dash import JupyterDash


app = JupyterDash(external_stylesheets=[dbc.themes.SLATE])

app.layout = html.Div([dbc.Card(
    [
        dbc.CardImg(src="/static/images/download_monkey.jpg", top=True),
        dbc.CardBody(
            [
                html.H4("Card title", className="card-title"),
                html.P(
                    "Some quick example text to build on the card title and "
                    "make up the bulk of the card's content.",
                    className="card-text", color="primary"
                ),
                dbc.Button("Go somewhere", color="info"),
            ]
        ),
    ],
    style={"width": "18rem"},
)])
 

if __name__ == '__main__':
    app.run_server(debug=True)