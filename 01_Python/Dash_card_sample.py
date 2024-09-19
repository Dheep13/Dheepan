import dash_bootstrap_components as dbc
import dash_html_components as html
from jupyter_dash import JupyterDash
app = JupyterDash(external_stylesheets=[dbc.themes.SLATE])

card = dbc.Card(
    [
        dbc.CardHeader("Marketing"),
        dbc.CardBody(
            [
                html.H4("201 new Leads", className="card-title"),
                html.P("Delivered this week compared...", className="card-text"),
            ]
        ),
    ],
    style={"width": "20rem"},
)

app.layout = html.Div([card])

if __name__ == '__main__':
    app.run_server(debug=True)
