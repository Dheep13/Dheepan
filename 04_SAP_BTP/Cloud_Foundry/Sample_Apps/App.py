import dash
import dash_table
import pandas as pd
import os

df = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/solar.csv')

app = dash.Dash(__name__)
port_from_env = os.getenv('PORT')
port = int(port_from_env) if port_from_env is not None else 5050

app.layout = dash_table.DataTable(
    id='table',
    columns=[{"name": i, "id": i} for i in df.columns],
    data=df.to_dict('records'),
)

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=port, debug=True)