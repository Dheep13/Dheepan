import json
from dash import Dash, html, dcc, Input, Output, callback
import plotly.graph_objs as go
import pandas as pd

# Load the JSON data
with open('paste.txt', 'r') as file:
    data = json.load(file)

# Convert the data to a pandas DataFrame
df = pd.DataFrame(data['data'])

# Convert date strings to datetime objects
df['date'] = pd.to_datetime(df['date'])

# Sort the DataFrame by date
df = df.sort_values('date')

# Create the Dash app
app = Dash(__name__)

# Define the layout of the app
app.layout = html.Div([
    html.H1('Interactive Apple Stock Dashboard'),
    
    html.Div([
        html.Div([
            dcc.Dropdown(
                id='chart-type',
                options=[
                    {'label': 'Candlestick', 'value': 'candlestick'},
                    {'label': 'Line', 'value': 'line'},
                    {'label': 'OHLC', 'value': 'ohlc'}
                ],
                value='candlestick'
            )
        ], style={'width': '30%', 'display': 'inline-block'}),
        
        html.Div([
            dcc.Dropdown(
                id='y-axis',
                options=[
                    {'label': 'Price', 'value': 'price'},
                    {'label': 'Volume', 'value': 'volume'}
                ],
                value='price'
            )
        ], style={'width': '30%', 'float': 'right', 'display': 'inline-block'})
    ]),
    
    dcc.Graph(id='stock-chart'),
    
    dcc.RangeSlider(
        id='date-slider',
        min=0,
        max=len(df) - 1,
        value=[0, len(df) - 1],
        marks={i: df['date'].iloc[i].strftime('%Y-%m-%d') for i in range(0, len(df), len(df)//5)}
    )
])

@callback(
    Output('stock-chart', 'figure'),
    Input('chart-type', 'value'),
    Input('y-axis', 'value'),
    Input('date-slider', 'value')
)
def update_chart(chart_type, y_axis, date_range):
    dff = df.iloc[date_range[0]:date_range[1]+1]
    
    if y_axis == 'price':
        if chart_type == 'candlestick':
            fig = go.Figure(go.Candlestick(
                x=dff['date'],
                open=dff['open'],
                high=dff['high'],
                low=dff['low'],
                close=dff['close'],
                name='AAPL'
            ))
        elif chart_type == 'line':
            fig = go.Figure(go.Scatter(
                x=dff['date'],
                y=dff['close'],
                mode='lines',
                name='Close Price'
            ))
        elif chart_type == 'ohlc':
            fig = go.Figure(go.Ohlc(
                x=dff['date'],
                open=dff['open'],
                high=dff['high'],
                low=dff['low'],
                close=dff['close'],
                name='AAPL'
            ))
        fig.update_layout(yaxis_title='Price')
    else:  # y_axis == 'volume'
        fig = go.Figure(go.Bar(
            x=dff['date'],
            y=dff['volume'],
            name='Volume'
        ))
        fig.update_layout(yaxis_title='Volume')
    
    fig.update_layout(
        title='AAPL Stock Data',
        xaxis_title='Date',
        hovermode='x unified'
    )
    
    return fig

if __name__ == '__main__':
    app.run_server(debug=True)