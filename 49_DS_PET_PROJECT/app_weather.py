import dash
from dash import html, dcc, Input, Output, callback
import plotly.graph_objs as go
import pandas as pd
import requests
# from datetime import datetime
from datetime import datetime, timedelta

# Initialize the Dash app
app = dash.Dash(__name__)

# Initialize an empty DataFrame to store our data
df = pd.DataFrame(columns=[
    'timestamp', 'temperature', 'feelslike', 'humidity', 'wind_speed',
    'pressure', 'precip', 'cloudcover', 'visibility', 'uv_index'
])

# Weatherstack API details
API_KEY = '0ff2e7a2ab6fd9344c43286f90a8cdc1'  # Replace with your actual API key
BASE_URL = 'http://api.weatherstack.com/current'
CITY = 'New Delhi'

def fetch_weather_data():
    params = {
        'access_key': API_KEY,
        'query': CITY
    }
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        data = response.json()
        current = data['current']
        location = data['location']
        return {
            'timestamp': datetime.strptime(location['localtime'], '%Y-%m-%d %H:%M'),
            'temperature': current['temperature'],
            'feelslike': current['feelslike'],
            'humidity': current['humidity'],
            'wind_speed': current['wind_speed'],
            'pressure': current['pressure'],
            'precip': current['precip'],
            'cloudcover': current['cloudcover'],
            'visibility': current['visibility'],
            'uv_index': current['uv_index'],
            'weather_descriptions': current['weather_descriptions'][0],
            'wind_dir': current['wind_dir']
        }
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

# Layout of the app
app.layout = html.Div([
    html.H1(f'Live Weather Dashboard - {CITY}'),
    html.Div([
        html.Div([
            html.H2('Current Weather'),
            html.Div(id='current-weather')
        ], style={'width': '30%', 'display': 'inline-block', 'vertical-align': 'top'}),
        html.Div([
            dcc.Graph(id='live-graph', animate=True)
        ], style={'width': '70%', 'display': 'inline-block'})
    ]),
    dcc.Interval(
        id='interval-component',
        interval=1*5*1000,  # in milliseconds (1 minute)
        n_intervals=0
    ),
    html.Div([
        html.Label('Select Time Window:'),
        dcc.Dropdown(
            id='time-window-dropdown',
            options=[
                {'label': 'Last 1 Minute', 'value': 1},
                {'label': 'Last Hour', 'value': 60},
                {'label': 'Last 6 Hours', 'value': 360},
                {'label': 'Last 24 Hours', 'value': 1440},
            ],
            value=60,
            style={'width': '200px'}
        )
    ])
])

@callback(
    Output('live-graph', 'figure'),
    Output('current-weather', 'children'),
    Input('interval-component', 'n_intervals'),
    Input('time-window-dropdown', 'value')
)
def update_graph_and_current(n, time_window):
    global df
    
    # Fetch new data
    new_data = fetch_weather_data()
    if new_data:
        # Create a new row as a DataFrame
        new_row = pd.DataFrame([new_data])
        
        # Concatenate the new row to the existing DataFrame
        df = pd.concat([df, new_row], ignore_index=True)
        
        # Keep only the data points within the selected time window
        cutoff_time = datetime.now() - timedelta(minutes=time_window)
        df = df[df['timestamp'] > cutoff_time]

        # Create the graph
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df['timestamp'], y=df['temperature'],
                        mode='lines+markers', name='Temperature (°C)'))
        fig.add_trace(go.Scatter(x=df['timestamp'], y=df['feelslike'],
                        mode='lines+markers', name='Feels Like (°C)'))
        fig.add_trace(go.Scatter(x=df['timestamp'], y=df['humidity'],
                        mode='lines+markers', name='Humidity (%)'))
        fig.add_trace(go.Scatter(x=df['timestamp'], y=df['wind_speed'],
                        mode='lines+markers', name='Wind Speed (km/h)'))

        fig.update_layout(
            title=f'Weather Trends - {CITY} (Last {time_window} minutes)',
            xaxis_title='Time',
            yaxis_title='Value',
            legend_title='Weather Parameters',
            xaxis=dict(
                range=[cutoff_time, datetime.now()],
                tickformat='%Y-%m-%d %H:%M'
            )
        )

        # Create current weather display
        current_weather = html.Div([
            html.P(f"Temperature: {new_data['temperature']}°C"),
            html.P(f"Feels Like: {new_data['feelslike']}°C"),
            html.P(f"Weather: {new_data['weather_descriptions']}"),
            html.P(f"Humidity: {new_data['humidity']}%"),
            html.P(f"Wind: {new_data['wind_speed']} km/h, {new_data['wind_dir']}"),
            html.P(f"Pressure: {new_data['pressure']} mb"),
            html.P(f"Precipitation: {new_data['precip']} mm"),
            html.P(f"Cloud Cover: {new_data['cloudcover']}%"),
            html.P(f"Visibility: {new_data['visibility']} km"),
            html.P(f"UV Index: {new_data['uv_index']}")
        ])

        return fig, current_weather
    
    # If there's an error fetching data, return the last known figure and current weather
    return dash.no_update, dash.no_update

if __name__ == '__main__':
    app.run_server(debug=True)