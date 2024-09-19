import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import pandas as pd


suicide = pd.read_csv('suicide rates.csv')
list_country = list(suicide['country'].unique())

app = dash.Dash(__name__, meta_tags=[{"name": "viewport", "content": "width=device-width"}])

app.layout = html.Div([

    html.Div([
        html.Div([
            html.Div([
                html.H3('World Suicides Data', style = {'margin-bottom': '5px', 'color': 'white'}),
            ])
        ], className = "create_container1 four columns", id = "title"),

    ], id = "header", className = "row flex-display", style = {"margin-bottom": "10px"}),


    html.Div([
        html.Div([
            html.P('Select Country', className = 'fix_label', style = {'color': 'white'}),
            dcc.Dropdown(id = 'select_country',
                         multi = False,
                         clearable = True,
                         disabled = False,
                         style = {'display': True},
                         value = 'Russian Federation',
                         placeholder = 'Select Countries',
                         options = [{'label': c, 'value': c}
                                    for c in list_country], className = 'dcc_compon'),

            html.P('Select Year', className = 'fix_label', style = {'color': 'white', 'margin-top': '30px'}),
            dcc.Slider(id = 'slider_year',
                       included = True,
                       updatemode='drag',
                       tooltip={'always_visible': True},
                       min = 1985,
                       max = 2016,
                       step = 1,
                       value = 2000,
                       marks = {str(yr): str(yr) for yr in range(1985, 2016, 5)},
                       className = 'dcc_compon'),

            html.P('Select Platform', className = 'fix_label', style = {'color': 'white', 'margin-top': '30px'}),
            dcc.Checklist(id = 'radio_items',
                          options = [{'label': d, 'value': d} for d in sorted(suicide['age'].unique())],
                          value=['35-54 years'],
                          style = {'color': 'white'}, className = 'dcc_compon'),

            ], className = "create_container2 four columns"),


        html.Div([
            dcc.Graph(id = 'bubble_chart',
                      config = {'displayModeBar': 'hover'}),

        ], className = "create_container2 eight columns"),

    ], className = "row flex-display"),

], id= "mainContainer", style={"display": "flex", "flex-direction": "column"})


@app.callback(Output('bubble_chart', 'figure'),
              [Input('select_country', 'value')],
              [Input('slider_year', 'value')],
              [Input('radio_items', 'value')])
def update_graph(select_country, slider_year, radio_items):
    suicide1 = suicide.groupby(['age', 'country', 'sex', 'year', 'population', 'suicides/100k pop'])['suicides_no'].sum().reset_index()
    suicide2 = suicide1[(suicide1['country'] == select_country) & (suicide1['year'] >= slider_year) & (suicide1['age'].isin(radio_items))]

    return {
        'data':[go.Scatter(
                    x=suicide2['year'],
                    y=suicide2['suicides_no'],
                    text = suicide2['sex'],
                    textposition = 'top center',
                    mode = 'markers + text',
                    marker = dict(
                        size = suicide2['suicides_no'] / 250,
                        color = suicide2['suicides_no'],
                        colorscale = 'HSV',
                        showscale = False,
                        line = dict(
                            color = 'MediumPurple',
                            width = 2
                        )),
                    hoverinfo='text',
                    hovertext=
                    '<b>Country</b>: ' + suicide2['country'].astype(str) + '<br>' +
                    '<b>Age</b>: ' + suicide2['age'].astype(str) + '<br>' +
                    '<b>Sex</b>: ' + suicide2['sex'].astype(str) + '<br>' +
                    '<b>Year</b>: ' + suicide2['year'].astype(str) + '<br>' +
                    '<b>Population</b>: ' + [f'{x:,.0f}' for x in suicide2['population']] + '<br>' +
                    '<b>Suicides/100k pop</b>: ' + [f'{x:,.0f}' for x in suicide2['suicides/100k pop']] + '<br>' +
                    '<b>Suicides</b>: ' + [f'{x:,.0f}' for x in suicide2['suicides_no']] + '<br>'


              )],


        'layout': go.Layout(
             plot_bgcolor='#010915',
             paper_bgcolor='#010915',
             title={
                'text': 'Suicides Data: ' + (select_country),

                'y': 0.96,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
             titlefont={
                        'color': 'white',
                        'size': 20},

             hovermode='x',
             xaxis=dict(title='<b>Year</b>',
                        tick0=0,
                        dtick=1,
                        color='white',
                        showline=True,
                        showgrid=False,
                        linecolor='white',
                        linewidth=1,


                ),

             yaxis=dict(title='<b>Suicides</b>',
                        color='white',
                        showline=False,
                        showgrid=True,
                        linecolor='white',

                ),

            legend = {
                'orientation': 'h',
                'bgcolor': '#010915',
                'x': 0.5,
                'y': 1.25,
                'xanchor': 'center',
                'yanchor': 'top'},
            font = dict(
                family = "sans-serif",
                size = 12,
                color = 'white',


                 )
        )

    }


if __name__ == '__main__':
    app.run_server(debug=True)