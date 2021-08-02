#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May  3 16:52:23 2021
 
@author: diluisi e bruAristimuha
"""

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Output, Input
import plotly.express as px
import dash_bootstrap_components as dbc
import pandas as pd
import numpy as np
import geopandas as gpd
import json
from shapely.geometry import shape
from plotly import graph_objs as go
from plotly.graph_objs import *
from pandas import to_datetime, read_csv
from flask_caching import Cache

import datetime as dt
import dash_daq as daq
import seaborn as sns

from matplotlib.colors import to_hex
pd.options.plotting.backend = "plotly"

# https://www.bootstrapcdn.com/bootswatch/
# https://www.bootstrapcdn.com/bootswatch/
# https://codepen.io/chriddyp/pen/bWLwgP - css
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP],
                meta_tags=[{'name': 'viewport',
                            'content': 'width=device-width, initial-scale=1'}]
                )

server = app.server

# cache = Cache(app.server, config={
#     'CACHE_TYPE': 'filesystem',
#     'CACHE_DIR': 'cache'
# })
# app.config.supress_callback_exceptions = True

timeout = 300



##############################################
# PRE-PROCESSING FUNCTION
##############################################
def create_weekday(df, diff_time):
    """
    
    """
    
    datetime = to_datetime(df['pub_utc_date'])

    
    
    datetime = datetime - pd.Timedelta(hours=diff_time)

    weekday = [date.weekday() 
               for date in datetime]
    
    hour = [date.hour 
            for date in datetime]
    
    return weekday, hour
    
def feature_extract(dataframe, timezone):
    """
    
    """
    
    weekday, hour = create_weekday(dataframe, timezone)
    
    dataframe['weekday'] = weekday
    dataframe['hour'] = hour
    
    return dataframe




# ------------------------------------------------------------------------------
# LEITURA DO ARQUIVO
# ------------------------------------------------------------------------------
# Arquivo > objeto dataframe


def weeakday_hour(dataframe):
    
    return dataframe.groupby(["weekday", 
                              "hour"]).size().unstack()    

dataframes = [read_csv("../data/process/alert_quito.csv"), read_csv("../data/process/alert_saopaulo.csv"), read_csv("../data/process/alert_lima.csv"), read_csv("../data/process/alert_montevideo.csv"), read_csv("../data/process/alert_xalapa.csv")]


option_year = [{"label": "2018", "value": 2018}, {"label": "2019", "value": 2019},
               {"label": "2020", "value": 2020}, {"label": "2021", "value": 2021}]

option_month =[{"label": "Enero", "value": 1}, {"label": "Febrero", "value": 2}, {"label": "Marzo", "value": 3}, 
               {"label": "Abril", "value": 4}, {"label": "Mayo", "value": 5}, {"label": "Junio", "value": 6},
               {"label": "Julio", "value": 7}, {"label": "Agosto", "value": 8}, {"label": "Septiembre", "value": 9}, 
               {"label": "Octubre", "value": 10}, {"label": "Noviembre", "value": 11}, {"label": "Diciembre", "value": 12}]

option_city = [{"label": "São Paulo", "value": "SãoPaulo"}, {"label": "Quito", "value": "Quito"}, 
               {"label": "Lima (MiraFlores)", "value": "Lima"}, {"label": "Montevideo", "value": "Montevideo"}, 
               {"label": "Xalapa", "value": "Xalapa"}, ]

option_week = [{"label": "Día laboral", "value": 1}, {"label": "Fin de semana", "value": 2}]

option_hour = [{"label": "0h-24h", "value":  0}, {"label": "22h-6h", "value":  1}, 
               {"label": "6h-14h", "value":  2}, {"label": "14h-22h", "value": 3}]

# ------------------------------------------------------------------------------
# LAYOUT
# ------------------------------------------------------------------------------

# Layout of Dash App
app.layout = html.Div(
    style={'textAlign': 'center', 'padding': '0px 0px 15px 15px',
           'font-family': 'roboto-light', 'vertical-align': 'middle'},
    children=[
        html.Div(
            className="row",
            style={'padding': '10px 5px 5px 5px'},
            children=[
                # Column for user controls
                html.Div(
                    className="four columns div-user-controls",
                    style={"position": "fixed",
                           'display': 'block',
                           'left': '25px'},

                    children=[
                        html.A(
                            html.Img(
                                src="assets/logo.png",
                                style={"float": "center",
                                       "height": "50px"},
                            ),
                            href="https://smartcities-bigdata.fgv.br/",
                        ),                

                        html.P(
                           "ACCIDENTES DE TRÁFICO",
                            style={"font-size": "30px"},
                        ),

                        html.Pre(),                        
                        
                        html.P(
                            """Seleccione la ciudad:""",
                        ),
                        html.Div(
                            className="div-for-dropdown",
                            children=[
                                # Dropdown for locations on map
                                dcc.Dropdown(
                                    id="city",
                                    options=option_city,
                                    placeholder="Xalapa",
                                    value="Xalapa",
                                    style={"width": "25rem"},
                                    searchable=False,
                                )
                            ],
                        ),
                        html.Pre(),
                        
                        html.P(
                            """Procesamiento en tiempo real:""",
                        ),
                        
                        html.Div(
                            className="div-for-dropdown",
                            children=[
                            daq.BooleanSwitch(
                              id='real-time',
                              on=False,
                                  color="#023A78",
                            ),
                            ]),
                        
                        html.Pre(),

                        html.P(
                            """Seleccione el año:""",
                        ),
                        html.Div(
                            id='div-year',
                            className="div-for-dropdown",
                            children=[
                                # Dropdown for locations on map
                                dcc.Dropdown(
                                    id="year",
                                    options=option_year,
                                    placeholder="",
                                    value=0,
                                    style={"width": "25rem"},
                                    searchable=False,
                                )
                            ],
                        ),
                        html.Pre(),


                        html.P(
                            """Seleccione el mese:"""
                        ),
                        
                        ########## TO-DO Multi-Value Dropdown
                        html.Div(
                            id='div-month',
                            className="div-for-dropdown",
                            children=[
                                # Dropdown for locations on map
                                dcc.Dropdown(
                                    id="month",
                                    options=option_month,
                                    placeholder="",
                                    value=0,
                                    style={"width": "25rem"},
                                    searchable=False,
                                )
                            ],
                        ),

                        
                        html.Pre(),


                        html.P(
                            """Seleccione el día de la semana:"""
                        ),
                        html.Div(
                            id='div-weekday',
                            className="div-for-dropdown",
                            children=[
                                # Dropdown for locations on map
                                dcc.Dropdown(
                                    id="weekday",
                                    options=option_week,
                                    placeholder="",
                                    value=0,
                                    style={"width": "25rem"},
                                    searchable=False,
                                )
                            ],
                        ),
                        html.Pre(),
                        html.P(
                            """Seleccione el tiempo:"""
                        ),
                        html.Div(
                            id="div-hour",
                            className="div-for-dropdown",
                            children=[
                                # Dropdown for locations on map
                                dcc.Dropdown(
                                    id="hour",
                                    options=option_hour,
                                    placeholder="",
                                    value=0,
                                    style={"width": "25rem"},
                                    searchable=False,
                                )
                            ],
                        ),                        
                                             
                        
                        html.Pre(),
                        html.Hr(),
                        html.Pre(),
                        html.H4(
                            """Accidentes reportados a Waze:""",
                        ),
                        
                        html.H4(
                            "",
                            id="count-waze"
                        ),
                        html.Pre(),
                        html.Pre(),
                        html.Pre(),
                        
                        #                         html.Pre(),
#                         html.H4(
#                             """Recuento de Prefeitura:""",
#                         ),
#                         html.H4(
#                             """0""",
#                             id="count-prefeitura"
#                         ),

#                         html.Pre(),
                        html.Hr(),
                    ],
                ),
                # Column for app graphs and plots
                html.Div(
                    className="",
                    style={"width": "70rem",  'position': 'absolute', 'left': '500px'
                           },
                    children=[
                        dcc.Graph(id="map-graph"),
                        html.Pre(),
                        dcc.Graph(id="histogram-hour"),
                        dcc.Graph(id="data-waze"),
                        #dcc.Graph(id="data-official"),
                        dcc.Store(id='intermediate-value')
                    ],
                ),
            ],
        ),
    ],
)

# ------------------------------------------------------------------------------
# CALL BACKS
# ------------------------------------------------------------------------------

@app.callback(
    Output('intermediate-value', 'data'),
    [Input('city', 'value'),
     Input('year', 'value'),
     Input('month', 'value'),
     Input('weekday', 'value'), 
     Input('hour', 'value')]
)
def load_database(city, year, month, weekday, hour):
    
    
    print(city, year, month, weekday, hour)
    
    dataframe = select_city(city)    
    dataframe = select_year(year, dataframe)    
    dataframe = select_month(month, dataframe)
    dataframe = select_weekday(weekday, dataframe)
    dataframe = select_hour(hour, dataframe)
    
    
    #print(dataframe)
    return dataframe.to_dict('records')

@app.callback(
    [Output('div-year', 'children'),
     Output('div-hour', 'children'),
     Output('div-weekday', 'children'),
     Output('div-month', 'children')],
    [Input('real-time', 'on')]
)
def update_realtime(real):
    if real:
          return  [dcc.Dropdown(id="year", placeholder="", 
                                value=0, style={"width": "25rem"}, disabled=True),
                   dcc.Dropdown(id="hour", placeholder="", 
                                value=0, style={"width": "25rem"}, disabled=True),
                  dcc.Dropdown(id="weekday", placeholder="", 
                               value=0, style={"width": "25rem"}, disabled=True),
                  dcc.Dropdown(id="month", placeholder="", 
                               value=0, style={"width": "25rem"}, disabled=True),
                  ]
    else:
        
        return  [dcc.Dropdown(id="year", placeholder="", 
                          value=0, style={"width": "25rem"}, searchable=False,
                        options=option_year),
                 dcc.Dropdown(id="hour", placeholder="", 
                          value=0, style={"width": "25rem"}, searchable=False,
                             options=option_hour),
                 dcc.Dropdown(id="weekday", placeholder="", 
                         value=0, style={"width": "25rem"}, searchable=False,
                             options=option_week),
                 dcc.Dropdown(id="month", placeholder="", 
                         value=0, style={"width": "25rem"}, searchable=False,
                             options=option_month),
                 
              ]
        
######################################################################

### Select city
#@cache.memoize(timeout=timeout) 
def select_city(city):
    if city == 'Quito':
        dataframe = dataframes[0]
    elif city =='SãoPaulo':
        dataframe = dataframes[1]    
    elif city == 'Lima':
        dataframe = dataframes[2]
    elif city == 'Montevideo':
        dataframe = dataframes[3]
    elif city == 'Xalapa':
        dataframe = dataframes[4]
    return dataframe

### Select year
def select_year(year, dataframe):
    if year == 0:
        return dataframe
    else:
        return dataframe[dataframe['year'] == year]
    
#### Select month
def select_month(month, dataframe):
    if month == 0:
        return dataframe
    else:
        return dataframe[dataframe['month'] == month]

#### Select weekday
def select_weekday(weekday, dataframe):
    if (weekday == 0):
        return dataframe
    elif (weekday == 1):
        range_weekday = [0, 1, 2, 3, 4, 5]
    elif (weekday == 2):
        range_weekday = [6, 7]

    return dataframe[dataframe['weekday'].isin(range_weekday)]

#### Select hour
def select_hour(hour, dataframe):
    if hour == "":
        return dataframe
    elif hour == 0:
        range_time = list(range(0,24))
    elif hour == 1:
        range_time = list(range(0,6)) + list(range(22,24))
    elif hour == 2:
        range_time = list(range(6,14))
    elif hour == 3:
        range_time = list(range(14,22))

    return dataframe[dataframe['hour'].isin(range_time)]

######################################################################
@app.callback(
    Output('map-graph', 'figure'),
    Input('intermediate-value', "data")
)
def update_map(dataframe):
    print("Entrou no mapa!!!!")
    df_map = pd.DataFrame(dataframe)
    
    fig = px.density_mapbox(df_map, lat="latitude", lon="longitude", 
                            hover_data=["street", "hour"], 
                            zoom=12, animation_frame='month', 
                            animation_group='month',
                            category_orders={'month': [1,2,3,4,5,6,7,8,9,10,11,12]},                               
                            color_continuous_scale='OrRd', opacity=1, radius=15)
    
    
    fig.update_layout(mapbox_style="carto-positron",
                      margin={"r": 0, "t": 0, "l": 0, "b": 0},
                      height=550,
                      )
    
    
    fig.update_layout(
        hoverlabel=dict(
            font_size=25,
            font_family="monospace"
        )
    )
    fig.update_traces(showlegend=False)
    fig.update_xaxes(automargin=True)
    fig.update(layout_showlegend=False)
    fig.update_layout(coloraxis_showscale=False)
    
    return fig
##########################################################################


@app.callback(
    Output('histogram-hour', 'figure'),
    [Input('intermediate-value', "data")]
)
def update_histogram(dataframe):
    print("Entrou Histograma!!!!")
    string_title = "Concentración de accidentes por hora del día y día de la semana."

    fig = px.imshow(weeakday_hour(pd.DataFrame(dataframe)), color_continuous_scale='OrRd')
    
    fig.update_layout({'plot_bgcolor': 'rgba(0, 0, 0, 0)',
                       'paper_bgcolor': 'rgba(0, 0, 0, 0)', },
                        yaxis_title="", margin={"r": 0}, title=string_title)
    fig.update_yaxes(showgrid=True, gridwidth=15, gridcolor='rgb(255,255,255)')

    fig.update_layout(coloraxis_showscale=False)
    
#     if range_time == 1:

#         fig.add_vrect(
#                 x0=5.5, x1=21.5, opacity=1, 
#             fillcolor='gray', line_width=0)
        
#     elif range_time == 2:
#         fig.add_vrect(
#             x0=-0.5, x1=5.5, opacity=1, 
#             fillcolor='gray', line_width=0)
    
#         fig.add_vrect(
#             x0=13.5, x1=23.5, opacity=1, 
#             fillcolor='gray', line_width=0)
    
#     elif range_time == 3:
#         fig.add_vrect(
#             x0=-0.5, x1=13.5, opacity=1, 
#             fillcolor='gray', line_width=0)

#         fig.add_vrect(
#             x0=21.5, x1=23.5, opacity=1, 
#             fillcolor='gray', line_width=0)
        
    return fig



# @app.callback(
#     Output('data-official', 'figure'),
#     [Input("year", "value")]
# )
# def update_date(year, range_time):
#     print(range_time)
#     fig_01 = df_count_quito.set_index("MES")['{}'.format(year)].T.plot.bar(
#         labels=dict(index="Meses del año.", value="Número de accidentes por mes."))
#     fig_01.update_layout(
#         title='Accidentes contabilizados por el ayuntamiento.')
#     fig_01.update(layout_showlegend=False)
#     fig_01.update_traces(marker_color='rgb(255, 148, 120)')
#     fig_01.update_layout(margin={"r": 0})
#     print("foi b")
#     return fig_01


@app.callback(
    Output('data-waze', 'figure'),
    [Input('intermediate-value', "data")]
)
def update_bar_waze(dataframe):
   
    fig_02 =  pd.DataFrame(dataframe)['month'].value_counts().sort_index().plot.bar(labels=dict(year="Meses del año.",
                                                         value="Número de accidentes reportados por usuarios de mes."))
    fig_02.update_layout(
        barmode='group', title='Accidentes reportados por usuarios de Waze.',
        xaxis_title='MES')

    fig_02.update(layout_showlegend=False)

    fig_02.update_traces(marker_color='rgb(255, 148, 120)')
    fig_02.update_layout(margin={"r": 0})

    print("foi c")
    return fig_02




@app.callback(
    [Output("count-waze", "children")],
    [Input('intermediate-value', "data")]
)
def update_count_waze(dataframe):

    count_value = pd.DataFrame(dataframe).size
    
    return html.H3(
             "{:.0f}".format(count_value),
              id="count-waze"
          ),




if __name__ == '__main__':
    app.run_server(debug=True, port=8000)