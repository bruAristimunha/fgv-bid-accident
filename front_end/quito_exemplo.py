import pandas as pd
import numpy as np
import dash                     #(version 1.0.0)
import dash_table
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

import plotly.offline as py     #(version 4.4.1)
import plotly.graph_objs as go
import plotly.express as px
import matplotlib.pylab as plt



def make_hour_map(waze_heatmap):
    fig, ax = plt.subplots(nrows=4, figsize=(15,7*3))
    year_array = [2018, 2019,2020,2021]
    for index_h, heat_year in enumerate(waze_heatmap):
        sns.heatmap(heat_year, ax=ax[index_h], cmap="coolwarm")
        ax[index_h].set_title(year_array[index_h])
        
    return fig
df_waze_alert = pd.read_csv("../data/alert_quito.csv")

print("Arquivo lido")

mapbox_access_token = 'pk.eyJ1IjoiYnJ1YXJpc3RpbXVuaGEiLCJhIjoiY2tvOWZjbmU5MDQ0bzJubW4zMGR5MWRhbSJ9.AaK9vUZtH_MFYYavYim76w'

app = dash.Dash(__name__)

blackbold={'color':'black', 'font-weight': 'bold'}

df_sub = df_waze_alert

#df_sub['color'] = df_sub['type'].apply(type2color)

df_sub_quito = df_sub[df_sub['city'] == 'Quito']
df_sample = df_sub_quito.sample(frac=0.5,random_state=42)

fig =px.scatter_mapbox(df_sample, lat="latitude", lon="longitude", hover_name="city", hover_data=["street"],
                        color_discrete_sequence=["fuchsia"], zoom=8, height=300)
fig.update_layout(mapbox_style="open-street-map")
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

df_waze_alert['weekday'] = [date.weekday() for date in pd.to_datetime(df_waze_alert['pub_utc_date'])]
accidents =  df_waze_alert[df_waze_alert['type'] == 'ACCIDENT']
accidents['hour_b'] = (accidents['hour'] - 5)%24

waze_heatmap = [accidents[accidents['year']== year].groupby(['weekday', 'hour_b',]).size().unstack() for year in [2018, 2019,2020,2021]]



app.layout = html.Div([
#---------------------------------------------------------------
    # Map
    html.Div([
        dcc.Graph(figure=fig)]    
        ),    
    html.Div([
        dcc.Graph(figure=make_hour_map(waze_heatmap))]
        ),
    
    ],            
    
)



df_sub = df_waze_alert

def type2color(t):
    if t == 'JAM':
        return '#ffff00'
    elif t == 'ROAD_CLOSED':
        return '#669999'
    elif t == 'WEATHERHAZARD':
        return '#0066ff'
    elif t == 'ACCIDENT':
        return '#ff0000'


# # Output of Graph
# @app.callback([Output('graph', 'figure')],
#               [Input('graph', 'figure')])
# def update_figure():


#     fig = 
#     fig.update_layout(mapbox_style="open-street-map")
#     fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
#     fig.show()


#     # Create figure
#     locations=[px.scatter_mapbox(df_sample, lat="latitude", lon="longitude", hover_name="city", hover_data=["street"],
#                             color_discrete_sequence=["fuchsia"], zoom=3, height=300)
#     )]

#     # Return figure
#     return {
#         'data': locations,
#         'layout': go.Layout(
#             uirevision= 'foo', #preserves state of figure/map after callback activated  
#             hovermode='closest',
#             hoverdistance=2,
#             title=dict(text="Acidentes em Quito",font=dict(size=50, color='green')),
#             mapbox=dict(
#                 accesstoken=mapbox_access_token,
#                 bearing=25,
#                 style='light',
#                 center=dict(
#                     lat=-0.1865938,
#                     lon=-78.5706264
#                 ),
#                 pitch=40,
#                 zoom=11.5
#             ),
#         )
#     }

if __name__ == '__main__':
    app.run_server(debug=False)