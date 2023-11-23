#!/usr/bin/python3
import plotly.graph_objects as go
import pandas as pd
import os

cwd=os.getcwd()
datadir=os.path.join(cwd,'data/')
datafiles=os.listdir(datadir)
testfile=os.path.join(datadir,'mongodb-sanitario.csv')
geojson_mx=os.path.join(cwd,'mexico.geojson')

df = pd.read_csv(testfile)

fig = go.Figure()
fig.add_trace(go.Scattergeo(
                    lon = df['Longitud(V1)'],
                    lat = df['Latitud(V1)'],
                    text = df['Tweet'],
                    marker_color='red',
                    marker_size=8 ))
fig.update_layout(width = 1200, height=800,
        title_text = "Mapa de variante: Sanitario",
        title_x=0.5,
        showlegend = True,
        geo = dict(
            scope = 'north america',
            resolution = 50,
            lonaxis_range= [ -130, -83 ],
            lataxis_range= [15, 33],
            landcolor = 'rgb(217, 217, 217)',
        )
    )
fig.show()
