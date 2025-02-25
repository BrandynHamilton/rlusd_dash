import os
import pandas as pd

from plotly.subplots import make_subplots
import plotly.graph_objects as go
import plotly.express as px
import plotly.io as pio
import plotly.offline as pyo
import plotly.colors as pc

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import datetime as dt
from datetime import timedelta
import sys
import requests
from dotenv import load_dotenv

from web3 import Web3
from eth_abi import decode
from eth_utils import decode_hex, to_text
from web3.middleware import geth_poa_middleware
import time
import json
from dash import Dash, html, dcc, Input, Output, State, callback
from dash import dash_table

import requests
from dune_client.client import DuneClient

from data_processing import main

from IPython.display import Image, display

# Adjust external_stylesheets to include the prefix
external_stylesheets = [
    'https://cdnjs.cloudflare.com/ajax/libs/normalize/8.0.1/normalize.min.css', 
    '/rlusd_dash/assets/styles.css'  # Adjusted path for proxying
]

# Initialize placeholders for the figures
fig1 = fig3 = fig4 = fig6 = fig7 = go.Figure()

scheduler = BackgroundScheduler(daemon=True)

# Initialize Dash App with adjusted config for proxying
app = Dash(
    __name__,
    external_stylesheets=external_stylesheets,
    assets_url_path='/rlusd_dash/assets',
    routes_pathname_prefix='/rlusd_dash/',
    requests_pathname_prefix='/rlusd_dash/'
)

# Adjust Dash URL prefixes for proxying
# app.config.assets_url_path = '/rlusd_dash/assets'
# app.config.routes_pathname_prefix = '/rlusd_dash/'
# app.config.requests_pathname_prefix = '/rlusd_dash/'

def scheduled_main():
    print("Running scheduled main() function...")
    global fig1, fig3, fig4, fig6, fig7
    # Call main() and update the figures
    fig1, fig3, fig4, fig6, fig7 = main()
    print("Updated figures from scheduled main() call.")

scheduled_main()

# Schedule the job to run every Sunday at midnight (adjust to your preferred time)
scheduler.add_job(
    scheduled_main, 
    CronTrigger(day_of_week='sun', hour=0, minute=0, second=0)
)
scheduler.start()

app.layout = html.Div(style={'backgroundColor': 'var(--color-background)'}, children=[
    html.H1(
        children='RLUSD Dashboard',
        style={
            'textAlign': 'center',
            'color': 'var(--wcm-color-fg-1)',
            'fontSize': '36px',
            'fontWeight': 'bold',
            'marginBottom': '20px'
        }
    ),
    dcc.Interval(
        id='interval-component',
        interval=60*60*1000,  # Refresh every hour (adjust as needed)
        n_intervals=0
    ),
    html.Div(className='graph-container', children=[
        dcc.Graph(id='supply_by_chain')
    ]),
    html.Div(className='graph-container', children=[
        dcc.Graph(id='ETH_LP_TVL')
    ]),
    html.Div(className='graph-container', children=[
        dcc.Graph(id='supply_by_liquidity')
    ]),
    html.Div(className='graph-container', children=[
        dcc.Graph(id='supply_by_liquidity2')
    ]),
    html.Div(className='graph-container', children=[
        dcc.Graph(id='supply_by_liquidity3')
    ]),

    # Footer Section
    html.Footer(style={
        'backgroundColor': '#333',
        'color': '#fff',
        'textAlign': 'center',
        'padding': '20px',
        'marginTop': '40px'
    }, children=[
        html.P([
            "Contact: ",
            html.A("brandynham1120@gmail.com",
                   href="mailto:brandynham1120@gmail.com",
                   style={'color': '#fff'})
        ]),
        html.P([
            "Github/Documentation: ",
            html.A("https://github.com/BrandynHamilton/rlusd_dash",
                   href="https://github.com/BrandynHamilton/rlusd_dash",
                   style={'color': '#fff'})
        ]),
    ])
])

@app.callback(
    Output('supply_by_chain', 'figure'),
    Output('ETH_LP_TVL', 'figure'),
    Output('supply_by_liquidity', 'figure'),
    Output('supply_by_liquidity2', 'figure'),
    Output('supply_by_liquidity3', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_graphs(n):
    return (
        fig1.return_fig(),
        fig3.return_fig(),
        fig4.return_fig(),
        fig6.return_fig(),
        fig7.return_fig()
    )

if __name__ == '__main__':
    app.run_server(port=8050, debug=False)
