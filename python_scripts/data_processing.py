import os
import pandas as pd

from plotly.subplots import make_subplots
import plotly.graph_objects as go
import plotly.express as px
import plotly.io as pio
import plotly.offline as pyo
import plotly.colors as pc

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

from diskcache import Cache
from pathlib import Path

import requests
from dune_client.client import DuneClient

from chart_builder.scripts.visualization_pipeline import visualization_pipeline
from chart_builder.scripts.utils import data_processing,create_df,open_json, main as chartBuilder

# Set the default template
from sql_queries.sql_scripts import lp_data
from python_scripts.utils import (flipside_api_results, call_api, prepare_data_for_simulation, dune_api_results)

load_dotenv()

#Connecting to proprietary data collection database

base_cache_dir = Path(__file__).resolve().parent
print(f'Base Directory: {base_cache_dir}')
cache = Cache(os.path.join(base_cache_dir, 'data_collection')) 

#Environment Variables
RLUSD_ETHEREUM = os.getenv('RLUSD_ETHEREUM')
RLUSD_XRP = os.getenv('RLUSD_XRP')
DUNE_KEY = os.getenv('DUNE_API_KEY')
FLIPSIDE_KEY = os.getenv('FLIPSIDE_API_KEY')
ETHEREUM_GATEWAY = os.getenv('ETHEREUM_GATEWAY')

dune = DuneClient(DUNE_KEY)
w3 = Web3(Web3.HTTPProvider(ETHEREUM_GATEWAY))

erc20_abi_path = 'abi/erc20_abi.json'

abi_paths = [erc20_abi_path]
abis = {}

for path in abi_paths:
    with open(path, "r") as file:
        abis[path] = json.load(file)

def main():

    today_utc = dt.datetime.now(dt.timezone.utc) 
    formatted_today_utc = today_utc.strftime('%Y-%m-%d %H:00:00')

    cached_timeseries = cache.get('timeseries',pd.DataFrame())
    weekly_data = cache.get('weekly_timeseries',pd.DataFrame())

    if cached_timeseries.empty:
        print(f'no cached data to process')

    rlusd_ETH_supply = cached_timeseries['RLUSD_ETH_Supply'].iloc[-1]
    rlusd_XRP_supply = cached_timeseries['RLUSD_XRPL_Supply'].iloc[-1]
    rlusd_in_xrp_lp = cached_timeseries['rlusd_bal'].iloc[-1]

    print(f'cached_timeseries: {cached_timeseries}')

    supply_dict = {
        "Blockchain":["Ethereum","XRP"],
        "Supply":[rlusd_ETH_supply,rlusd_XRP_supply]
    }

    supply_df = pd.DataFrame(supply_dict)

    total_rlusd_supply = supply_df['Supply'].sum()

    eth_rlusd_bal_timeseries = eth_rlusd_pool[eth_rlusd_pool['symbol']=='RLUSD']

    rlusd_in_eth_lp = eth_rlusd_pool[eth_rlusd_pool['symbol']=='RLUSD'].iloc[0][['current_bal']].values[0]

    total_rlusd_in_lp = rlusd_in_eth_lp + rlusd_in_xrp_lp

    percent_rlusd_in_lp = (total_rlusd_in_lp / total_rlusd_supply)*100

    supply_comp = {
        "Status":["In-Liquidity","Out-Liquidity"],
        "Amount":[total_rlusd_in_lp, (total_rlusd_supply-total_rlusd_in_lp)]
    }

    supply_comp_df = pd.DataFrame(supply_comp)

    xrpl_lp_timeseries = cache.get('timeseries')

    xrpl_lp_timeseries['dt'] = pd.to_datetime(xrpl_lp_timeseries['dt'])
    xrpl_lp_timeseries['dt'] = pd.to_datetime(xrpl_lp_timeseries['dt'].dt.strftime('%Y-%m-%d %H:00:00'))

    xrpl_lp_timeseries.set_index('dt',inplace=True)

    rlusd_xrp_df = xrpl_lp_timeseries[['rlusd_bal']]
    rlusd_xrp_df['blockchain'] = 'XRP Ledger'
    rlusd_xrp_df.rename(columns={"rlusd_bal":"current_bal"},inplace=True)

    eth_rlusd_bal_timeseries.index = eth_rlusd_bal_timeseries.index.strftime('%Y-%m-%d %H:00:00')

    rlusd_eth_df = eth_rlusd_bal_timeseries[['current_bal']]
    rlusd_eth_df['blockchain'] = 'Ethereum'

    rlusd_eth_df.index = pd.to_datetime(rlusd_eth_df.index)

    merged_df = pd.merge(
        rlusd_eth_df[['current_bal']].rename(columns={"current_bal":"RLUSD_ETH_LP"}),
        rlusd_xrp_df[['current_bal']].rename(columns={"current_bal":"RLUSD_XRP_LP"}),
        left_index=True,
        right_index=True,
        how='right'
    ).ffill()

    rlusd_eth_df = prepare_data_for_simulation(rlusd_eth_df,rlusd_xrp_df.index.min(),rlusd_xrp_df.index.max())

    rlusd_xrp_df.ffill(inplace=True)
    daily_eth_lp = rlusd_eth_df.resample('D').last()
    daily_xrpl_lp = rlusd_xrp_df.resample('D').last()

    combined_rlusd_lp = pd.concat([daily_eth_lp,daily_xrpl_lp])
    combined_rlusd_lp.index = pd.to_datetime(combined_rlusd_lp.index)
    combined_rlusd_lp.sort_index(inplace=True)

    combined_vol.sort_index(inplace=True)
    combined_vol_total = combined_vol.groupby(combined_vol.index)[['volume']].sum()
    vol_by_chain = combined_vol.groupby('blockchain')[['volume']].sum().reset_index()

    fig1, fig3, fig4, fig6, fig7 = create_charts()

    return fig1, fig3, fig4, fig6, fig7

def clean_dataset_values(rl_usd_xrp_pool_data_og): 

    rl_usd_xrp_pool_data = rl_usd_xrp_pool_data_og.copy()

    rl_usd_xrp_pool_data["amount2_value"] = rl_usd_xrp_pool_data["amount2"].apply(lambda x: float(x["value"]) if isinstance(x, dict) and "value" in x else None)

    rl_usd_xrp_pool_data["token1"] = rl_usd_xrp_pool_data["amount"].apply(lambda x: x["currency"] if isinstance(x, dict) else "XRP")
    rl_usd_xrp_pool_data["amount1"] = rl_usd_xrp_pool_data["amount"].apply(lambda x: float(x["value"]) if isinstance(x, dict) and "value" in x else pd.to_numeric(x, errors="coerce"))

    # Extract currency from 'amount2'
    rl_usd_xrp_pool_data["token2"] = rl_usd_xrp_pool_data["amount2"].apply(lambda x: x["currency"] if isinstance(x, dict) else "XRP")
    rl_usd_xrp_pool_data["amount2"] = rl_usd_xrp_pool_data["amount2"].apply(lambda x: float(x["value"]) if isinstance(x, dict) and "value" in x else pd.to_numeric(x, errors="coerce"))

    rl_usd_xrp_pool_data["token1"] = rl_usd_xrp_pool_data["token1"].replace("524C555344000000000000000000000000000000", "RLUSD")
    rl_usd_xrp_pool_data["token2"] = rl_usd_xrp_pool_data["token2"].replace("524C555344000000000000000000000000000000", "RLUSD")

    xpr_rlusd_pool = rl_usd_xrp_pool_data[
        ((rl_usd_xrp_pool_data["token1"] == "XRP") | (rl_usd_xrp_pool_data["token1"] == "RLUSD")) &
        ((rl_usd_xrp_pool_data["token2"] == "XRP") | (rl_usd_xrp_pool_data["token2"] == "RLUSD"))
    ]

    xpr_rlusd_pool['amount1_norm'] = xpr_rlusd_pool['amount1'] / 1e6

    rlusd_in_xrp_lp = xpr_rlusd_pool['amount2_value'].values[0]

    xrp_in_xrp_lp = xpr_rlusd_pool['amount1_norm'].values[0]

    return rl_usd_xrp_pool_data, xpr_rlusd_pool, rlusd_in_xrp_lp, xrp_in_xrp_lp

    



    
    


