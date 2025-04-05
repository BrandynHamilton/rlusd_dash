from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import os
import pandas as pd

from flask import Flask, jsonify

import datetime as dt
from datetime import timedelta
import sys
import requests
from dotenv import load_dotenv

import time
import json

from diskcache import Cache
from web3 import Web3
import requests

from python_scripts.utils import (call_api, get_pagination_results)
from python_scripts.data_processing import (clean_dataset_values)

from defiquant import (pool_data, active_addresses, token_dex_stats)
from defiquant import (flipside_api_results,dune_api_results)

load_dotenv()

app = Flask(__name__)

DUNE_QUERY_ID = os.getenv('DUNE_QUERY_ID')
RLUSD_XRP_ADDRESS = os.getenv('RLUSD_XRP')
RLUSD_ETHEREUM_ADDRESS = os.getenv('RLUSD_ETHEREUM')
ETHEREUM_GATEWAY = os.getenv('ETHEREUM_GATEWAY')

DUNE_QUERY_DIR = 'data/rlusd_eth_dex_stats.csv'

FLIPSIDE_KEY = os.getenv('ETHEREUM_GATEWAY')

w3 = Web3(Web3.HTTPProvider(ETHEREUM_GATEWAY))

erc20_abi_path = 'abi/erc20_abi.json'

abi_paths = [erc20_abi_path]
abis = {}

for path in abi_paths:
    with open(path, "r") as file:
        abis[path] = json.load(file)

cache = Cache('data_collection')

def update_cache_data(data, key='timeseries',time_col='dt',keep_subset=None, granularity='H'):
    #timeseries column must have dt as name, 

    if keep_subset is None:
        keep_subset = []

    keep_cols = [time_col]+keep_subset
    
    print(f'keep_cols: {keep_cols}')

    if isinstance(data, pd.DataFrame):  
        new_data = data
    elif isinstance(data, dict):       
        new_data = pd.DataFrame([data])
    else:
        raise TypeError('Pass a DataFrame or dict for data')

    historical_data = cache.get(f'{key}', pd.DataFrame())
    # historical_data = historical_data.reset_index()
    historical_data = pd.concat([historical_data, new_data]).reset_index(drop=True)
    historical_data.drop_duplicates(subset=keep_cols, keep='last', inplace=True)
    historical_data[time_col] = pd.to_datetime(historical_data[time_col])
    
    print(f'historical_data before resampling:\n{historical_data}')

    if granularity is not None:
        group_cols = keep_subset
        historical_data = (
            historical_data
            .set_index(time_col)
            .sort_index()
            .groupby(group_cols, group_keys=False)
            .resample(granularity)
            .ffill()
            .reset_index()
        )

    print(f'historical_data after resampling:\n{historical_data}')
    cache.set(f'{key}', historical_data)
    print(f'Saved {key} with {granularity}')

def supply_data():
    # Ethereum Supply
    rlusd_ETH_supply = None
    rlusd_XRP_supply = None

    try:
        rlusd_contract = w3.eth.contract(address=RLUSD_ETHEREUM_ADDRESS, abi=abis['abi/erc20_abi.json'])
        rlusd_ETH_supply = rlusd_contract.functions.totalSupply().call() / 1e18
    except Exception as e:
        print(f'web3.py call failed: {e}')

    # XRPL Supply
    try:
        base_url = f'https://api.xrpscan.com/api/v1/account/{RLUSD_XRP_ADDRESS}/obligations'
        data = call_api(base_url)
        rlusd_raw = pd.DataFrame(data)
        rlusd_XRP_supply = float(rlusd_raw['value'].values[0])
    except Exception as e:
        print(f'xrpscan call failed: {e}')

    return rlusd_XRP_supply, rlusd_ETH_supply

def xrpl_pools(pool='rhWTXC2m2gGGA9WozUaoMm6kLAVPb1tcS3'):

    "This gets all rlusd pools in AMM or returns a singlular pool"

    rl_usd_xrp_pool_data = None

    if pool is None:
        print(f'pool is none')

        try:

            base_url = f'https://api.xrpscan.com/api/v1/amm/pools'
            amm_data = get_pagination_results(base_url)
            amm_df = pd.DataFrame(amm_data)

            amm_df["AssetName_Extracted"] = amm_df["AssetName"].apply(lambda x: x.get("name") if isinstance(x, dict) else None)
            amm_df["Asset2Name_Extracted"] = amm_df["Asset2Name"].apply(lambda x: x.get("name") if isinstance(x, dict) else None)

            filtered_df = amm_df[(amm_df["AssetName_Extracted"] == "RLUSD") | (amm_df["Asset2Name_Extracted"] == "RLUSD")]

            rlusd_pools = filtered_df['Account'].unique()

            all_results = []

            for ammpool in rlusd_pools:
                base_url = f'https://api.xrpscan.com/api/v1/amm/{ammpool}'
                data = call_api(base_url)
                all_results.append(data)

            rl_usd_xrp_pool_data = pd.DataFrame(all_results)
        except Exception as e:
            print(f'amm call failed: {e}')
    else:
        try:
            #I believe there is only one xrp/rlusd pool so for now only tracking that
            base_url = f'https://api.xrpscan.com/api/v1/amm/{pool}'
            data = call_api(base_url)
            rl_usd_xrp_pool_data = pd.DataFrame([data])
        except Exception as e:
            print(f'amm call failed: {e}')
    
    return rl_usd_xrp_pool_data

def hourly_data():
    print(f'Running Main')

    # Here we are collecting supply by chain, and supply in XRPL AMM 

    rlusd_XRP_supply, rlusd_ETH_supply = supply_data()
    
    rl_usd_xrp_pool_data = xrpl_pools(pool='rhWTXC2m2gGGA9WozUaoMm6kLAVPb1tcS3')

    _, _, rlusd_in_xrp_lp, xrp_in_xrp_lp = clean_dataset_values(rl_usd_xrp_pool_data)

    today_utc = dt.datetime.now(dt.timezone.utc) 
    formatted_today_utc = today_utc.strftime('%Y-%m-%d %H:00:00')

    timeseries_entry = {
        "dt":today_utc,
        "hour":formatted_today_utc,
        "xrp_bal":xrp_in_xrp_lp,
        "rlusd_bal":rlusd_in_xrp_lp,
        "RLUSD_XRPL_Supply":rlusd_XRP_supply,
        "RLUSD_ETH_Supply":rlusd_ETH_supply
    }

    update_cache_data(data=timeseries_entry,key='timeseries',
                      time_col='dt',granularity='H')

    print(f'Data collected at {today_utc}')
    return {"status": "success", "timestamp": today_utc.isoformat()}

def ethereum_pool_data(data_start_date):
    eth_rlusd_pool_query1 = pool_data('0xd001ae433f254283fece51d4acce8c53263aa186',start_date=data_start_date)
    eth_rlusd_pool = flipside_api_results(eth_rlusd_pool_query1,FLIPSIDE_KEY)

    eth_rlusd_pool_query2 = pool_data('0xcc6d2f26d363836f85a42d249e145ec0320d3e55',start_date=data_start_date)
    eth_rlusd_pool2 = flipside_api_results(eth_rlusd_pool_query2,FLIPSIDE_KEY)

    eth_rlusd_pool.dropna(inplace=True)
    eth_rlusd_pool2.dropna(inplace=True)

    eth_rlusd_pool = pd.concat([eth_rlusd_pool.drop(columns=['total_tvl','__row_index']),eth_rlusd_pool2.drop(columns=['total_tvl','__row_index'])]).groupby(['symbol','dt']).sum().reset_index()

    eth_rlusd_pool['dt'] = pd.to_datetime(eth_rlusd_pool['dt'])
    eth_rlusd_pool.set_index('dt',inplace=True)

    eth_rlusd_pool.sort_index(inplace=True, ascending=False)
    total_tvl = eth_rlusd_pool.groupby(eth_rlusd_pool.index)['tvl'].sum()

    eth_rlusd_pool = eth_rlusd_pool.merge(
        total_tvl.to_frame('total_tvl'),
        left_index=True,
        right_index=True,
        how='inner'
    )

    return eth_rlusd_pool

def dex_data(data_start_date):
    rlusd_eth_dex_stats = dune_api_results(DUNE_QUERY_ID,True,DUNE_QUERY_DIR)

    xrpl_vol_base_url = f"https://api.geckoterminal.com/api/v2/networks/xrpl/pools/524C555344000000000000000000000000000000.rMxCKbEDwqr76QuheSUMdEGf4B9xJ8m5De_XRP/ohlcv/day"
    data = call_api(xrpl_vol_base_url)

    xrpl_vol = pd.DataFrame(data['data']['attributes']['ohlcv_list'])

    # Rename columns for clarity (assuming standard OHLCV format)
    xrpl_vol.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']

    # Convert timestamp to datetime for readability
    xrpl_vol['timestamp'] = pd.to_datetime(xrpl_vol['timestamp'], unit='s')

    rlusd_eth_dex_stats['dt'] = pd.to_datetime(rlusd_eth_dex_stats['dt'])
    rlusd_eth_dex_stats['dt'] = rlusd_eth_dex_stats['dt'].dt.strftime('%Y-%m-%d')

    xrpl_vol.set_index('timestamp',inplace=True)
    xrpl_vol['blockchain'] = 'XRPL'

    rlusd_eth_dex_stats['blockchain'] = 'Ethereum'
    rlusd_eth_dex_stats.rename(columns={"vol":"volume"},inplace=True)
    rlusd_eth_dex_stats.set_index('dt',inplace=True)
    rlusd_eth_dex_stats.index = pd.to_datetime(rlusd_eth_dex_stats.index)

    filtered_rlusd_eth_dex = rlusd_eth_dex_stats[['blockchain','volume']].resample('D').agg({
        "blockchain":'last',
        "volume":'sum'
    })
    filtered_rlusd_xrpl_dex = xrpl_vol[['blockchain','volume']].resample('D').agg({
        "blockchain":'last',
        "volume":'sum'
    })

    combined_vol = pd.concat([filtered_rlusd_eth_dex, filtered_rlusd_xrpl_dex])

    update_cache_data(data=combined_vol,key='dex_data',
                      time_col='dt',granularity='D')

# Create and start the scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(
    hourly_data, 
    trigger=CronTrigger(minute=0),  # Runs at the top of every hour
    id='data_fetch_job', 
    replace_existing=True
)
scheduler.start()

@app.route('/')
def home():
    return jsonify({"message": "RLUSD Data Collection Service Running!"})

@app.route('/run_job', methods=['POST'])
def run_job():
    """Endpoint to manually trigger the job"""
    result = hourly_data()
    return jsonify(result)

@app.route('/status', methods=['GET'])
def job_status():
    """Check the next scheduled run time"""
    job = scheduler.get_job('data_fetch_job')
    if job:
        return jsonify({"next_run_time": job.next_run_time.isoformat()})
    return jsonify({"error": "Job not found"}), 404

@app.route('/dataset', methods=['GET'])
def get_timeseries():
    """Returns cached dataset"""
    dataset = cache.get('timeseries')

    if dataset.empty:
        return jsonify({"error":"No data found"}), 404

    dataset_json = dataset.to_dict(orient='records')
    return jsonify(dataset_json), 200

@app.route('/clear_cache', methods=['POST'])
def clear_cache():
    """Endpoint to clear the cache"""
    try:
        cache.clear()
        return jsonify({"message": "Cache cleared successfully!"}), 200
    except Exception as e:
        return jsonify({"error": f"Failed to clear cache: {str(e)}"}), 500

if __name__ == '__main__':
    print("Flask app running with background scheduler...")
    app.run(host='0.0.0.0', port=5256, debug=True, use_reloader=False)  # use_reloader=False to avoid duplicate jobs





