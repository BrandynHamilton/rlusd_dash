from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import os
import pandas as pd

from flask import Flask, jsonify, request

import datetime as dt
from dotenv import load_dotenv

import json

from diskcache import Cache
from web3 import Web3

from python_scripts.data_processing import (clean_dataset_values)
from python_scripts.apis import (supply_data,xrpl_pools ,ethereum_pool_data)

load_dotenv()

app = Flask(__name__)

DUNE_QUERY_ID = os.getenv('DUNE_QUERY_ID')
RLUSD_XRP_ADDRESS = os.getenv('RLUSD_XRP_ADDRESS')
RLUSD_ETHEREUM_ADDRESS = os.getenv('RLUSD_ETHEREUM_ADDRESS')
ETHEREUM_GATEWAY = os.getenv('ETHEREUM_GATEWAY')

DUNE_QUERY_DIR = 'data/rlusd_eth_dex_stats.csv'
BACKUP_DIR = 'data'

FLIPSIDE_KEY = os.getenv('FLIPSIDE_KEY')

w3 = Web3(Web3.HTTPProvider(ETHEREUM_GATEWAY))

erc20_abi_path = 'abi/erc20_abi.json'

abi_paths = [erc20_abi_path]
abis = {}

for path in abi_paths:
    with open(path, "r") as file:
        abis[path] = json.load(file)

cache = Cache('data_collection')

def update_cache_data(data, key='timeseries',time_col='dt',keep_subset=None, granularity=None):
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

    breakpoint()

    if granularity is not None:
        group_cols = keep_cols
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
    historical_data.to_csv(os.path.join(BACKUP_DIR,f'{key}.csv'))
    print(f'Saved {key} with {granularity}')

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
                      time_col='hour',granularity=None)

    print(f'Hourly data collected at {today_utc}')
    return {"status": "success", "timestamp": today_utc.isoformat()}
    
def daily_data():

    today_utc = dt.datetime.now(dt.timezone.utc) 

    eth_rlusd_pool_data = cache.get('eth_rlusd_pool_data',pd.DataFrame())
    if not eth_rlusd_pool_data.empty:
        eth_start_date = eth_rlusd_pool_data['dt'].iloc[-1]
        print(f'eth_start_date: {eth_start_date}')
    else:
        eth_start_date = today_utc.strftime('%Y-%m-%d')

    dex_data = cache.get('dex_data',pd.DataFrame())
    if not dex_data.empty:
        dex_start_date = dex_data['dt'].iloc[-1]
        print(f'dex_start_date: {dex_start_date}')
    else:
        dex_start_date = None

    if dex_start_date:
        start_timestamp = int(dex_start_date.timestamp())
        print(f'start_timestamp: {start_timestamp}')

    combined_vol = dex_data(start_date=start_timestamp)
    
    eth_rlusd_pool = ethereum_pool_data(start_date=eth_start_date)

    update_cache_data(data=eth_rlusd_pool.reset_index(),key='eth_rlusd_pool_data',
                      time_col='dt',keep_subset=['symbol'],
                      granularity=None)
    
    update_cache_data(data=combined_vol.rename_axis('dt').reset_index(),key='dex_data',
                      time_col='dt',keep_subset=['blockchain'],
                      granularity=None)
    
    print(f'Daily data collected at {today_utc}')
    return {"status": "success", "timestamp": today_utc.isoformat()}
 
# Create and start the scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(
    hourly_data, 
    trigger=CronTrigger(minute=0),  # Runs at the top of every hour
    id='hourly_fetch_job', 
    replace_existing=True
)
scheduler.add_job(
    daily_data, 
    trigger=CronTrigger(day='*', hour=0, minute=0),  # Every day at midnight
    id='daily_fetch_job', 
    replace_existing=True
)
scheduler.start()

@app.route('/')
def home():
    return jsonify({"message": "RLUSD Data Collection Service Running!"})

@app.route('/run_job', methods=['POST'])
def run_job():
    """Endpoint to manually trigger the job"""
    data = request.get_json()
    print(F'data: {data}')
    if not data:
        return jsonify({"status": "error", "message": "Missing JSON payload"}), 400

    job_type = data.get('type')
    if job_type == 'hour':
        result = hourly_data()
    elif job_type == 'day':
        result = daily_data()
    else:
        return jsonify({"status": "error", "message": "Invalid job type"}), 400

    return jsonify(result)

@app.route('/status', methods=['GET'])
def job_status():
    """Check the next scheduled run time"""
    data = request.get_json()
    if not data:
        return jsonify({"status": "error", "message": "Missing JSON payload"}), 400

    job_type = data.get('type')

    if job_type == 'hour':
        job = scheduler.get_job('hourly_fetch_job')
    elif job_type == 'day':
        job = scheduler.get_job('daily_fetch_job')
    else:
        return jsonify({"status": "error", "message": "Invalid job type"}), 400

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





