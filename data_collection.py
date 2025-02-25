from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import os
import pandas as pd

from flask import Flask, jsonify

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

import time
import json

from diskcache import Cache
from web3 import Web3
import requests

load_dotenv()

app = Flask(__name__)

RLUSD_XRP = os.getenv('RLUSD_XRP')
RLUSD_ETHEREUM = os.getenv('RLUSD_ETHEREUM')
ETHEREUM_GATEWAY = os.getenv('ETHEREUM_GATEWAY')

w3 = Web3(Web3.HTTPProvider(ETHEREUM_GATEWAY))

erc20_abi_path = 'abi/erc20_abi.json'

abi_paths = [erc20_abi_path]
abis = {}

for path in abi_paths:
    with open(path, "r") as file:
        abis[path] = json.load(file)

cache = Cache('data_collection')

def call_api(base_url):
    response = requests.get(base_url)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        print("data:", data)
        return data
    else:
        print("Error:", response.status_code, response.text)
        return None

def get_pagination_results(base_url):
    # Pagination parameters
    limit = 100  # Number of records per request
    offset = 0  # Start from the first record
    all_results = []  # Store all retrieved results

    while True:
        # Construct URL with pagination parameters
        url = f"{base_url}?offset={offset}&limit={limit}"
        
        # Make API request
        response = requests.get(url)
        
        # Check for successful response
        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.text}")
            break
        
        # Parse JSON response
        data = response.json()
        
        # Append results
        if not data:  # Stop when no more data is returned
            break
        
        all_results.extend(data)
        
        # Update offset to fetch the next batch
        offset += limit

    # Print the total number of results retrieved
    print(f"Total records fetched: {len(all_results)}")
    return all_results

def update_historical_data(live_comp):
    new_data = pd.DataFrame([live_comp])
    historical_data = cache.get(f'timeseries', pd.DataFrame())
    historical_data = pd.concat([historical_data, new_data]).reset_index(drop=True)
    historical_data.drop_duplicates(subset='dt', keep='last', inplace=True)
    cache.set(f'timeseries', historical_data)

def main():
    print(f'Running Main')
    rlusd_contract = w3.eth.contract(address=RLUSD_ETHEREUM, abi=abis['abi/erc20_abi.json'])
    rlusd_ETH_supply = rlusd_contract.functions.totalSupply().call() /1e18
    base_url = f'https://api.xrpscan.com/api/v1/account/{RLUSD_XRP}/obligations'
    data = call_api(base_url)
    rlusd_raw = pd.DataFrame(data)
    rlusd_XRP_supply = float(rlusd_raw['value'].values[0])


    #This gets all rlusd pools

    # base_url = f'https://api.xrpscan.com/api/v1/amm/pools'
    # amm_data = get_pagination_results(base_url)
    # amm_df = pd.DataFrame(amm_data)

    # amm_df["AssetName_Extracted"] = amm_df["AssetName"].apply(lambda x: x.get("name") if isinstance(x, dict) else None)
    # amm_df["Asset2Name_Extracted"] = amm_df["Asset2Name"].apply(lambda x: x.get("name") if isinstance(x, dict) else None)

    # filtered_df = amm_df[(amm_df["AssetName_Extracted"] == "RLUSD") | (amm_df["Asset2Name_Extracted"] == "RLUSD")]

    # rlusd_pools = filtered_df['Account'].unique()

    # all_results = []

    # for pool in rlusd_pools:
    #     base_url = f'https://api.xrpscan.com/api/v1/amm/{pool}'
    #     data = call_api(base_url)
    #     all_results.append(data)

    # rl_usd_xrp_pool_data = pd.DataFrame(all_results)

    #I believe there is only one xrp/rlusd pool so for now only tracking that
    base_url = f'https://api.xrpscan.com/api/v1/amm/rhWTXC2m2gGGA9WozUaoMm6kLAVPb1tcS3'
    data = call_api(base_url)
    rl_usd_xrp_pool_data = pd.DataFrame([data])

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

    today_utc = dt.datetime.now(dt.timezone.utc) 
    formatted_today_utc = today_utc.strftime('%Y-%m-%d %H:00:00')

    timeseries_entry = {
        "dt":today_utc,
        "xrp_bal":xrp_in_xrp_lp,
        "rlusd_bal":rlusd_in_xrp_lp,
        "RLUSD_XRPL_Supply":rlusd_XRP_supply,
        "RLUSD_ETH_Supply":rlusd_ETH_supply
    }

    update_historical_data(timeseries_entry)

    print(f'Data collected at {today_utc}')
    return {"status": "success", "timestamp": today_utc.isoformat()}

# Create and start the scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(
    main, 
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
    result = main()
    return jsonify(result)

@app.route('/status', methods=['GET'])
def job_status():
    """Check the next scheduled run time"""
    job = scheduler.get_job('data_fetch_job')
    if job:
        return jsonify({"next_run_time": job.next_run_time.isoformat()})
    return jsonify({"error": "Job not found"}), 404

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





