from python_scripts.utils import call_api
from defiquant import dune_api_results
import pandas as pd
import requests
from web3 import Web3
import os
import json
from dotenv import load_dotenv
from defiquant import (pool_data, active_addresses, token_dex_stats)
from defiquant import (flipside_api_results,dune_api_results)
from cachetools import TTLCache, cached

daily_cache = TTLCache(maxsize=100, ttl=7200)  # Cache valid for 6h for flexibility
hourly_cache = TTLCache(maxsize=100, ttl=3000)  # Cache valid for 50m for flexibility 

load_dotenv()

ETHEREUM_GATEWAY = os.getenv('ETHEREUM_GATEWAY')
RLUSD_ETHEREUM_ADDRESS = os.getenv('RLUSD_ETHEREUM_ADDRESS')
RLUSD_XRP_ADDRESS = os.getenv('RLUSD_XRP_ADDRESS')
DUNE_QUERY_ID = os.getenv('DUNE_QUERY_ID')
FLIPSIDE_KEY = os.getenv('FLIPSIDE_KEY')

DUNE_QUERY_DIR = 'data/rlusd_eth_dex_stats.csv'
erc20_abi_path = 'abi/erc20_abi.json'

abi_paths = [erc20_abi_path]
abis = {}

for path in abi_paths:
    with open(path, "r") as file:
        abis[path] = json.load(file)

w3 = Web3(Web3.HTTPProvider(ETHEREUM_GATEWAY))

from python_scripts.utils import (call_api, get_pagination_results)

def dune_dex_data(DUNE_QUERY_ID,DUNE_QUERY_DIR):
    rlusd_eth_dex_stats = dune_api_results(DUNE_QUERY_ID,DUNE_QUERY_DIR)

    rlusd_eth_dex_stats['dt'] = pd.to_datetime(rlusd_eth_dex_stats['dt'])
    rlusd_eth_dex_stats['dt'] = rlusd_eth_dex_stats['dt'].dt.strftime('%Y-%m-%d')

    rlusd_eth_dex_stats['blockchain'] = 'Ethereum'
    rlusd_eth_dex_stats.rename(columns={"vol":"volume"},inplace=True)
    rlusd_eth_dex_stats.set_index('dt',inplace=True)
    rlusd_eth_dex_stats.index = pd.to_datetime(rlusd_eth_dex_stats.index)

    return rlusd_eth_dex_stats

def gecko_terminal_pool_data(
    network='xrpl',
    pool='524C555344000000000000000000000000000000.rMxCKbEDwqr76QuheSUMdEGf4B9xJ8m5De_XRP',
    freq='day',
    start_date=None,
    limit=1000
):
    url = f"https://api.geckoterminal.com/api/v2/networks/{network}/pools/{pool}/ohlcv/{freq}"

    params = {
        "limit": limit,
        "currency": "usd"
    }

    if start_date is not None:
        if isinstance(start_date, pd.Timestamp):
            start_ts = int(start_date.timestamp())
        elif isinstance(start_date, str):
            start_ts = int(pd.to_datetime(start_date).timestamp())
        else:
            start_ts = int(start_date)  # assume epoch integer
        params["before_timestamp"] = start_ts

    data = call_api(url, params=params)
    df = pd.DataFrame(data['data']['attributes']['ohlcv_list'])

    df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    df['blockchain'] = network.upper()

    return df.set_index('timestamp')

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

def dex_data(start_date):

    xrpl_vol = gecko_terminal_pool_data(start_date=start_date)

    rlusd_eth_dex_stats = dune_dex_data(DUNE_QUERY_ID,DUNE_QUERY_DIR)

    filtered_rlusd_eth_dex = rlusd_eth_dex_stats[['blockchain','volume']].resample('D').agg({
        "blockchain":'last',
        "volume":'sum'
    })
    filtered_rlusd_xrpl_dex = xrpl_vol[['blockchain','volume']].resample('D').agg({
        "blockchain":'last',
        "volume":'sum'
    })

    combined_vol = pd.concat([filtered_rlusd_eth_dex, filtered_rlusd_xrpl_dex]).sort_index()

    return combined_vol

def ethereum_pool_data(start_date):

    eth_rlusd_pool_query1 = pool_data(network='ethereum',address='0xd001ae433f254283fece51d4acce8c53263aa186',
                                      start_date=start_date, freq='D')
    eth_rlusd_pool = flipside_api_results(eth_rlusd_pool_query1,FLIPSIDE_KEY)

    eth_rlusd_pool_query2 = pool_data(network='ethereum',address='0xcc6d2f26d363836f85a42d249e145ec0320d3e55',
                                      start_date=start_date, freq='D')
    eth_rlusd_pool2 = flipside_api_results(eth_rlusd_pool_query2,FLIPSIDE_KEY)

    eth_rlusd_pool.dropna(inplace=True)
    eth_rlusd_pool2.dropna(inplace=True)

    print(f'eth_rlusd_pool: {eth_rlusd_pool}, eth_rlusd_pool: {eth_rlusd_pool}')

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