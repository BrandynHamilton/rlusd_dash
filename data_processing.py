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

import requests
from dune_client.client import DuneClient

from chart_builder.scripts.visualization_pipeline import visualization_pipeline
from chart_builder.scripts.utils import data_processing,create_df,open_json, main as chartBuilder

# Set the default template
from sql_queries.sql_scripts import lp_data
from python_scripts.utils import flipside_api_results

load_dotenv()

def dune_api_results(query_num, save_csv=False, csv_path=None):
    results = dune.get_latest_result(query_num)
    df = pd.DataFrame(results.result.rows)

    if save_csv and csv_path:
        df.to_csv(csv_path, index=False)
    return df

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
    
def prepare_data_for_simulation(price_timeseries, start_date, end_date):
    """
    Ensure price_timeseries has entries for start_date and end_date.
    If not, fill in these dates using the last available data.
    """
    # Ensure 'ds' is in datetime format
    # price_timeseries['hour'] = pd.to_datetime(price_timeseries['hour'])
    
    # Set the index to 'ds' for easier manipulation
    # if price_timeseries.index.name != 'hour':
    #     price_timeseries.set_index('hour', inplace=True)

    print(f'price index: {price_timeseries.index}')

    price_timeseries.index = price_timeseries.index.tz_localize(None)
    
    # Check if start_date and end_date exist in the data
    required_dates = pd.date_range(start=start_date, end=end_date, freq='H')
    all_dates = price_timeseries.index.union(required_dates)
    
    # Reindex the dataframe to ensure all dates from start to end are present
    price_timeseries = price_timeseries.reindex(all_dates)
    
    # Forward fill to handle NaN values if any dates were missing
    price_timeseries.fillna(method='ffill', inplace=True)

    # Reset index if necessary or keep the datetime index based on further needs
    # price_timeseries.reset_index(inplace=True, drop=False)
    # price_timeseries.rename(columns={'index': 'hour'}, inplace=True)
    # price_timeseries.set_index('hour',inplace=True)
    
    return price_timeseries
    
cache = Cache('data_collection')

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

rlusd_contract = w3.eth.contract(address=RLUSD_ETHEREUM, abi=abis['abi/erc20_abi.json'])

def main():
    rlusd_ETH_supply = rlusd_contract.functions.totalSupply().call() /1e18

    base_url = f'https://api.xrpscan.com/api/v1/account/{RLUSD_XRP}/obligations'
    data = call_api(base_url)

    rlusd_raw = pd.DataFrame(data)
    rlusd_XRP_supply = float(rlusd_raw['value'].values[0])

    supply_dict = {
        "Blockchain":["Ethereum","XRP"],
        "Supply":[rlusd_ETH_supply,rlusd_XRP_supply]
    }

    supply_df = pd.DataFrame(supply_dict)

    total_rlusd_supply = supply_df['Supply'].sum()

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

    eth_rlusd_pool_query1 = lp_data('0xd001ae433f254283fece51d4acce8c53263aa186')
    eth_rlusd_pool = flipside_api_results(eth_rlusd_pool_query1,FLIPSIDE_KEY)

    eth_rlusd_pool_query2 = lp_data('0xcc6d2f26d363836f85a42d249e145ec0320d3e55')
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

    rlusd_eth_dex_stats = dune_api_results(4695750,True,'data/rlusd_eth_dex_stats.csv')

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

    filtered_rlusd_eth_dex = rlusd_eth_dex_stats[['blockchain','volume']].resample('W-SUN').agg({
        "blockchain":'last',
        "volume":'sum'
    })
    filtered_rlusd_xrpl_dex = xrpl_vol[['blockchain','volume']].resample('W-SUN').agg({
        "blockchain":'last',
        "volume":'sum'
    })

    combined_vol = pd.concat([filtered_rlusd_eth_dex, filtered_rlusd_xrpl_dex])
    combined_vol.sort_index(inplace=True)
    combined_vol_total = combined_vol.groupby(combined_vol.index)[['volume']].sum()
    vol_by_chain = combined_vol.groupby('blockchain')[['volume']].sum().reset_index()

    fig1 = visualization_pipeline(
        df=supply_df,
            title='rlusd_fig1',
            # start_date='2024-01-01',
            # end_date='2024-12-31',
            # end_date='2024-10-01',
            chart_type='pie',
            dimensions=dict(height=351, width=730),
            groupby = 'Blockchain',
            num_col='Supply',
            # cols_to_plot='All',
            # line_col=['cumulative_freq'],
            # bar_col=['freg'],
            tickangle=0,
            dropna=True,
            area=True,
            # colors=combined_colors,
            # axes_data=dict(y1=['UAW'],y2=[]),
            tickformat=dict(x='%b<br>`%y',y1=None,y2=None),
            legend_font_size=12,
            # font_family=font_family,
            cumulative_sort=True,
            normalize=False,
            decimals=True,
            decimal_places=0,
            y2=True,
            # min_tick_spacing=50,
            # buffer=3,
            barmode='group',
            mode='lines+markers',
            # font_size=14,
            # line_width=3,
            margin=dict(l=0, r=0, t=50, b=0),
            marker_size=3,
            tickprefix=dict(y1='$',y2=None),
            ticksuffix=dict(y1=None,y2=None),
            axes_titles=dict(y1=None,y2=None),
            show_legend=False,
            hole_size=0,
            line_width=0,
            descending=False,
            buffer = 0.25,
            annotations=True,
            text=True,
            orientation='h',
            sort_list=True,
            autosize=False,
            max_annotation=True,
            annotation_prefix=None,
            auto_title=False,
            axes_font_colors='auto',
            to_reverse=False,
            line_color='black',
            use_single_color=False,
            custom_annotation = ['2022-10-31'],
            # axes_font_colors='auto',
        )
    chartBuilder(
        fig=fig1,
        add_the_date=False,
        clean_columns=True,
        date_xy=dict(x=0.05,y=1.02),
        keep_top_n=False,
        # groupby=True,
        other=True,
        save=False,
        show=False,
        topn=4,
        title_xy=dict(x=0.1,y=0.92),
        title='RLUSD Supply by Chain',
        # dashed_line=True,
        # date='2023-04-12',
        # annotation_text='Staked ETH <br> Withdrawl Activated'
    )

    fig3 = visualization_pipeline(
    df=supply_comp_df,
        title='rlusd_fig3',
        # start_date='2024-01-01',
        # end_date='2024-12-31',
        # end_date='2024-10-01',
        chart_type='pie',
        dimensions=dict(height=351, width=730),
        groupby = 'Status',
        num_col='Amount',
        # cols_to_plot='All',
        # line_col=['cumulative_freq'],
        # bar_col=['freg'],
        tickangle=0,
        dropna=True,
        area=True,
        # axes_data=dict(y1=['UAW'],y2=[]),
        tickformat=dict(x='%b<br>`%y',y1=None,y2=None),
        legend_font_size=12,
        cumulative_sort=True,
        normalize=False,
        decimals=True,
        decimal_places=0,
        y2=True,
        # min_tick_spacing=50,
        # buffer=3,
        barmode='group',
        mode='lines+markers',
        # font_size=14,
        # line_width=3,
        marker_size=3,
        tickprefix=dict(y1='$',y2=None),
        ticksuffix=dict(y1=None,y2=None),
        axes_titles=dict(y1=None,y2=None),
        show_legend=False,
        hole_size=0,
        line_width=0,
        descending=False,
        buffer = 0.25,
        annotations=True,
        text=True,
        orientation='h',
        sort_list=True,
        margin=dict(l=0, r=0, t=100, b=0),
        autosize=False,
        max_annotation=True,
        annotation_prefix=None,
        auto_title=False,
        axes_font_colors='auto',
        to_reverse=False,
        line_color='black',
        use_single_color=False,
        custom_annotation = ['2022-10-31'],
        # axes_font_colors='auto',
    )

    chartBuilder(
        fig=fig3,
        add_the_date=False,
        clean_columns=True,
        date_xy=dict(x=0.05,y=1.02),
        keep_top_n=False,
        # groupby=True,
        other=True,
        save=False,
        show=False,
        title_xy=dict(x=0.1,y=0.9),
        topn=4,
        title='Share of RLUSD Locked in Liquidity Pools'
        # dashed_line=True,
        # date='2023-04-12',
        # annotation_text='Staked ETH <br> Withdrawl Activated'
    )

    fig4 = visualization_pipeline(
    df=combined_rlusd_lp[combined_rlusd_lp.index >= rlusd_xrp_df.index.min()],
        title='rlusd_fig4',
        start_date=str(rlusd_xrp_df.index.min()),
        # end_date='2024-12-31',
        # end_date='2024-10-01',
        chart_type='line',
        dimensions=dict(height=351, width=730),
        groupby = 'blockchain',
        num_col='current_bal',
        # cols_to_plot=["RLUSD_ETH_LP","RLUSD_XRP_LP"],
        # line_col=['cumulative_freq'],
        # bar_col=['freg'],
        tickangle=0,
        dropna=True,
        area=True,
        # axes_data=dict(y1=['total_tvl'],y2=[]),
        # tickformat=dict(x='%b<br>`%y',y1=None,y2=None),
        legend_font_size=12,
        cumulative_sort=True,
        normalize=False,
        decimals=True,
        decimal_places=1,
        y2=True,
        min_tick_spacing=50,
        # buffer=3,
        barmode='group',
        mode='lines',
        # font_size=14,
        # line_width=3,
        marker_size=3,
        tickprefix=dict(y1='$',y2=None),
        ticksuffix=dict(y1=None,y2=None),
        axes_titles=dict(y1=None,y2=None),
        show_legend=True,
        hole_size=0,
        # line_width=1,
        descending=False,
        margin=dict(l=0, r=0, t=100, b=0),
        legend_background=dict(bgcolor='white',bordercolor='black',
                                                                                                        borderwidth=1, itemsizing='constant',
                                                                                                        yanchor="top",xanchor="center",buffer=8),
        # buffer = 0.25,
        annotations=False,
        legend_placement=dict(x=0.75,y=0.9),
        text=True,
        orientation='h',
        sort_list=True,
        autosize=False,
        max_annotation=False,
        annotation_prefix="$",
        auto_title=False,
        axes_font_colors='auto',
        to_reverse=False,
        line_color='black',
        use_single_color=False,
        custom_annotation = ['2022-10-31'],
        # axes_font_colors='auto',
    )

    chartBuilder(
        fig=fig4,
        add_the_date=True,
        clean_columns=False,
        # date_xy=dict(x=0.05,y=1.02),
        keep_top_n=False,
        # groupby=True,
        other=True,
        title='RLUSD Locked in Liquidity Pools <br>by Chain Over Time',
        title_xy=dict(x=0.1,y=0.9),
        save=False,
        show=False,
        date_xy=dict(x=0.93,y=1.05),
        # title_xy=dict(x=0.1,y=0.9),
        topn=4,
        # dashed_line=True,
        # date='2023-04-12',
        # annotation_text='Staked ETH <br> Withdrawl Activated'
    )

    fig7 = visualization_pipeline(
    df=vol_by_chain,
        title='rlusd_fig7',
        # start_date='2024-01-01',
        # end_date='2024-12-31',
        # end_date='2024-10-01',
        chart_type='pie',
        dimensions=dict(height=351, width=730),
        groupby = 'blockchain',
        num_col='volume',
        # cols_to_plot='All',
        # line_col=['cumulative_freq'],
        # bar_col=['freg'],
        tickangle=0,
        dropna=True,
        area=True,
        # axes_data=dict(y1=['UAW'],y2=[]),
        tickformat=dict(x='%b<br>`%y',y1=None,y2=None),
        legend_font_size=12,
        cumulative_sort=True,
        normalize=False,
        decimals=True,
        decimal_places=0,
        y2=True,
        # min_tick_spacing=50,
        # buffer=3,
        barmode='group',
        mode='lines+markers',
        # font_size=14,
        # line_width=3,
        marker_size=3,
        tickprefix=dict(y1='$',y2=None),
        ticksuffix=dict(y1=None,y2=None),
        axes_titles=dict(y1=None,y2=None),
        show_legend=False,
        hole_size=0,
        line_width=0,
        descending=False,
        buffer = 0.25,
        margin=dict(l=0, r=0, t=100, b=0),
        annotations=True,
        text=True,
        orientation='h',
        sort_list=True,
        autosize=False,
        max_annotation=True,
        annotation_prefix='$',
        auto_title=False,
        axes_font_colors='auto',
        to_reverse=False,
        line_color='black',
        use_single_color=False,
        custom_annotation = ['2022-10-31'],
        # axes_font_colors='auto',
    )
    chartBuilder(
        fig=fig7,
        add_the_date=False,
        clean_columns=False,
        date_xy=dict(x=0.05,y=1.02),
        keep_top_n=False,
        # groupby=True,
        other=True,
        save=False,
        show=False,
        title_xy=dict(x=0.1,y=0.92),
        title='RLUSD Cumulative DEX Volume <br>by Chain',
        topn=4,
        # dashed_line=True,
        # date='2023-04-12',
        # annotation_text='Staked ETH <br> Withdrawl Activated'
    )

    print(f'formatted_today_utc: {formatted_today_utc}')
    print(f'combined_vol.index: {combined_vol.index}')
    # combined_vol[combined_vol.index < formatted_today_utc]

    fig6 = visualization_pipeline(
    df=combined_vol[combined_vol.index < combined_vol.index.max()],
        title='rlusd_fig6',
        start_date=str(rlusd_xrp_df.index.min()),
        # end_date='2024-12-31',
        # end_date='2024-10-01',
        chart_type='bar',
        dimensions=dict(height=351, width=730),
        groupby = 'blockchain',
        num_col='volume',
        # cols_to_plot=["RLUSD_ETH_LP","RLUSD_XRP_LP"],
        # line_col=['cumulative_freq'],
        # bar_col=['freg'],
        tickangle=0,
        dropna=True,
        area=True,
        # colors=combined_colors,
        # axes_data=dict(y1=['total_tvl'],y2=[]),
        # tickformat=dict(x='%b<br>`%y',y1=None,y2=None),
        legend_font_size=12,
        # font_family=font_family,
        legend_placement=dict(x=0.8,y=0.9),
        cumulative_sort=True,
        normalize=False,
        decimals=True,
        decimal_places=1,
        y2=True,
        min_tick_spacing=50,
        # buffer=3,
        # barmode='group',
        mode='lines',
        # font_size=14,
        # line_width=3,
        marker_size=3,
        margin=dict(l=0, r=0, t=50, b=0),
        tickprefix=dict(y1='$',y2=None),
        ticksuffix=dict(y1=None,y2=None),
        axes_titles=dict(y1=None,y2=None),
        show_legend=True,
        hole_size=0,
        # line_width=1,
        descending=True,
        buffer = 3,
        annotations=False,
        text=False,
        orientation='h',
        sort_list=True,
        autosize=False,
        max_annotation=False,
        annotation_prefix="$",
        auto_title=False,
        axes_font_colors='auto',
        to_reverse=False,
        line_color='black',
        use_single_color=False,
        custom_annotation = ['2022-10-31'],
        # axes_font_colors='auto',
    )

    chartBuilder(
        fig=fig6,
        add_the_date=True,
        clean_columns=False,
        date_xy=dict(x=0.87,y=.98),
        title_xy=dict(x=0.1,y=0.9),
        keep_top_n=False,
        # groupby=True,
        other=True,
        title='RLUSD DEX Volume By Chain',
        save=False,
        show=False,
        topn=4,
        # dashed_line=True,
        # date='2023-04-12',
        # annotation_text='Staked ETH <br> Withdrawl Activated'
    )

    return fig1, fig3, fig4, fig6, fig7 

    



    
    


