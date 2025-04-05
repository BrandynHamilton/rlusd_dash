import json
import requests
import time
import pandas as pd
import os
from dotenv import load_dotenv
from dune_client.client import DuneClient

load_dotenv()

DUNE_KEY = os.getenv('DUNE_API_KEY')

dune = DuneClient(DUNE_KEY)

def flipside_api_results(query, api_key, attempts=10, delay=30):

    url = "https://api-v2.flipsidecrypto.xyz/json-rpc"
    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key
    }

    # Step 1: Create the query
    payload = {
        "jsonrpc": "2.0",
        "method": "createQueryRun",
        "params": [
            {
                "resultTTLHours": 1,
                "maxAgeMinutes": 0,
                "sql": query,
                "tags": {"source": "python-script", "env": "production"},
                "dataSource": "snowflake-default",
                "dataProvider": "flipside"
            }
        ],
        "id": 1
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code != 200:
        print(f"Query creation failed. Status: {response.status_code}, Response: {response.text}")
        raise Exception("Failed to create query.")

    try:
        response_data = response.json()
    except json.JSONDecodeError as e:
        print(f"Error decoding query creation response: {e}. Response text: {response.text}")
        raise

    query_run_id = response_data.get('result', {}).get('queryRun', {}).get('id')
    if not query_run_id:
        print(f"Query creation response: {response_data}")
        raise KeyError("Failed to retrieve query run ID.")

    # Step 2: Poll for query completion
    for attempt in range(attempts):
        status_payload = {
            "jsonrpc": "2.0",
            "method": "getQueryRunResults",
            "params": [
                {
                    "queryRunId": query_run_id,
                    "format": "json",
                    "page": {"number": 1, "size": 10000}
                }
            ],
            "id": 1
        }

        response = requests.post(url, headers=headers, json=status_payload)
        if response.status_code != 200:
            print(f"Polling error. Status: {response.status_code}, Response: {response.text}")
            time.sleep(delay)
            continue

        try:
            resp_json = response.json()
        except json.JSONDecodeError as e:
            print(f"Error decoding polling response: {e}. Response text: {response.text}")
            time.sleep(delay)
            continue

        if 'result' in resp_json and 'rows' in resp_json['result']:
            all_rows = []
            page_number = 1

            while True:
                status_payload["params"][0]["page"]["number"] = page_number
                response = requests.post(url, headers=headers, json=status_payload)
                resp_json = response.json()

                if 'result' in resp_json and 'rows' in resp_json['result']:
                    rows = resp_json['result']['rows']
                    if not rows:
                        break  # No more rows to fetch
                    all_rows.extend(rows)
                    page_number += 1
                else:
                    break

            return pd.DataFrame(all_rows)

        if 'error' in resp_json and 'not yet completed' in resp_json['error'].get('message', '').lower():
            print(f"Query not completed. Retrying in {delay} seconds...")
            time.sleep(delay)
        else:
            print(f"Unexpected polling error: {resp_json}")
            raise Exception(f"Polling error: {resp_json}")

    raise TimeoutError(f"Query did not complete after {attempts} attempts.")

def prepare_data_for_simulation(price_timeseries, start_date, end_date):
    """
    Ensure price_timeseries has entries for start_date and end_date.
    If not, fill in these dates using the last available data.
    """

    print(f'price index: {price_timeseries.index}')

    price_timeseries.index = price_timeseries.index.tz_localize(None)
    
    # Check if start_date and end_date exist in the data
    required_dates = pd.date_range(start=start_date, end=end_date, freq='H')
    all_dates = price_timeseries.index.union(required_dates)
    
    # Reindex the dataframe to ensure all dates from start to end are present
    price_timeseries = price_timeseries.reindex(all_dates)
    
    # Forward fill to handle NaN values if any dates were missing
    price_timeseries.fillna(method='ffill', inplace=True)

    return price_timeseries

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