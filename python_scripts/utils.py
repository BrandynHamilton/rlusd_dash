import json

def flipside_api_results(query, api_key, attempts=10, delay=30):
    import requests
    import time
    import pandas as pd

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