# http_utils.py
import requests
import time

MAX_RETRIES = 5

def fetch_transactions(url,params, headers = {"accept": "application/json"}):
    """Fetch transactions from the API with retry mechanism."""
    retries = 0
    while retries < MAX_RETRIES:
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            retries += 1
            if retries < MAX_RETRIES:
                print("API error. Retrying in 5 seconds...")
                time.sleep(5)
            else:
                raise Exception("Max retries reached. Exiting.")
