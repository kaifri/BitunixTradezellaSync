#!/usr/bin/env python3
"""
Fetch new futures trades from Bitunix and export to TradeZella Generic Template CSV.

This script:
  • Uses the Bitunix REST endpoint GET /api/v1/futures/trade/get_history_trades to retrieve your account's historical futures trades.
  • Generates request signatures as per Bitunix docs (double SHA-256).
  • Persists the timestamp of the last exported trade in a local state file, ensuring only new trades are exported each run.
  • Loads API credentials from a local JSON file (`credentials.json`) to avoid passing keys on the command line.
  • Transforms trades into TradeZella's Generic CSV format (Date, Time, Symbol, Buy/Sell, Quantity, Price, Spread, Expiration, Strike, Call/Put, Commission, Fees).

Usage:
    1. Create `credentials.json` in the script folder with:
       {
         "api_key": "YOUR_API_KEY",
         "secret_key": "YOUR_SECRET_KEY"
       }
    2. Run:
       python bitunix_to_tradezella.py [--output new_trades.csv]

Options:
    --output      Path for the generated CSV (default: new_trades.csv)
"""
import os
import json
import time
import uuid
import hashlib
import argparse
import requests
import base64
import secrets
import logging
from urllib.parse import urlencode
from datetime import datetime, timezone, timedelta

# Configure logging for better debugging and security monitoring
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bitunix_to_tradezella.log"),
        logging.StreamHandler()
    ]
)

# Constants
API_URL = "https://fapi.bitunix.com/api/v1/futures/trade/get_history_trades"
STATE_FILE = "last_export_state.json"
CREDENTIALS_FILE = "credentials.json"
FIELDNAMES = [
    "Date", "Time", "Symbol", "Buy/Sell", "Quantity", "Price",
    "Spread", "Expiration", "Strike", "Call/Put", "Commission", "Fees"
]

def load_credentials():
    """Load API credentials from credentials.json"""
    if not os.path.exists(CREDENTIALS_FILE):
        logging.error(f"Credentials file '{CREDENTIALS_FILE}' not found.")
        raise FileNotFoundError(f"Credentials file '{CREDENTIALS_FILE}' not found.")
    with open(CREDENTIALS_FILE, 'r') as f:
        cfg = json.load(f)
    api_key = cfg.get('api_key')
    secret_key = cfg.get('secret_key')
    if not api_key or not secret_key:
        logging.error("Both 'api_key' and 'secret_key' must be set in credentials.json.")
        raise KeyError("Both 'api_key' and 'secret_key' must be set in credentials.json.")
    logging.info("Credentials loaded successfully.")
    return api_key, secret_key


class BitunixClient:
    """
    A client for interacting with the BitUnix cryptocurrency exchange API.
    """
    BASE_URL = "https://fapi.bitunix.com"

    def __init__(self, api_key: str, api_secret: str):
        if not api_key or not api_secret:
            raise ValueError("API key and secret must be provided")
        self.api_key = api_key
        self.api_secret = api_secret
        self.session = requests.Session()

    def _generate_nonce(self) -> str:
        """Generate a nonce for API requests."""
        # Use a simple alphanumeric string for the nonce
        return uuid.uuid4().hex

    def sign_request(self, nonce: str, timestamp: str, query_params: dict = None, body: str = None) -> str:
        """Generate a signature for BitUnix API requests."""
        # Sort query parameters by key in ascending ASCII order and concatenate key-value pairs
        query_string = ''.join(f"{k}{v}" for k, v in sorted(query_params.items())) if query_params else ""
        # Ensure body is a compact JSON string without spaces
        body = json.dumps(json.loads(body), separators=(',', ':')) if body else ""

        # Concatenate fields for the first hash
        message = f"{nonce}{timestamp}{self.api_key}{query_string}{body}"

        # First SHA256 encryption
        digest = hashlib.sha256(message.encode('utf-8')).hexdigest()

        # Concatenate digest with secret key for the second hash
        sign_input = f"{digest}{self.api_secret}"
        sign = hashlib.sha256(sign_input.encode('utf-8')).hexdigest()

        return sign

    def fetch_trades(self, start_time: int) -> list:
        """Retrieve all trades with a timestamp greater than `start_time` (ms) in UTC."""
        trades = []
        skip = 0
        limit = 100

        while True:
            now_ms = str(int(time.time() * 1000))
            nonce = secrets.token_hex(8)
            # Prepare query parameter
            params = {'startTime': start_time, 'skip': skip, 'limit': limit}
            signature = self.sign_request(nonce, now_ms, query_params=params)

            headers = {
                'api-key': self.api_key,
                'timestamp': now_ms,
                'nonce': nonce,
                'sign': signature,
                'Content-Type': 'application/json'
            }

            try:
                resp = self.session.get(f"{self.BASE_URL}/api/v1/futures/trade/get_history_trades", headers=headers, params=params)
                resp.raise_for_status()
                response_json = resp.json()
                if 'error' in response_json:
                    logging.error(f"API Error: {response_json.get('error', 'Unknown error')}")
                    break

                page = response_json.get('data', {}).get('tradeList', [])
                if not page:
                    break
                for tr in page:
                    ctime = int(tr.get('ctime', 0))
                    if ctime > start_time:
                        trades.append(tr)
                skip += len(page)
                if len(page) < limit:
                    break
            except requests.exceptions.HTTPError as e:
                logging.error(f"HTTP error occurred: {e}")
                logging.error(f"Response content: {resp.text}")
                break
            except ValueError:
                logging.error(f"Invalid JSON response: {resp.text}")
                break
            except Exception as e:
                logging.error(f"An unexpected error occurred: {e}")
                break
        logging.info(f"Fetched {len(trades)} trades.")
        return trades


def load_state() -> int:
    """Load the last exported timestamp (ms since epoch) in UTC."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            data = json.load(f)
            return data.get('last_time', 0)

    # If no state file exists, load the default start time from the config file
    if os.path.exists(CREDENTIALS_FILE):
        with open(CREDENTIALS_FILE, 'r') as f:
            cfg = json.load(f)
            start_time = cfg.get('start_time')
            if start_time:
                try:
                    # Parse ISO 8601 format to Unix timestamp in milliseconds
                    dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                    start_time_ms = int(dt.timestamp() * 1000)
                    logging.info(f"No state file found. Using start_time from config: {start_time_ms}")
                    return start_time_ms
                except ValueError:
                    logging.error(f"Invalid start_time format in config: {start_time}")
                    raise

    # Default to epoch start if no config or state file
    logging.info("No state file or config start_time found. Defaulting to epoch start.")
    return 0


def save_state(last_time: int):
    """Save the most recent export timestamp in UTC."""
    with open(STATE_FILE, 'w') as f:
        json.dump({'last_time': last_time}, f)


def transform_trades(trades: list) -> list:
    """
    Convert raw trade dicts to rows matching TradeZella's Generic CSV.
    """
    rows = []
    for tr in sorted(trades, key=lambda x: int(x.get('ctime', 0))):
        ctime = int(tr.get('ctime', 0))
        dt = datetime.fromtimestamp(ctime / 1000, tz=timezone.utc)
        date_str = f"{dt.month}/{dt.day}/{dt.year % 100}"  # mm/dd/yy format
        time_str = dt.strftime("%H:%M:%S")  # 24-hour format

        # Ensure correct formatting for TradeZella
        rows.append({
            'Date': date_str,
            'Time': time_str,
            'Symbol': tr.get('symbol', ''),
            'Buy/Sell': tr.get('side', '').upper(),  # Ensure BUY/SELL is uppercase
            'Quantity': tr.get('qty', ''),
            'Price': tr.get('price', ''),
            'Spread': 'Crypto',  # Use 'Crypto' for Crypto trades
            'Expiration': '',  # Leave blank unless expiration is provided
            'Strike': '',  # Leave blank unless it's an options trade
            'Call/Put': '',  # Ignore unless it's an options trade
            'Commission': tr.get('fee', ''),
            'Fees': ''  # Leave blank if no additional fees
        })
    return rows


def main():
    parser = argparse.ArgumentParser(
        description="Export new Bitunix futures trades to TradeZella CSV"
    )
    parser.add_argument(
        '--output', default=None,
        help='Output CSV file (default: new_trades_<timestamp>.csv)'
    )
    args = parser.parse_args()

    try:
        api_key, secret_key = load_credentials()
        client = BitunixClient(api_key, secret_key)

        last_time = load_state()
        new_trades = client.fetch_trades(last_time)
        if not new_trades:
            logging.info("No new trades since last export.")
            return

        rows = transform_trades(new_trades)

        # Generate a default filename with a timestamp if not provided
        output_filename = args.output or f"new_trades_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        import csv
        with open(output_filename, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()
            writer.writerows(rows)

        latest_time = max(int(t['ctime']) for t in new_trades)
        save_state(latest_time)
        logging.info(f"Exported {len(new_trades)} new trades to {output_filename}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == '__main__':
    main()
