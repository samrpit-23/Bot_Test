import time
import pandas as pd
import requests
from datetime import datetime, timedelta
import numpy as np

def fetch_delta_ohlc(symbol: str, resolution: str, months: int, rate_limit: float = 0.3):
    """
    Fetch OHLC from Delta Exchange assuming timestamps in API are IST-based epoch seconds.
    Corrects the timestamps to proper IST datetime.
    """
    base_url = "https://api.india.delta.exchange/v2/history/candles"
    headers = {'Accept': 'application/json'}

    now = datetime.now()
    end_time = int(now.timestamp())
    start_time = int((now - timedelta(days=months * 30)).timestamp())

    all_batches = []

    while True:
        params = {'resolution': resolution, 'symbol': symbol, 'start': start_time, 'end': end_time}
        resp = requests.get(base_url, headers=headers, params=params)
        if resp.status_code != 200:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")

        result = resp.json().get('result', [])
        if not result:
            break

        raw = pd.DataFrame(result, columns=['time', 'open', 'high', 'low', 'close', 'volume'])

        if raw.empty:
            break

        # --- Compute oldest timestamp for pagination ---
        oldest_ts = int(raw['time'].min())

        # Rename numeric columns
        raw = raw.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'})

        # Correct IST datetime (API timestamp is already IST)
        # Epoch seconds in IST → subtract 5:30 to get proper IST datetime
        raw['OpenTime'] = pd.to_datetime(raw['time'], unit='s') + pd.Timedelta(hours=5, minutes=30)
        raw['OpenTime'] = raw['OpenTime'].dt.strftime('%Y-%m-%d %H:%M:%S')

        raw = raw[['OpenTime', 'Open', 'High', 'Low', 'Close', 'Volume']]

        all_batches.append(raw)

        end_time = oldest_ts - 1

        if len(raw) < 2000:
            break

        time.sleep(rate_limit)

    if not all_batches:
        return pd.DataFrame(columns=['OpenTime', 'Open', 'High', 'Low', 'Close', 'Volume'])

    df = pd.concat(all_batches).drop_duplicates(subset='OpenTime').sort_values('OpenTime').reset_index(drop=True)
    return df



