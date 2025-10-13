import time
import pandas as pd
import requests
from datetime import datetime, timedelta,timezone
import numpy as np
import sqlite3

# --- VWAP calculation ---
def add_vwap(df):
    typical_price = (df["High"] + df["Low"] + df["Close"]) / 3
    cumulative_vol = df["Volume"].cumsum()
    cumulative_vp = (typical_price * df["Volume"]).cumsum()
    df["VWAP"] = cumulative_vp / cumulative_vol
    return df

    
def fetch_delta_ohlc(symbol: str, resolution: str, hours: int, rate_limit: float = 0.3):
    """
    Fetch OHLC from Delta Exchange assuming timestamps in API are IST-based epoch seconds.
    Corrects the timestamps to proper IST datetime.

    Args:
        symbol (str): Trading symbol on Delta Exchange.
        resolution (str): Candle interval (e.g., '1m', '5m', '1h', '1d').
        hours (int): Number of past hours of data to fetch.
        rate_limit (float): Delay (in seconds) between paginated API calls.
    """
    base_url = "https://api.india.delta.exchange/v2/history/candles"
    headers = {'Accept': 'application/json'}

    now = datetime.now()
    end_time = int(now.timestamp())
    start_time = int((now - timedelta(hours=hours)).timestamp())

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

        # Rename columns
        raw = raw.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low',
                                  'close': 'Close', 'volume': 'Volume'})

        # Correct IST datetime
        raw['OpenTime'] = pd.to_datetime(raw['time'], unit='s') + pd.Timedelta(hours=5, minutes=30)
        raw['OpenTime'] = raw['OpenTime'].dt.strftime('%Y-%m-%d %H:%M:%S')

        raw = raw[['OpenTime', 'Open', 'High', 'Low', 'Close', 'Volume']]

        all_batches.append(raw)

        # Update end_time for pagination
        end_time = oldest_ts - 1

        if len(raw) < 2000:
            break

        time.sleep(rate_limit)

    if not all_batches:
        return pd.DataFrame(columns=['OpenTime', 'Open', 'High', 'Low', 'Close', 'Volume'])

    df = pd.concat(all_batches).drop_duplicates(subset='OpenTime').sort_values('OpenTime').reset_index(drop=True)
    # Assuming df is already sorted by OpenTime
    df = df.iloc[:-1].reset_index(drop=True)
    return add_vwap(df)

def update_fvg_table(db_path: str, symbol: str, timeframe: str = "5m"):
    """
    Incrementally updates FairValueGaps table in SQLite.
    - Adds new Bullish/Bearish FVGs.
    - Updates Duration and IsActive for existing FVGs.
    - Deactivates FVGs when price fills them.
    
    Parameters:
        db_path: str - path to SQLite DB
        ohlc_df: pd.DataFrame - must include ['OpenTime', 'Open', 'High', 'Low', 'Close', 'Volume', 'VWAP']
        symbol: str - symbol name (e.g. "BANKNIFTY")
        timeframe: str - e.g. "5m"
    """
    ohlc_df = fetch_delta_ohlc("BTCUSD", "5m", hours=24, rate_limit=0.3)
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Ensure table exists
    cur.execute("""
    CREATE TABLE IF NOT EXISTS FairValueGaps (
        Id INTEGER PRIMARY KEY AUTOINCREMENT,
        Symbol TEXT NOT NULL,
        ActiveTime TEXT,
        FVGStart REAL,
        FVGEnd REAL,
        Direction TEXT,
        FVGType TEXT,
        TimeFrame TEXT,
        Duration INTEGER,
        GapSize REAL,
        DistanceFromVWAP REAL,
        IsActive INTEGER DEFAULT 1,
        IsRetested INTEGER DEFAULT 0,
        Priority INTEGER
    );
    """)

    # --- 1️⃣ Fetch existing FVGs from DB ---
    existing_fvgs = pd.read_sql_query(
        "SELECT * FROM FairValueGaps WHERE Symbol = ? AND TimeFrame = ?",
        conn,
        params=(symbol, timeframe)
    )

    # --- 2️⃣ Detect new FVGs from data ---
    new_fvgs = []
    for i in range(2, len(ohlc_df)):
        prev2, prev1, curr = ohlc_df.iloc[i-2], ohlc_df.iloc[i-1], ohlc_df.iloc[i]

        # 🟩 Bullish FVG condition
        if prev2["High"] < curr["Low"]:
            gap_start = prev2["High"]
            gap_end = curr["Low"]
            gap_size_pct = round((gap_end - gap_start) / gap_start * 100,2)
            vwap_dist_pct = round((gap_start - curr["VWAP"]) / curr["VWAP"] * 100,2)

            new_fvgs.append({
                "Symbol": symbol,
                "ActiveTime": (pd.to_datetime(curr["OpenTime"]) + timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S"),
                "FVGStart": gap_start,
                "FVGEnd": gap_end,
                "Direction": "Bullish",
                "FVGType": None,
                "TimeFrame": timeframe,
                "Duration": 0,
                "GapSize": gap_size_pct,
                "DistanceFromVWAP": vwap_dist_pct,
                "IsActive": 1
            })

        # 🟥 Bearish FVG condition
        elif prev2["Low"] > curr["High"]:
            gap_start = curr["High"]
            gap_end = prev2["Low"]
            gap_size_pct = round((gap_end - gap_start) / gap_start * 100,2)
            vwap_dist_pct = round((gap_start - curr["VWAP"]) / curr["VWAP"] * 100,2)

            new_fvgs.append({
                "Symbol": symbol,
                "ActiveTime": (pd.to_datetime(curr["OpenTime"]) + timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S"),
                "FVGStart": gap_start,
                "FVGEnd": gap_end,
                "Direction": "Bearish",
                "FVGType": None,
                "TimeFrame": timeframe,
                "Duration": 0,
                "GapSize": gap_size_pct,
                "DistanceFromVWAP": vwap_dist_pct,
                "IsActive": 1
            })

    # --- 3️⃣ Insert new FVGs (avoid duplicates) ---
    for fvg in new_fvgs:
        exists = cur.execute("""
            SELECT 1 FROM FairValueGaps 
            WHERE Symbol=? AND TimeFrame=? AND FVGStart=? AND FVGEnd=? AND Direction=? 
        """, (symbol, timeframe, fvg["FVGStart"], fvg["FVGEnd"], fvg["Direction"])).fetchone()
        
        if not exists:
            cur.execute("""
                INSERT INTO FairValueGaps 
                (Symbol, ActiveTime, FVGStart, FVGEnd, Direction, FVGType, TimeFrame, Duration, GapSize, DistanceFromVWAP, IsActive)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                fvg["Symbol"], fvg["ActiveTime"], fvg["FVGStart"], fvg["FVGEnd"],
                fvg["Direction"], fvg["FVGType"], fvg["TimeFrame"], fvg["Duration"],
                fvg["GapSize"], fvg["DistanceFromVWAP"], fvg["IsActive"]
            ))

    # --- 4️⃣ Update Duration and Deactivate FVGs if filled ---
    for _, fvg in existing_fvgs.iterrows():
        if fvg["IsActive"] == 1:
            # Increment Duration for active FVG
            cur.execute("""
                UPDATE FairValueGaps
                SET Duration = Duration + 5
                WHERE Id = ?
            """, (fvg["Id"],))

            # Check for fill condition
            recent_close = ohlc_df.iloc[-1]["Close"]

            # 🔻 Deactivate Bearish FVG if price closes above its end
            if fvg["Direction"] == "Bearish" and recent_close > fvg["FVGEnd"]:
                cur.execute("UPDATE FairValueGaps SET IsActive = 0 WHERE Id = ?", (fvg["Id"],))
            
            # 🔺 Deactivate Bullish FVG if price closes below its start
            elif fvg["Direction"] == "Bullish" and recent_close < fvg["FVGStart"]:
                cur.execute("UPDATE FairValueGaps SET IsActive = 0 WHERE Id = ?", (fvg["Id"],))

    conn.commit()
    conn.close()
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] ✅ FVG table updated successfully for {symbol}.")

