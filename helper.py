import time
import pandas as pd
import requests
from datetime import datetime, timedelta,timezone
import numpy as np
import sqlite3
from pytz import timezone as tz
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
    if resolution != "1m":
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
    ohlc_df = fetch_delta_ohlc("BTCUSD", "5m", hours=24, rate_limit=0.2)
    
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
        """, (
            fvg["Symbol"], fvg["TimeFrame"], fvg["FVGStart"], fvg["FVGEnd"], fvg["Direction"]
        )).fetchone()
        
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
            IST = tz("Asia/Kolkata")

            # Convert ActiveTime (IST) → UTC
            active_time_ist = IST.localize(pd.to_datetime(fvg["ActiveTime"]))
            active_time_utc = active_time_ist.astimezone(timezone.utc)
            current_time_utc = datetime.now(timezone.utc)

            # Calculate total duration in minutes
            duration_min = int((current_time_utc - active_time_utc).total_seconds() // 60)

            # Round down to nearest timeframe interval (e.g., 5m)
            tf_minutes = int(timeframe.replace("m", ""))  # "5m" → 5
            rounded_duration = (duration_min // tf_minutes) * tf_minutes

            cur.execute("""
                UPDATE FairValueGaps
                SET Duration = ?
                WHERE Id = ?
            """, (rounded_duration, fvg["Id"]))

            # Check for fill condition
            recent_close = ohlc_df.iloc[-1]["Close"]

            # 🔻 Deactivate Bearish FVG if price closes above its end
            if fvg["Direction"] == "Bearish" and recent_close > fvg["FVGEnd"]:
                # Deactivate FVG
                cur.execute("UPDATE FairValueGaps SET IsActive = 0 WHERE Id = ?", (fvg["Id"],))
                # Deactivate related RetestGap entries
                cur.execute("UPDATE RetestGap SET IsActive = 0 WHERE FairValueGapId = ?", (fvg["Id"],))
            
            # 🔺 Deactivate Bullish FVG if price closes below its start
            elif fvg["Direction"] == "Bullish" and recent_close < fvg["FVGStart"]:
                # Deactivate FVG
                cur.execute("UPDATE FairValueGaps SET IsActive = 0 WHERE Id = ?", (fvg["Id"],))
                # Deactivate related RetestGap entries
                cur.execute("UPDATE RetestGap SET IsActive = 0 WHERE FairValueGapId = ?", (fvg["Id"],))


    conn.commit()
    conn.close()
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] ✅ FVG table updated successfully for {symbol}.")

def check_and_insert_retest_gaps(symbol,db_path: str):
    """
    Checks the latest 1-minute candle to see if it retests the latest active Fair Value Gap (FVG).
    If retest detected:
        - Updates FairValueGap.IsRetest = 1
        - Inserts record into RetestGap
    If the FVG becomes inactive, marks corresponding RetestGap rows as inactive.
    
    Parameters:
        db_path: str - Path to SQLite database.
        df_1m: pd.DataFrame - Must contain columns:
            ['OpenTime', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume']
    """

    df_1m = fetch_delta_ohlc(symbol, "1m", hours=1, rate_limit=0.1)
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Step 1: Fetch latest active FairValueGap (IsRetest=0, IsActive=1, TimeFrame=5m)
    cur.execute("""
        SELECT Id, Symbol, FVGStart, FVGEnd, Direction, TimeFrame, IsActive
        FROM FairValueGaps
        WHERE TimeFrame = '5m'
          AND IsActive = 1
          AND IsRetested = 0
          AND ActiveTime = (
              SELECT MAX(ActiveTime)
              FROM FairValueGaps
              WHERE TimeFrame = '5m'
                AND IsActive = 1
          );
    """)
    fvg = cur.fetchone()

    if not fvg:
        print("No active Fair Value Gap found.")
        conn.close()
        return

    fvg_id, symbol, fvg_start, fvg_end, direction, timeframe, is_active = fvg

    print(fvg)

    # Step 2: Filter the DataFrame for this symbol
    df_symbol = df_1m

    if df_symbol.empty:
        print(f"No 1-min data found for symbol {symbol}.")
        conn.close()
        return

    # Step 3: Take only the latest 1-minute candle
    latest_row = df_symbol.sort_values("OpenTime").iloc[-1]
    print(latest_row.head())

    # Step 4: Check the retest condition
    retest_detected = False
    if direction.lower() == "bullish" and latest_row["Low"] <= fvg_start:
        retest_detected = True
    elif direction.lower() == "bearish" and latest_row["High"] >= fvg_start:
        retest_detected = True

    # Step 5: If retest detected → update FairValueGap & insert into RetestGap
    if retest_detected:
        print(f"Retest detected for {symbol} at {latest_row['OpenTime']}")

        # Update FairValueGap.IsRetest
        cur.execute("""
            UPDATE FairValueGaps
            SET IsRetested = 1
            WHERE Id = ?
        """, (fvg_id,))

        # Insert new record into RetestGap
        cur.execute("""
            INSERT INTO RetestGap (
                Symbol, OpenTime, FairValueGap, TimeFrame, Direction, Type,
                Open, High, Low, Close, Volume, IsActive
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
        """, (
            symbol,
            latest_row["OpenTime"],
            fvg_id,
            timeframe,
            direction,
            "-",
            latest_row["Open"],
            latest_row["High"],
            latest_row["Low"],
            latest_row["Close"],
            latest_row["Volume"]
        ))

        conn.commit()

    # Step 6: If the FVG becomes inactive → deactivate its related RetestGap
    cur.execute("SELECT IsActive FROM FairValueGaps WHERE Id = ?", (fvg_id,))
    is_active_now = cur.fetchone()
    if is_active_now and is_active_now[0] == 0:
        cur.execute("""
            UPDATE RetestGap
            SET IsActive = 0
            WHERE FairValueGap = ?
        """, (fvg_id,))
        conn.commit()

    conn.close()