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
    #if resolution != "1m":
        #df = df.iloc[:-1].reset_index(drop=True)
    return add_vwap(df)

def update_fvg_table(db_path: str, symbol: str, timeframe: str = "5m", ohlc_df=None):
    """
    Updates FairValueGaps table based on the latest closed OHLC data passed in.
    - ActiveTime = candle close time (OpenTime + timeframe)
    - Updates Duration and IsActive
    - Deactivates FVGs if price fills them
    """

    if ohlc_df is None:
        # fallback: fetch latest 24h data
        ohlc_df = fetch_delta_ohlc(symbol, timeframe, hours=24, rate_limit=0.2)

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
        Priority INTEGER,
        LastModifiedDate TIMESTAMP
    );
    """)

    # Fetch existing FVGs
    existing_fvgs = pd.read_sql_query(
        "SELECT * FROM FairValueGaps WHERE Symbol = ? AND TimeFrame = ?",
        conn,
        params=(symbol, timeframe)
    )

    # Detect new FVGs from latest candles
    new_fvgs = []
    for i in range(2, len(ohlc_df)):
        prev2, prev1, curr = ohlc_df.iloc[i-2], ohlc_df.iloc[i-1], ohlc_df.iloc[i]

        # Bullish FVG
        if prev2["High"] < curr["Low"]:
            new_fvgs.append({
                "Symbol": symbol,
                "ActiveTime": (pd.to_datetime(curr["OpenTime"]) + timedelta(minutes=int(timeframe.replace("m","")))).strftime("%Y-%m-%d %H:%M:%S"),
                "FVGStart": prev2["High"],
                "FVGEnd": curr["Low"],
                "Direction": "Bullish",
                "FVGType": None,
                "TimeFrame": timeframe,
                "Duration": 0,
                "GapSize": round((curr["Low"] - prev2["High"])/prev2["High"]*100,2),
                "DistanceFromVWAP": round((prev2["High"] - curr["VWAP"])/curr["VWAP"]*100,2),
                "IsActive": 1
            })

        # Bearish FVG
        elif prev2["Low"] > curr["High"]:
            new_fvgs.append({
                "Symbol": symbol,
                "ActiveTime": (pd.to_datetime(curr["OpenTime"]) + timedelta(minutes=int(timeframe.replace("m","")))).strftime("%Y-%m-%d %H:%M:%S"),
                "FVGStart": curr["High"],
                "FVGEnd": prev2["Low"],
                "Direction": "Bearish",
                "FVGType": None,
                "TimeFrame": timeframe,
                "Duration": 0,
                "GapSize": round((prev2["Low"] - curr["High"])/curr["High"]*100,2),
                "DistanceFromVWAP": round((curr["High"] - curr["VWAP"])/curr["VWAP"]*100,2),
                "IsActive": 1
            })

    # Insert new FVGs (skip duplicates)
    for fvg in new_fvgs:
        exists = cur.execute("""
            SELECT 1 FROM FairValueGaps 
            WHERE Symbol=? AND TimeFrame=? AND FVGStart=? AND FVGEnd=? AND Direction=?
        """, (fvg["Symbol"], fvg["TimeFrame"], fvg["FVGStart"], fvg["FVGEnd"], fvg["Direction"])).fetchone()
        if not exists:
            cur.execute("""
                INSERT INTO FairValueGaps 
                (Symbol, ActiveTime, FVGStart, FVGEnd, Direction, FVGType, TimeFrame, Duration, GapSize, DistanceFromVWAP, IsActive,LastModifiedDate)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)
            """, (
                fvg["Symbol"], fvg["ActiveTime"], fvg["FVGStart"], fvg["FVGEnd"],
                fvg["Direction"], fvg["FVGType"], fvg["TimeFrame"], fvg["Duration"],
                fvg["GapSize"], fvg["DistanceFromVWAP"], fvg["IsActive"],datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            ))

    # Update Duration and deactivate if filled (use last candle close)
    recent_close = ohlc_df.iloc[-1]["Close"]
    IST = tz("Asia/Kolkata")

    for _, fvg in existing_fvgs.iterrows():
        if fvg["IsActive"] == 1:
            active_time_ist = IST.localize(pd.to_datetime(fvg["ActiveTime"]))
            duration_min = int((datetime.now(IST) - active_time_ist).total_seconds() // 60)
            tf_minutes = int(timeframe.replace("m",""))
            rounded_duration = (duration_min // tf_minutes) * tf_minutes

            cur.execute("UPDATE FairValueGaps SET Duration=? WHERE Id=?", (rounded_duration, fvg["Id"]))

            # Deactivate based on last closed candle
            if fvg["Direction"] == "Bearish" and recent_close > fvg["FVGEnd"]:
                cur.execute("UPDATE FairValueGaps SET IsActive=0 WHERE Id=?", (fvg["Id"],))
                cur.execute("UPDATE RetestGap SET IsActive=0 WHERE FairValueGap=?", (fvg["Id"],))

            elif fvg["Direction"] == "Bullish" and recent_close < fvg["FVGStart"]:
                cur.execute("UPDATE FairValueGaps SET IsActive=0 WHERE Id=?", (fvg["Id"],))
                cur.execute("UPDATE RetestGap SET IsActive=0 WHERE FairValueGap=?", (fvg["Id"],))

    conn.commit()
    conn.close()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ FVG table updated for {symbol}")

def check_and_insert_retest_gaps(symbol,db_path: str,df_1m = None):
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

    #df_1m = fetch_delta_ohlc(symbol, "1m", hours=1, rate_limit=0.1)
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Step 1: Fetch latest active FairValueGap (IsRetest=0, IsActive=1, TimeFrame=5m)
    cur.execute("""
        SELECT Id, Symbol, ActiveTime,FVGStart, FVGEnd, Direction, TimeFrame, IsActive
        FROM FairValueGaps
        WHERE TimeFrame = '5m'
          AND IsActive = 1
          AND IsRetested = 0
          AND Duration <> '0'
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

    fvg_id, symbol, active_time ,fvg_start, fvg_end, direction, timeframe, is_active = fvg

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
    if direction.lower() == "bullish" and latest_row["Low"] <= fvg_end and latest_row["OpenTime"]>active_time:
        retest_detected = True
    elif direction.lower() == "bearish" and latest_row["High"] >= fvg_start and latest_row["OpenTime"]>active_time :
        retest_detected = True

    # Step 5: If retest detected → update FairValueGap & insert into RetestGap
    if retest_detected:
        print(f"Retest detected for {symbol} at {latest_row['OpenTime']}")

        # Update FairValueGap.IsRetest
        cur.execute("""
            UPDATE FairValueGaps
            SET IsRetested = 1,
                LastModifiedDate = CURRENT_TIMESTAMP
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
            float(latest_row["Volume"]) if latest_row["Volume"] is not None else 0
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

def trigger_trade(symbol,db_path: str, df_1m: pd.DataFrame):
    """
    Checks for trade trigger conditions and inserts a new trade entry if triggered.
    """
    if df_1m.empty:
        print("No data in DataFrame.")
        return

    # Get latest candle
    latest_candle = df_1m.iloc[-1]
    latest_close = latest_candle["Close"]
    latest_time = latest_candle["OpenTime"]

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Step 1: Fetch the latest untraded RetestGap
    cur.execute("""
        SELECT Id, Direction, High, Low, FairValueGap
        FROM RetestGap
        WHERE IsActive = 1 AND IsTraded = 0
        ORDER BY OpenTime DESC
        LIMIT 1
    """)
    retest_gap = cur.fetchone()

    if not retest_gap:
        print("No active Retest Found found.")
        conn.close()
        return

    retest_id, direction, rg_high, rg_low, fvg_id = retest_gap

    # Step 2: Check trigger conditions
    trigger = False
    if direction.lower() == "bullish" and latest_close > rg_high:
        trigger = True
    elif direction.lower() == "bearish" and latest_close < rg_low:
        trigger = True

    if not trigger:
        print("No trade trigger condition met.")
        conn.close()
        return

    # Step 3: Get FVG_END from FairValueGaps table for stop loss
    cur.execute("SELECT FVGStart,FVGEnd FROM FairValueGaps WHERE Id = ?", (fvg_id,))
    result = cur.fetchone()
    if not result:
        print("No corresponding FairValueGap found.")
        conn.close()
        return

    fvg_start = result[0]
    fvg_end = result[1]

    # Step 4: Calculate Initial StopLoss and Target
    if direction.lower() == "bullish":
        initial_stoploss = round(fvg_start - (fvg_start * 0.00005), 2)
        stoploss_points = round(latest_close - initial_stoploss, 2)
        initial_target = round(latest_close + (3 * stoploss_points), 2)
    else:
        initial_stoploss = round(fvg_end + (fvg_end * 0.00005), 2)
        stoploss_points = round(initial_stoploss - latest_close, 2)
        initial_target = round(latest_close - (3 * stoploss_points), 2)

    # Step 5: Insert Trade in Trades Table
    cur.execute("""
        INSERT INTO Trades 
        (Symbol,EntryTime, RetestGap, EntryCandle, Open, High, Low, Close, Volume, 
         Direction, CandleType, Stratagy, Lot, RemainingLot, 
         IntialStopLoss, IntialTarget, ModifiedStopLoss, ModifiedTarget, LastModifiedDate)
        VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        symbol,
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        retest_id,
        latest_time,
        latest_candle["Open"],
        latest_candle["High"],
        latest_candle["Low"],
        latest_candle["Close"],
        float(latest_candle["Volume"]) if latest_candle["Volume"] is not None else 0,
        direction.capitalize(),
        "",
        "FVG",
        200,
        200,
        initial_stoploss,
        initial_target,
        None,
        None,
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    ))

    #Insert Trade in TradesStatus Table
    # ✅ Get the last inserted Trade ID
    trade_id = cur.lastrowid

    # Step 6: Insert corresponding entry in TradeStatus Table
    cur.execute("""
        INSERT INTO TradeStatus
        (Symbol,Quantity, EntryTime, Trade, EntryPrice, Status)
        VALUES (?,?, ?, ?, ?, ?)
    """, (
        symbol,
        200,
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        trade_id,  # <-- foreign key reference to Trades.Id
        latest_candle["Close"],
        "Running"
    ))

    # Step 6: Optionally mark RetestGap as traded
    cur.execute("UPDATE RetestGap SET IsTraded = 1 WHERE Id = ?", (retest_id,))

    conn.commit()
    conn.close()

    print(f"✅ Trade triggered and inserted for {direction.upper()} RetestGap ID {retest_id}")

def update_trade_status(df_1m: pd.DataFrame, symbol: str, db_path: str):
    """
    Updates Trades and logs TradeStatus for PartialBooked, SL, TG trades.
    Calculates ExitPrice and PnL for TradeStatus and updates IsOpen when trade is closed.
    """

    if df_1m.empty:
        print("⚠️ df_1m is empty. Skipping update.")
        return

    latest_candle = df_1m.iloc[-1]
    high = float(latest_candle["High"])
    low = float(latest_candle["Low"])
    close = float(latest_candle["Close"])

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Fetch trades that have remaining lots
    cur.execute("""
        SELECT Id, EntryTime, RetestGap, EntryCandle, Open, High, Low, Close, Volume,
               Direction, CandleType, Stratagy, Lot, RemainingLot, IntialStopLoss, IntialTarget,
               ModifiedStopLoss, ModifiedTarget
        FROM Trades
        WHERE Direction IS NOT NULL AND RemainingLot > 0
    """)
    trades = cur.fetchall()

    if not trades:
        print("no trade found")
        return

    for trade in trades:
        (trade_id, _, _, _, entry_open, entry_high, entry_low, entry_close, volume,
         direction, candle_type, stratagy, lot, remaining_lot, sl, tg, mod_sl, mod_tg) = trade

        total_lot = remaining_lot if remaining_lot else lot
        status = "Active"
        new_remaining_lot = total_lot
        partial_executed = False
        booked_qty = 0
        exit_price = None
        pnl = 0

        # --- Bullish ---
        if direction == "Bullish":
            if low < sl:
                status = "SL"
                new_remaining_lot = 0
                exit_price = sl
                pnl = (exit_price - close) * total_lot
            elif high >= tg:
                status = "TG"
                new_remaining_lot = 0
                exit_price = tg
                pnl = (exit_price - close) * total_lot
            elif high >= entry_open + ((tg - entry_open) * 2 / 3):
                status = "PartialBooked"
                booked_qty = total_lot / 2
                new_remaining_lot = total_lot - booked_qty
                exit_price = entry_open + ((tg - entry_open) * 2 / 3)
                pnl = (exit_price - close) * booked_qty
                partial_executed = True

        # --- Bearish ---
        elif direction == "Bearish":
            if high > sl:
                status = "SL"
                new_remaining_lot = 0
                exit_price = sl
                pnl = (close - exit_price) * total_lot
            elif low <= tg:
                status = "TG"
                new_remaining_lot = 0
                exit_price = tg
                pnl = (close - exit_price) * total_lot
            elif low <= entry_open - ((entry_open - tg) * 2 / 3):
                status = "PartialBooked"
                booked_qty = total_lot / 2
                new_remaining_lot = total_lot - booked_qty
                exit_price = entry_open - ((entry_open - tg) * 2 / 3)
                pnl = (close - exit_price) * booked_qty
                partial_executed = True

        # --- Update Trades RemainingLot ---
        cur.execute("""
            UPDATE Trades
            SET RemainingLot = ?, LastModifiedDate = ?
            WHERE Id = ?
        """, (new_remaining_lot, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), trade_id))

        # --- Insert or Update TradeStatus ---
        # Only update/insert for PartialBooked or closed trade
        if status in ["PartialBooked", "SL", "TG"]:
            # Check if PartialBooked record exists
            cur.execute("""
                SELECT Id FROM TradeStatus
                WHERE Trade = ? AND Status = ?
            """, (trade_id, "PartialBooked"))
            existing = cur.fetchone()

            quantity_to_log = booked_qty if status == "PartialBooked" else total_lot
            is_open_flag = 1 if new_remaining_lot > 0 else 0

            if status == "PartialBooked" and existing:
                # Update existing record for PartialBooked
                cur.execute("""
                    UPDATE TradeStatus
                    SET Quantity = Quantity + ?, ExitPrice = ?, Pnl = ?, IsOpen = ?, Timestamp = ?
                    WHERE Id = ?
                """, (booked_qty, exit_price, pnl, is_open_flag, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), existing[0]))
                print(trade_id)
            else:
                # Insert new record
                cur.execute("""
                    INSERT INTO TradeStatus
                    (Symbol,Trade, Status, Quantity, EntryPrice, IsOpen, LastModifiedDate)
                    VALUES (?,?, ?, ?, ?,?, ?)
                """, (symbol,trade_id, status, quantity_to_log, entry_close,  is_open_flag, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")))
                print(trade_id)

            if partial_executed:
                print(f"🔸 Trade {trade_id} partially booked: {booked_qty} booked, {new_remaining_lot} remaining")
            else:
                print(f"❌ Trade {trade_id} closed with status {status}, PnL: {pnl}")

    conn.commit()
    conn.close()
    print(f"✅ Trade statuses updated for {symbol}")
