import sqlite3
import time
from datetime import datetime,timedelta,timezone
import logging
from helper import update_fvg_table,check_and_insert_retest_gaps,trigger_trade,fetch_delta_ohlc,update_trade_status


db_path =r"""C:\Users\sit456\Desktop\JyBot\bot.db"""
# --- SQLite DB Setup ---
#conn = sqlite3.connect("trades.db", check_same_thread=False)
conn = sqlite3.connect(db_path, check_same_thread=False)

#conn = sqlite3.connect("/data/trades.db", check_same_thread=False)
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS FairValueGaps (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    Symbol TEXT NOT NULL,
    ActiveTime TEXT,              -- Timestamp or datetime (ISO 8601 string)
    FVGStart REAL,                -- Price level (start)
    FVGEnd REAL,                  -- Price level (end)
    Direction TEXT,               -- 'Bullish' / 'Bearish'
    FVGType TEXT,                 -- e.g. 'Standard', 'Extended', etc.
    TimeFrame TEXT,               -- e.g. '1m', '5m', '1h', '1D'
    Duration INTEGER,             -- Duration in candles or minutes
    GapSize REAL,                 -- Percentage value (e.g. 1.25 for 1.25%)
    DistanceFromVWAP REAL,        -- Percentage value (e.g. 0.75 for 0.75%)
    IsActive INTEGER DEFAULT 1,   -- 1 = Active, 0 = Inactive
    IsRetested INTEGER DEFAULT 0, -- 1 = Retested, 0 = Not Retested
    Priority INTEGER              -- Priority ranking or weight
)
""")
conn.commit()

# Define IST timezone
IST = timezone(timedelta(hours=5, minutes=30))


def run_bot():
    symbol = "BTCUSD"
    time_offset = timedelta(seconds=173)  # system clock leads by 173 seconds

    while True:
        now = datetime.now(IST) - time_offset
        current_minute = now.minute
        current_second = now.second

        # --- Run FVG update every 5 minutes ---
        if current_minute % 5 == 0 and current_second < 5:
            print(f"\n[{now.strftime('%H:%M:%S')}] Running update_fvg_table()")
            update_fvg_table(db_path, symbol, timeframe="5m")

        # --- Run retest check every 1 minute ---
        print(f"[{now.strftime('%H:%M:%S')}] Running check_and_insert_retest_gaps()")
        #df_1m = fetch_latest_1min_data(symbol)  # <-- you must define this function to get latest 1-min candle
        df_1m = fetch_delta_ohlc(symbol, "1m", hours=1, rate_limit=0.1)
        check_and_insert_retest_gaps(symbol,db_path,df_1m)
        trigger_trade(symbol,db_path,df_1m)
        #update_trade_status(df_1m,symbol,db_path)
        # --- Calculate next 1-minute mark ---
        next_minute = (now.replace(second=0, microsecond=0) + timedelta(minutes=1))
        sleep_seconds = (next_minute - (datetime.now(IST) - time_offset)).total_seconds()

        print(f"Next 1-min cycle at: {next_minute}, sleeping for {sleep_seconds:.2f} seconds")
        time.sleep(max(0, sleep_seconds))
        

if __name__ == "__main__":
    logging.info("Trading bot started.")
    run_bot()
