import sqlite3
import time
from datetime import datetime,timedelta,timezone
import logging
from helper import update_fvg_table


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
    time_offset = timedelta(seconds=173)  # system clock leads by 173 seconds

    while True:
        update_fvg_table(db_path, "BTCUSD", timeframe="5m")

        # Get corrected time (subtract offset)
        now = datetime.now(IST) - time_offset
        print("Corrected time:", now)

        # Round up to next 5-minute mark
        next_minute = (now.minute // 5 + 1) * 5
        next_run = now.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=next_minute)

        # Handle hour rollover
        if next_minute >= 60:
            next_run = next_run.replace(hour=now.hour + 1, minute=0)

        # Compute sleep time based on corrected time
        sleep_seconds = (next_run - (datetime.now(IST) - time_offset)).total_seconds()

        print(f"Next run at: {next_run}, sleeping for {sleep_seconds:.2f} seconds")
        time.sleep(sleep_seconds)
        

if __name__ == "__main__":
    logging.info("Trading bot started.")
    run_bot()
