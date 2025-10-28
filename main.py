import sqlite3
import time
from datetime import datetime, timedelta, timezone
import logging
import os
import threading
from flask import Flask, send_file, jsonify

# --- Import your helper functions ---
from helper import (
    update_fvg_table,
    check_and_insert_retest_gaps,
    trigger_trade,
    fetch_delta_ohlc,
    update_trade_status,
    TABLE_SCHEMAS
)

# --- Database Path ---
db_path = os.path.join(os.getcwd(), "bot2.db")
print(f"✅ Using DB Path: {db_path}")
if os.path.exists(db_path):
    os.remove(db_path)
    print("✅ Database file deleted successfully.")
else:
    print("⚠️ Database file not found.")

# --- SQLite DB Setup ---
conn = sqlite3.connect(db_path, check_same_thread=False)
cursor = conn.cursor()
# Ensure table exists
cursor.execute(TABLE_SCHEMAS["FairValueGaps"])
conn.commit()
conn.close()
# Define IST timezone
IST = timezone(timedelta(hours=5, minutes=30))

# --- Flask App for DB Download ---
app = Flask(__name__)

@app.route("/")
def home():
    return jsonify({
        "message": "Trading Bot is running",
        "db_download": "/download-db",
        "status": "OK"
    })

@app.route("/download-db")
def download_db():
    try:
        return send_file(db_path, as_attachment=True)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# --- Trading Bot Logic ---
def run_bot():
    symbol = "BTCUSD"
    time_offset = timedelta(seconds=0)

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

        df_1m = fetch_delta_ohlc(symbol, "1m", hours=1, rate_limit=0.1)
        check_and_insert_retest_gaps(symbol, db_path, df_1m)
        trigger_trade(symbol, db_path, df_1m)
        # update_trade_status(df_1m, symbol, db_path)

        # --- Sleep until next 1-min mark ---
        next_minute = (now.replace(second=0, microsecond=0) + timedelta(minutes=1))
        sleep_seconds = (next_minute - (datetime.now(IST) - time_offset)).total_seconds()

        print(f"Next 1-min cycle at: {next_minute}, sleeping for {sleep_seconds:.2f} seconds")
        time.sleep(max(0, sleep_seconds))


# --- Start Everything ---
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("🚀 Trading bot started.")

    # Run the bot in a background thread so Flask can also run
    bot_thread = threading.Thread(target=run_bot, daemon=True)
    bot_thread.start()

    # Run Flask web server (Railway uses the PORT env variable)
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
