import requests
import sqlite3
import time
from datetime import datetime
import logging

# --- Logging Setup ---
logging.basicConfig(
    filename='bot.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- SQLite DB Setup ---
#conn = sqlite3.connect("trades.db", check_same_thread=False)
conn = sqlite3.connect("trades.db", check_same_thread=False)

#conn = sqlite3.connect("/data/trades.db", check_same_thread=False)
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT,
    signal TEXT,
    price REAL,
    timestamp TEXT
)
""")
conn.commit()

API_URL = "https://api.delta.exchange/v2/tickers/BTCUSD"

def fetch_price():
    try:
        response = requests.get(API_URL, timeout=5)
        response.raise_for_status()
        data = response.json()
        return float(data['result']['mark_price'])
    except Exception as e:
        logging.error(f"Error fetching price: {e}")
        return None

def strategy(price):
    # Simple placeholder strategy
    if price < 60000:
        return "BUY"
    elif price > 119000:
        return "SELL"
    return None

def run_bot():
    while True:
        price = fetch_price()
        if price:
            signal = strategy(price)
            logging.info(f"Price: {price}, Signal: {signal}")

            if signal:
                cursor.execute(
                    "INSERT INTO trades (symbol, signal, price, timestamp) VALUES (?, ?, ?, ?)",
                    ("BTCUSD", signal, price, datetime.now().isoformat())
                )
                conn.commit()
                logging.info(f"Trade stored: {signal} at {price}")

        time.sleep(60)  # Wait 1 minute

if __name__ == "__main__":
    logging.info("Trading bot started.")
    run_bot()
