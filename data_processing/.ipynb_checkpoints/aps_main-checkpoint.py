# aps_main.py

from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import pytz


from bootstrap import bootstrap_hierarchy  # ⬅️ Import bootstrap logic this is the initial at start run every interval

# List of financial symbols and intervals
symbols = ['BTC/USDT', 'ETH/USDT', 'XRP/USDT', 'SOL/USDT', 'ADA/USDT']
intervals = ['1m', '5m', '30m', '4h']

def process_data(symbol, interval):
    # ✅ Replace with actual fetching + regression logic
    print(f"[{datetime.now()}] Processing {symbol} at interval {interval}")

def process_interval_job(interval):
    for symbol in symbols:
        process_data(symbol, interval)

# Initialize scheduler
scheduler = BlockingScheduler(timezone=pytz.utc)

# Schedule recurring interval jobs
scheduler.add_job(lambda: process_interval_job('1m'), trigger='cron', minute='*')
scheduler.add_job(lambda: process_interval_job('5m'), trigger='cron', minute='*/5')
scheduler.add_job(lambda: process_interval_job('30m'), trigger='cron', minute='*/30')
scheduler.add_job(lambda: process_interval_job('4h'), trigger='cron', hour='*/4', minute=0)

if __name__ == "__main__":
    print("🟡 Bootstrapping initial data for all symbols...")
    for symbol in symbols:
        bootstrap_hierarchy(symbol)

    print("✅ Bootstrap complete. Starting APScheduler...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("🛑 Scheduler stopped.")

