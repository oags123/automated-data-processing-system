#all imports files here 
from fetch import DataFetcher
from segments import RegressionSegments # Import the class
from quantreg import QuantileRegressionLines  # Import the class
import line_properties
import plot_chart
#from apscheduler.schedulers.background import BackgroundScheduler
import time
from datetime import datetime, timedelta, timezone
from regression_main import run_regression_lines, get_past_time_4h_iso8601


def process_all_intervals(symbol="BTC/USD", exchange_name="coinbase"):
    current_time = int(time.time() * 1000) # Current time in milliseconds
    start_time = get_past_time_4h_iso8601(current_time) # Subtract 2000 hours
    start_time_count = 0
    data_frames = {}

    for time_interval in ['1h', '30m', '5m', '1m']:  # Process intervals in hierarchical order
        try:
            # Create an instance of DataFetcher
            fetcher = DataFetcher(symbol, start_time, current_time, exchange_name, start_time_count, time_interval)
            # Fetch prices data
            df = fetcher.fetch_main()
            data_frames[time_interval] = df
            start_time = run_regression_lines(df, time_interval)  #returns start_time_lower_interval and plots the charts 
        except Exception as e:
            print(f"⚠️ Error processing {time_interval}: {e}")

    return data_frames


# Define symbols and the interval runner
symbols = ['BTC_USDT', 'ETH_USDT', 'XRP_USDT', 'SOL_USDT', 'ADA_USDT']
# Time intervals to process
intervals = ['4h', '30m', '5m', '1m']

def bootstrap_hierarchy(symbol):
    print(f"🔄 Bootstrapping {symbol}...")

def process_interval_job(interval):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Running {interval} job")
    time.sleep(2)  # Simulated work

def interval_runner():
    now = datetime.now()
    hour = now.hour
    minute = now.minute

    if hour % 4 == 0 and minute == 0:
        process_interval_job('4h')
        process_interval_job('30m')
        process_interval_job('5m')
        process_interval_job('1m')
    elif minute % 30 == 0:
        process_interval_job('30m')
        process_interval_job('5m')
        process_interval_job('1m')
    elif minute % 5 == 0:
        process_interval_job('5m')
        process_interval_job('1m')
    else:
        process_interval_job('1m')

# Initialize scheduler
scheduler = BackgroundScheduler()

# 🔹 ADD YOUR JOB HERE — BEFORE starting the scheduler!
scheduler.add_job(
    interval_runner,
    trigger='cron',
    minute='*',
    id='interval_dispatcher',
    max_instances=1,
    coalesce=True
)

# 🔹 Start everything
if __name__ == "__main__":
    print("🟡 Bootstrapping initial data for all symbols...")
    for symbol in symbols:
        bootstrap_hierarchy(symbol)

    print("✅ Bootstrap complete. Starting APScheduler...")
    try:
        scheduler.start()
        print("🚀 APScheduler started. Running jobs...")
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        print("🛑 Scheduler stopped.")
        scheduler.shutdown()

