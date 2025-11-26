# all imports here
from fetch import DataFetcher
from segments import RegressionSegments
from quantreg import QuantileRegressionLines
import line_properties
import plot_chart
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd
import time
from datetime import datetime, timezone, timedelta
from regression_main import run_regression_lines, get_past_time_4h_iso8601



# ───────────────────────────────────────────────
# Global shared state
# ───────────────────────────────────────────────
symbol = "BTC/USD"
exchange_name = "coinbase"

data_frames = {}   # { '1h': df, '30m': df, '5m': df, '1m': df }
start_times = {}   # { '1h': start_time, '30m': start_time, '5m': start_time, '1m': start_time }


# ───────────────────────────────────────────────
# Bootstrap historical data for all intervals
# ───────────────────────────────────────────────
def process_all_intervals(symbol=symbol, exchange_name=exchange_name):
    global data_frames, start_times
    print(f"🟡 Bootstrapping historical data for {symbol} on {exchange_name}...")

    current_time = int(time.time() * 1000)
    start_time = get_past_time_4h_iso8601(current_time)
    start_time_count = 0  # start counter at 0 for the very first DataFetcher

    for time_interval in ['1h', '30m', '5m', '1m']:
        try:
            print(f"📊 Fetching {time_interval} interval data...")
            fetcher = DataFetcher(symbol, start_time, current_time, exchange_name, start_time_count, time_interval)
            df = fetcher.fetch_main()

            if df is None or df.empty:
                print(f"⚠️ No data fetched for {time_interval}. Skipping.")
                continue

            # Run regression and get start_time for next lower interval
            new_start_time = run_regression_lines(df, time_interval)

            # Store DataFrame and start_time
            data_frames[time_interval] = df
            start_times[time_interval] = new_start_time

            print(f"✅ Bootstrapped {time_interval}: {len(df)} rows | start_time: {new_start_time}")

            # Pass this start_time to the next lower interval
            start_time = new_start_time

        except Exception as e:
            print(f"⚠️ Error bootstrapping {time_interval}: {e}")

    print("🏁 Bootstrap complete.\n")


# ───────────────────────────────────────────────
# Continuous updater — runs at correct schedule
# ───────────────────────────────────────────────
def process_interval_job(interval):
    global data_frames, start_times

    try:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔁 Updating {interval} data...")

        df_old = data_frames.get(interval)
        if df_old is None or df_old.empty:
            print(f"⚠️ No existing data for {interval}. Skipping update.")
            return

        last_timestamp = df_old['date'].iloc[-1]
        current_time = int(time.time() * 1000)

        # Start count from current number of rows (for continuity)
        start_time_count = len(df_old)

        # Fetch only new candles since last timestamp
        fetcher = DataFetcher(symbol, last_timestamp, current_time, exchange_name, start_time_count, interval)
        df_new = fetcher.fetch_main()

        if df_new is None or df_new.empty:
            print(f"ℹ️ No new data for {interval}. Already up to date.")
            return

        # Merge and clean duplicates
        df_updated = (
            pd.concat([df_old, df_new])
            .drop_duplicates(subset=['date'])
            .sort_values('date')
            .reset_index(drop=True)
        )

        # Run regression and get new start time
        new_start_time = run_regression_lines(df_updated, interval)
        old_start_time = start_times.get(interval)

        # ──────────────────────────────
        # Trimming logic
        # ──────────────────────────────
        if interval == '1h':
            # Keep around 2000 rows; drop oldest 1 every hour if exceeded
            if len(df_updated) > 2000:
                df_updated = df_updated.iloc[1:].reset_index(drop=True)
                print(f"🕒 Dropped oldest 1 row from 1h interval.")
        else:
            # Drop rows before new start_time only if start_time changed
            if old_start_time != new_start_time:
                before_len = len(df_updated)
                df_updated = df_updated[df_updated['date'] >= new_start_time].reset_index(drop=True)
                after_len = len(df_updated)
                print(f"✂️ Trimmed {before_len - after_len} rows from {interval} (new start_time).")
            else:
                print(f"ℹ️ {interval} start_time unchanged — no trimming needed.")

        # Update global DataFrame and start_time
        data_frames[interval] = df_updated
        start_times[interval] = new_start_time

        print(f"✅ {interval} updated: {len(df_updated)} candles total.\n")

    except Exception as e:
        print(f"⚠️ Error updating {interval}: {e}\n")


# ───────────────────────────────────────────────
# Scheduler logic — determines which intervals run
# ───────────────────────────────────────────────
def interval_runner():
    now = datetime.now()
    hour = now.hour
    minute = now.minute

    if hour % 4 == 0 and minute == 0:
        process_interval_job('1h')
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


# ───────────────────────────────────────────────
# Initialize scheduler
# ───────────────────────────────────────────────
scheduler = BackgroundScheduler()
scheduler.add_job(
    interval_runner,
    trigger='cron',
    minute='*',
    id='interval_dispatcher',
    max_instances=1,
    coalesce=True
)


# ───────────────────────────────────────────────
# Main entrypoint
# ───────────────────────────────────────────────
if __name__ == "__main__":
    print("🟡 Bootstrapping initial price data for all time intervals...")
    process_all_intervals()
    print("✅ Bootstrap complete. Starting APScheduler...")

    try:
        scheduler.start()
        print("🚀 APScheduler started. Running jobs every minute...")
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        print("🛑 Scheduler stopped.")
        scheduler.shutdown()
