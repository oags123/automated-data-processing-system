import ccxt
import time
import pandas as pd
from datetime import datetime

#esta class es para hacer el fetch de los precios nueva para hacer el fetch desde USA Septiembre 15 2025

class DataFetcher:
    def __init__(self, symbol, start_time, current_time, exchange_name, start_time_count, time_interval):
        # Initialize the exchange client (e.g., Coinbase)
        self.exchange = getattr(ccxt, exchange_name)()  # Dynamically initialize the exchange
        self.symbol = symbol
        self.start_time = self.exchange.parse8601(start_time)  # Convert ISO format to ms
        self.current_time = current_time
        self.start_time_count = start_time_count
        self.time_interval = time_interval
        
    # Function to calculate the difference between start_time and current_time in minutes
    def get_time_diff_in_minutes(self):
        return (self.current_time - self.start_time) // 60000  # Difference in minutes

    # Function to fetch data for a given time window
    def fetch_data(self, limit=300):
        ohlcv_data = self.exchange.fetch_ohlcv(self.symbol, self.time_interval, self.start_time, limit=limit)
        return ohlcv_data

    def get_interval_ms(self):
        mapping = {
            '1m': 60_000,
            '5m': 300_000,
            '15m': 900_000,
            '30m': 1_800_000,
            '1h': 3_600_000,
            '6h': 21_600_000,
            '1d': 86_400_000,
        }
        return mapping.get(self.time_interval, 60_000)

    def process_data(self, ohlcv_data):
        # Convert the OHLCV data into a DataFrame
        df = pd.DataFrame(ohlcv_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        
        # Add 'time_count' column starting from start_time_count and incrementing by 1 for each row
        df['time_count'] = range(self.start_time_count, self.start_time_count + len(df))
        
        # Add 'date_time_iso' column for human-readable date-time
        df['date_time_iso'] = df['timestamp'].apply(lambda x: datetime.utcfromtimestamp(x / 1000).strftime('%Y-%m-%d %H:%M:%S'))
        
        # Add 'date' column (just the date part of the timestamp)
        df['date'] = df['timestamp']
        
        # Reorder columns to match the desired structure
        df = df[['time_count', 'date', 'date_time_iso', 'open', 'high', 'low', 'close', 'volume']]

        return df

    # Function to fetch data in chunks or once depending on the time difference
    def fetch_main(self):
        time_diff = self.get_time_diff_in_minutes()
        ohlcv_data = []

        if time_diff > 300:
            # If the time difference exceeds 300 minutes, fetch in multiple calls
            while self.start_time < self.current_time:
                # Fetch data for a chunk (300 candles per request)
                ohlcv_chunk = self.fetch_data(300)
                ohlcv_data.extend(ohlcv_chunk)
    
                # Update the start_time for the next request (next minute after the last fetched data)
                if ohlcv_chunk:
                    last_timestamp = ohlcv_chunk[-1][0]
                    #self.start_time = last_timestamp + 60000  # Set the next start time to one minute after the last candle
                    self.start_time = last_timestamp + self.get_interval_ms()

    
                # Sleep for a second to avoid hitting the rate limit
                time.sleep(1)
        else:
            # If the time span is less than or equal to 300 minutes, fetch data once
            ohlcv_data = self.fetch_data(300)

        df = self.process_data(ohlcv_data)

        return df

