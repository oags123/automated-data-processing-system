import ccxt
import pandas as pd
from datetime import datetime, timedelta, timezone

def get_past_time_4h_iso8601():
    # Get the current UTC time
    now_utc = datetime.now(timezone.utc)
    # Subtract 2000 hours
    past_time = now_utc - timedelta(hours=2000)
    # Format the time in ISO 8601 and add 'Z'
    return past_time.strftime('%Y-%m-%dT%H:%M:%SZ')

def increment_timestamp(last_timestamp_str, interval):
    """
    Increments a given timestamp string by a specified interval.
    
    Parameters:
    - last_timestamp_str (str): The last timestamp in "YYYY-MM-DD HH:MM:SS" format.
    - interval (str): The interval to increment ('1m', '5m', '30m', '4h').

    Returns:
    - str: The incremented timestamp in "YYYY-MM-DDTHH:MM:SSZ" format.
    """
    # Convert string timestamp to datetime object
    last_datetime = pd.to_datetime(last_timestamp_str, format="%Y-%m-%d %H:%M:%S")

    # Define the interval mapping
    interval_mapping = {
        "1m": pd.Timedelta(minutes=1),
        "5m": pd.Timedelta(minutes=5),
        "30m": pd.Timedelta(minutes=30),
        "4h": pd.Timedelta(hours=4)
    }

    # Increment by the specified interval
    if interval in interval_mapping:
        new_datetime = last_datetime + interval_mapping[interval]
    else:
        raise ValueError("Invalid interval. Choose from '1m', '5m', '30m', '4h'.")

    # Convert to required format "YYYY-MM-DDTHH:MM:SSZ"
    return new_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")

# Example usage
#last_timestamp = "2025-02-12 01:17:00"

# Get updated timestamps for different intervals
#updated_1m = increment_timestamp(last_timestamp, "1m")
#updated_5m = increment_timestamp(last_timestamp, "5m")
#updated_30m = increment_timestamp(last_timestamp, "30m")
#updated_4h = increment_timestamp(last_timestamp, "4h")

#print("1-minute update:", updated_1m)  # "2025-02-12T01:18:00Z"
#print("5-minute update:", updated_5m)  # "2025-02-12T01:22:00Z"
#print("30-minute update:", updated_30m)  # "2025-02-12T01:47:00Z"
#print("4-hour update:", updated_4h)  # "2025-02-12T05:17:00Z"





# start_time is only required when time_interval is not equal to '4h' 
class FetchPrices:
    def __init__(self, time_interval, symbol, start_time_iso, start_time_count):
        """
        Initialize the FetchPrices class.
        - If time_interval is '4h', start_time is automatically set to a calculated past ISO8601 timestamp.
        - For other time intervals, start_time must be provided or remain None.
        
        Args:
            time_interval (str): The time interval (e.g., '4h', '30m', '5m', '1m').
            start_time (str, optional): The starting time in ISO8601 format for fetching prices.
        """
        valid_intervals = ['4h', '30m', '5m', '1m']  

        if time_interval not in valid_intervals:
            raise ValueError(f"Unsupported time interval: {time_interval}")

        self.time_interval = time_interval #'4h', '30m', '5m', '1m'
        
        end_time_iso = self.get_time_iso8601()
        binance = ccxt.binanceusdm()  # Binance US Dollar Margin Futures
        self.binance = binance
        self.start_time = binance.parse8601(start_time_iso)
        self.end_time = binance.parse8601(end_time_iso)
        
        self.interval_minutes = None
        self.interval_milliseconds = None

        self.symbol = symbol
        self.start_time_count = start_time_count
        self.set_interval_variables()
        
    def set_interval_variables(self):
        """
        Assign interval variables based on the provided time_interval.
        """
        intervals = {
            '4h': (240, 14400000),
            '30m': (30, 1800000),
            '5m': (5, 300000),
            '1m': (1, 60000)
        }
        if self.time_interval in intervals:
            self.interval_minutes, self.interval_milliseconds = intervals[self.time_interval]
        else:
            raise ValueError(f"Unsupported time interval: {self.time_interval}")

    @staticmethod
    def get_time_iso8601():
        # Get the current UTC time
        now_utc = datetime.now(timezone.utc)
        # Format the time in ISO 8601 and add 'Z'
        return now_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

    @staticmethod
    def is_data_complete(ohlcv, expected_interval):
        """
        Check if the OHLCV data is complete for the given expected interval.
    
        Parameters:
        - ohlcv: List of OHLCV data.
        - expected_interval: The expected interval between each data point in milliseconds.
    
        Returns:
        - bool: True if data is complete, False otherwise.
        """
        for i in range(len(ohlcv) - 1):
            if (ohlcv[i + 1][0] - ohlcv[i][0]) != expected_interval:
                return False
        return True
    
    def fetch_data(self):
    #self = exchange, symbol, timeframe/time_interval, start_time, end_time, interval_milliseconds):
        """
        Fetches OHLCV data for the specified symbol and timeframe from the exchange.
    
        Parameters:
        - exchange: Exchange object to fetch data from.
        - symbol: Symbol to fetch data for.
        - timeframe: Timeframe for the data.
        - start_time: Start time in milliseconds since epoch.
        - end_time: End time in milliseconds since epoch.
        - interval_milliseconds: Interval between each data point in milliseconds.
    
        Returns:
        - df: DataFrame containing the fetched data.
        """
        data = []
        limit = 1000
        while self.start_time < self.end_time:
            try:
                ohlcv = self.binance.fetch_ohlcv(self.symbol, self.time_interval, since=self.start_time, limit=limit)
                if not ohlcv:
                    break
                if not self.is_data_complete(ohlcv, self.interval_milliseconds):
                    print("Warning: Incomplete or irregular data detected")
                    break
                last_time = ohlcv[-1][0]
                if last_time > self.end_time:
                    ohlcv = [candle for candle in ohlcv if candle[0] <= self.end_time]
                    data += ohlcv
                    break
                data += ohlcv
                self.start_time = last_time + self.interval_milliseconds  # Adjust for next fetch
            except Exception as e:
                print(f"An error occurred: {e}")
                break
    
        df = pd.DataFrame(data, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
        df['date'] = pd.to_datetime(df['date'], unit='ms')
        df.set_index('date', inplace=True)

        return df

    def add_time_intervals(self, df):
        """
        Adds a column to a DataFrame counting specified minute intervals.
    
        Parameters:
        - df: pd.DataFrame, the original DataFrame with a datetime index.
        - interval_minutes: int, the length of the interval in minutes.
    
        Returns:
        - pd.DataFrame, the DataFrame with an additional 'time_count' column.
        """
        #df['time_count'] = range(len(df))
        
        #index duplicate column
        #start_time_count, pass the start value as a parameter when using range(). This allows you to control where the numbering begins.
        #df['time_count'] = range(self.start_time_count, self.start_time_count + len(df))
        df['time_count'] = range(int(self.start_time_count), int(self.start_time_count) + len(df))
        return df

    def run_all(self):
        """
        Execute the data fetching and processing pipeline.
        
        Returns:
            pd.DataFrame: The final processed DataFrame with OHLCV data and time intervals.
        """
        df = self.fetch_data()
        processed_df = self.add_time_intervals(df)
        return processed_df



