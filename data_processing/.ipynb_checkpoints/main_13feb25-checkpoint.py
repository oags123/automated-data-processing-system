#main_13feb25.py

import time
from datetime import datetime

import pandas as pd
from fetch import *
from segments import RegressionSegments # Import the class
from quantreg import QuantileRegressionLines  # Import the class
import line_properties
from sqlite_utils import *
from pinescript_code import *



# List of financial symbols to process
symbols = ['BTC/USDT', 'ETH/USDT', 'XRP/USDT', 'SOL/USDT', 'ADA/USDT']
# Time intervals to process
intervals = ['4h', '30m', '5m', '1m']

def process_data(symbol, interval):
    """
    Simulate processing financial data for a given symbol and interval.
    This function is a placeholder for actual data retrieval and analysis.
    
    :param symbol: The financial trading pair (e.g., 'BTC/USDT')
    :param interval: The time interval for data analysis (e.g., '5m', '30m')
    """
    print(f"Processing {symbol} at interval {interval} - {datetime.now()}")

def process_intervals(intervals_to_run):
    """
    Process all required intervals for all symbols in hierarchical order.
    Ensures that when a higher interval is due, all lower intervals are processed as well.
    
    :param intervals_to_run: A set of intervals that need to be processed in this cycle.
    """

    for symbol in symbols:  # Process all intervals of one symbol before moving to the next
        for interval in ['4h', '30m', '5m', '1m']:  # Process intervals in hierarchical order
            if interval in intervals_to_run:  # Only process intervals that are due
                process_data(symbol, interval)  # Finish all processing for this symbol first

def calculate_sleep_seconds():
    """
    Calculate the sleep time until the next required execution.
    Ensures the script runs at the start of the next minute without unnecessary delays.
    
    :return: Number of seconds to sleep until the next minute begins.
    """
    now = datetime.now()
    seconds_to_next_minute = 60 - now.second  # Sleep until the start of the next minute
    return seconds_to_next_minute

if __name__ == "__main__":
    # Initial run: Process all intervals at script startup
    process_intervals(intervals)
    time.sleep(calculate_sleep_seconds())
    
    while True:
        now = datetime.now()
        intervals_to_run = []  # Use a list to maintain order
        
        # Determine which intervals need processing based on the current time
        if now.hour % 4 == 0 and now.minute == 0:
            # Every 4 hours (midnight, 4 AM, 8 AM, etc.), process all intervals
            intervals_to_run.extend(['4h', '30m', '5m', '1m'])
        elif now.minute % 30 == 0:
            # Every 30 minutes (e.g., 00:30, 01:00, 01:30, etc.), process 30m, 5m, and 1m
            intervals_to_run.extend(['30m', '5m', '1m'])
        elif now.minute % 5 == 0:
            # Every 5 minutes (e.g., 00:05, 00:10, 00:15, etc.), process 5m and 1m
            intervals_to_run.extend(['5m', '1m'])
        else:
            # Every 1 minute, process only the 1m interval
            intervals_to_run.append('1m')
        
        # Execute the required processing for the selected intervals
        process_intervals(intervals_to_run)
        
        # Sleep until the start of the next minute to maintain precise timing
        time.sleep(calculate_sleep_seconds())




#
#
#
#
#
#
#
#
#first run script agrupar los primeros 3 pasos en una funcion.

    #4h interval analysis
    #1.- START_TIME get the past 2000 interval candle from right now to be the start for analysis: get start time function
    #2.- fetch and get the prices data frame for this interval :  df = fetch_prices_get_df(interval, symbol, start_time, start_time_count)
    #3.- Do the analysis function:  regression_analysis(interval, symbol, df) 

    # 30m  interval analysis
    #1.- START_TIME get lower interval 30m start time, it is saved on a sqlite database table, get the name of the table and get the start time: get start time function
    #2.- fetch and get the prices data frame for this interval : df = fetch_prices_get_df(interval, symbol, start_time, start_time_count)
    #3.- Do the analysis function:  regression_analysis(interval, symbol, df) 

    # 5m  interval analysis
    #1.- START_TIME get lower interval 5m start time, it is saved on a sqlite database table, get the name of the table and get the start time: get start time function
    #2.- fetch and get the prices data frame for this interval : df = fetch_prices_get_df(interval, symbol, start_time, start_time_count)
    #3.- Do the analysis function:  regression_analysis(interval, symbol, df) 

    # 1m  interval analysis
    #1.- START_TIME get lower interval 1m start time, it is saved on a sqlite database table, get the name of the table and get the start time: get start time function
    #2.- fetch and get the prices data frame for this interval : df = fetch_prices_get_df(interval, symbol, start_time, start_time_count)
    #3.- Do the analysis function:  regression_analysis(interval, symbol, df) 

def fetch_prices_get_df(time_interval, symbol, start_time_iso, start_time_count):
    # Step 1: Instantiate the FetchPrices class
    fetcher = FetchPrices(time_interval, symbol, start_time_iso, start_time_count)

    # Step 2: Call the run_all method
    df = fetcher.run_all()

    return df



def regression_analysis(time_interval, symbol, df):

    #linear regression segments process
    segments = RegressionSegments(df) 
    
    # Step 2: Call the run_all method
    regression_segmentation = segments.segmentation_processes()
    regression_segmentation2 = segments.remove_duplicates_preserve_order(regression_segmentation)

    #quantile regression process
    qr = QuantileRegressionLines(df, regression_segmentation2)
    
    df_qr = qr.run_all()
    # df_qr goes to plot chart

    #Line properties
    lines_properties = line_properties.calculate_and_add_properties(df_qr)
    lines_coordinates = line_properties.calculate_and_add_coordinates_only(df_qr)
    start_time_lower_interval = line_properties.format_datetime_intervals(regression_segmentation2, df, time_interval)

    #save data frames to SQLite data base

    #extract only 3 letters from the symbol to avoid errors with sqlite with /
    short_symbol = symbol[:3]  # Extract the first three letters (BTC)
    
    table_name1 = f"prices_{short_symbol}_{time_interval}"
    table_name2 = f"qr_lines_{short_symbol}_{time_interval}"
    table_name3 = f"lines_properties_{short_symbol}_{time_interval}"
    table_name4 = f"lines_coordinates_{short_symbol}_{time_interval}"
    table_name5 = f"start_time_lower_interval_{short_symbol}_{time_interval}"
    
    save_df_sqlite(df, table_name1, if_exists='replace')
    save_df_sqlite(df_qr, table_name2, if_exists='replace')
    save_df_sqlite(lines_properties, table_name3, if_exists='replace')
    save_df_sqlite(lines_coordinates, table_name4, if_exists='replace')
    save_df_sqlite(start_time_lower_interval, table_name5, if_exists='replace')
    

def start_all_intervals_regression_analysis(symbol, interval=all):
    #extract only 3 letters from the symbol to avoid errors with sqlite with /
    short_symbol = symbol[:3]  # Extract the first three letters (BTC)
    
    #4h regression analysis
    start_time_4h = get_past_time_4h_iso8601() #fetch.py  Subtract 2000 hours
    df_4h = fetch_prices_get_df('4h', symbol, start_time_4h, start_time_count=0)
    regression_analysis('4h', symbol, df_4h) 
    
    #get lower interval 30m start time
    table_name_start_time_4h = f"start_time_lower_interval_{short_symbol}_4h"
    start_time_30m = get_start_time(table_name_start_time_4h)
    
    # 30m regression analysis
    df_30m = fetch_prices_get_df('30m', symbol, start_time_30m, start_time_count=0)
    regression_analysis('30m', symbol, df_30m) 

    #get lower interval 5m start time
    table_name_start_time_30m = f"start_time_lower_interval_{short_symbol}_30m"
    start_time_5m = get_start_time(table_name_start_time_30m)
    
    # 5m regression analysis
    df_5m = fetch_prices_get_df('5m', symbol, start_time_5m, start_time_count=0)
    regression_analysis('5m', symbol, df_5m) 

    #get lower interval start time
    table_name_start_time_5m = f"start_time_lower_interval_{short_symbol}_5m"
    start_time_1m = get_start_time(table_name_start_time_5m)
    
    # 1m regression analysis
    df_1m = fetch_prices_get_df('1m', symbol, start_time_1m, start_time_count=0)
    regression_analysis('1m', symbol, df_1m) 


def update_interval_regression_analysis(symbol, interval):
    #extract only 3 letters from the symbol to avoid errors with sqlite with /
    short_symbol = symbol[:3]  # Extract the first three letters (BTC)

    if interval == "4h":
    #4h regression analysis
        df_4h = update_df('4h', symbol)
        regression_analysis('4h', symbol, df_4h) 

    #get lower interval 30m start time
    table_name_start_time_4h = f"start_time_lower_interval_{short_symbol}_4h"
    start_time_30m = get_start_time(table_name_start_time_4h)
    
    # 30m regression analysis
    df_30m = fetch_prices_get_df('30m', symbol, start_time_30m, start_time_count=0)
    regression_analysis('30m', symbol, df_30m) 

    #get lower interval 5m start time
    table_name_start_time_30m = f"start_time_lower_interval_{short_symbol}_30m"
    start_time_5m = get_start_time(table_name_start_time_30m)
    
    # 5m regression analysis
    df_5m = fetch_prices_get_df('5m', symbol, start_time_5m, start_time_count=0)
    regression_analysis('5m', symbol, df_5m) 

    #get lower interval start time
    table_name_start_time_5m = f"start_time_lower_interval_{short_symbol}_5m"
    start_time_1m = get_start_time(table_name_start_time_5m)
    
    # 1m regression analysis
    df_1m = fetch_prices_get_df('1m', symbol, start_time_1m, start_time_count=0)
    regression_analysis('1m', symbol, df_1m) 

def update_df(interval, symbol):
    #This function will fetch the new prices and merge with the old df and get a new updated df and return it
    short_symbol = symbol[:3]  # Extract the first three letters (BTC)
    
    table_name = f"prices_{short_symbol}_{interval}"
    
    last_timedate_value = get_last_date_value(table_name, "date")
    #print(last_timedate_value)
    last_time_count_value = get_last_date_value(table_name, "time_count")
    print(last_time_count_value)
    start_time_iso_incremented = increment_timestamp(last_timedate_value, interval)
    #print(start_time_iso_incremented)
    
    
    
    
    # Step 1: Instantiate the FetchPrices class
    fetcher = FetchPrices(interval, symbol, start_time_iso_incremented)

    # Step 2: Call the run_all method
    new_update_df = fetcher.run_all()
    print(new_update_df)
    #get the old df with the prices sqlite to pandas df
    old_df = dbtable_todf(table_name)
    print(old_df)

update_regression_analysis('1m', 'BTC/USDT')

def run_all_symbols(interval):
    # List of symbols string inputs
    symbols = ['BTC/USDT'], 'ETH/USDT', 'XRP/USDT', 'SOL/USDT', 'ADA/USDT']
    
    # Run the function for each input
    for value in symbols:
        #run all the corresponding functions process and analysis for the corresponding interval 
        #update existing df here. I need last datetime and last time_count index
        all_intervals_regression_analysis(value, interval)
        generate_total_pine_script_code(value)
