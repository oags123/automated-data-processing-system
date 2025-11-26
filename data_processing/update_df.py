



#
#
#
#
#
#
#
#updating data frame 
#
#
#
#
#
#
#

#Tiempo ahorita sin redondear 
# Get the current UTC date and time
#timenow_utc = datetime.utcnow()

#print(f"Current UTC date and time: {timenow_utc}")


#tiempo ahorita redondeado
def get_current_time_now_rounded():    
    # Get the current UTC time
    now_utc = datetime.utcnow()
    
    # Round down to the nearest minute by subtracting the current seconds (and microseconds)
    rounded_utc = now_utc - timedelta(seconds=now_utc.second, microseconds=now_utc.microsecond)
    
    #print(f"Current UTC time, rounded down to the nearest minute: {rounded_utc}")
    return rounded_utc

def update_dataframe_current_time(df_old, interval, interval_minutes):
    #update dataframe timeframe 
    #cambio para usarlo en todos los timeframes intervals 
    def convert_time_format_tobinance(original_datetime):
        # Check if the input is a pandas Timestamp, convert to datetime object if true
        if isinstance(original_datetime, pd.Timestamp):
            datetime_obj = original_datetime.to_pydatetime()
        elif isinstance(original_datetime, str):
            # Specify the original format of the datetime string
            original_format = "%Y-%m-%d %H:%M:%S"
            # Parse the original datetime string into a datetime object
            datetime_obj = datetime.strptime(original_datetime, original_format)
        else:
            # If the input is already a datetime object, use it directly
            datetime_obj = original_datetime
        
        # Specify the desired output format
        # 'T' is included as a literal character in the format string
        # 'Z' indicates UTC timezone
        desired_format = "%Y-%m-%dT%H:%M:%SZ"
        
        # Convert the datetime object back into a string with the desired format
        converted_datetime_str = datetime_obj.strftime(desired_format)
        #print("convert_time_format_tobinance")
        #print(converted_datetime_str)
        
        return converted_datetime_str
    
    def update_count_intervals(df_new, df_old, interval_minutes):
        """
        Adds a column to a DataFrame counting specified minute intervals, continuing
        from the time_count of another DataFrame.
        """
        if df_new.index.min() is pd.NaT or df_old.index.max() is pd.NaT:
            print("Warning: Date-time indices contain NaT values.")
            return None
        
        # Ensure the DataFrame's index is in datetime format and sort the indexes
        for df_to_process in [df_new, df_old]:
            if not pd.api.types.is_datetime64_any_dtype(df_to_process.index):
                df_to_process.index = pd.to_datetime(df_to_process.index)
            df_to_process.sort_index(inplace=True)
        
        # Calculate the starting point for time_count in the new df_new
        if not df_old.empty:
            last_time_count = df_old['time_count'].iloc[-1]
            
            # Example check for NaN values and print for debugging
            if pd.isna(last_time_count):
                print("Warning: 'last_time_count' is NaN.")
            try:
                time_diff = (df_new.index.min() - df_old.index.max()).total_seconds() / (interval_minutes * 60)
                start_point = last_time_count + ceil(time_diff)  # Adjusted to use ceil for rounding up
            except Exception as e:
                print(f"Error calculating start_point: ")
                return None
        else:
            start_point = 0
        
        # Calculate the number of specified minute intervals since the start of the new DataFrame
        df_new['time_count'] = (((df_new.index - df_new.index.min()).total_seconds()) / (interval_minutes * 60)).astype(int) + start_point
        
        #print("update_count_intervals")
        #print(df_new)
        
        return df_new
        
    def update_old_data_frame(df_old, interval, interval_minutes):
        # Accessing the last DatetimeIndex value of the last row
        last_datetime_index_df_old_minus1 = df_old.index[-1]
        #print(last_datetime_index_df_old )
        #add 1 minute to the last to avoid fetching the same minute
        #   
        # Ensure the datetime object is properly formatted as string
        if isinstance(last_datetime_index_df_old_minus1, pd.Timestamp):
            last_datetime_index_df_old_minus1_str = last_datetime_index_df_old_minus1.strftime('%Y-%m-%d %H:%M:%S')
        else:
            last_datetime_index_df_old_minus1_str = last_datetime_index_df_old_minus1  # Assuming it's already a string
    
        # Parse the string to datetime object
        datetime_obj = datetime.strptime(last_datetime_index_df_old_minus1_str, '%Y-%m-%d %H:%M:%S')   
        datetime_obj += timedelta(minutes=interval_minutes)
        last_datetime_index_df_old = datetime_obj.strftime('%Y-%m-%d %H:%M:%S')
        #last_datetime_index_df_old = agarra el ultimo tiempo del data frame viejito le agrega 1 min o los minutos 
        #y te lo regresa en string con formato normal pero ya sumado el siguiente interval para que no repita el fetch
        
        #print("New datetime:", last_datetime_index_df_old )
        #
        # Round down to the nearest minute by subtracting the current seconds (and microseconds)
        timenow_rounded_utc = get_current_time_now_rounded()
        #timenow_rounded_utc = now_utc - timedelta(seconds=now_utc.second, microseconds=now_utc.microsecond)
        #print(f"Current UTC time, rounded down to the nearest minute: {timenow_rounded_utc}")
        
        update_start_date_time_iso = convert_time_format_tobinance(last_datetime_index_df_old)
        update_end_date_time_iso = convert_time_format_tobinance(timenow_rounded_utc)

            # Check if the start and end date-times are equal
        if update_start_date_time_iso == update_end_date_time_iso:
        # If they are equal, exit the function. Return None or a specific value indicating the early exit.
            print("Function update_old_data_frame exited early due to equal date-time values. Skipping check_skips_in_df.")
            return None
    
        # If they are not equal, continue with the function's logic
        
        update_start_time_binance = binance.parse8601(update_start_date_time_iso)
        update_end_time_binance = binance.parse8601(update_end_date_time_iso)

        interval_milliseconds = interval_minutes * 60 * 1000
        # Fetch data
        update_df_new = fetch_data(binance, 'BTC/USDT', interval, update_start_time_binance, update_end_time_binance, interval_milliseconds)
        # For counting 1-minute intervals
        update_df_new_count = update_count_intervals(update_df_new, df_old, interval_minutes)
        
        # Append new_data to df and drop duplicates based on index (DateTime)
        updated_df_new = pd.concat([df_old, update_df_new_count]).sort_index().drop_duplicates(keep='last')
        # Check for duplicates in the 'time_count' column
        if updated_df_new['time_count'].duplicated().any():
            print("Error: There are duplicate values in the 'time_count' column.")
        # Convert the index to a Series to use the .duplicated() method, checking for duplicate index values
        if pd.Series(updated_df_new.index).duplicated().any():
            print("Error: There are duplicate values in the DataFrame index.")

        #print("update_old_data_frame")
        #print(updated_df_new)
        
        return updated_df_new
    
    def check_skips_in_df(df):
        # Ensure the index is sorted
        df.sort_index(inplace=True)
        
        # Check for skipped numbers in 'time_count'
        time_count_diff = df['time_count'].diff() - 1  # We expect each diff to be 1, so subtracting 1 should give 0
        # Ignore the first row since it has no previous row to compare with
        skipped_time_counts = time_count_diff[time_count_diff != 0].iloc[1:]
        
        if skipped_time_counts.empty:
            pass
            #print("No skipped numbers in 'time_count'.")
        else:
            print("Skipped numbers found in 'time_count' at rows:", skipped_time_counts.index.tolist())
        
        # Check for skipped datetime in index (assuming the index is already in datetime format)
        datetime_diff = df.index.to_series().diff().dt.total_seconds() / 60 - 1  # Convert diff to minutes and subtract 1
        # Ignore the first row for the same reason
        skipped_datetimes = datetime_diff[datetime_diff != 0].iloc[1:]
        
        if skipped_datetimes.empty:
            pass
            #print("No skipped datetime in index.")
        else:
            print("Skipped datetime found in index at positions:")#, skipped_datetimes.index.tolist())
    
    updated_df = update_old_data_frame(df_old, interval, interval_minutes)
    
    # Check if the function returned a value indicating it did not exit early
    if updated_df is not None:
        # If result is not None, it means the function did not exit early, so we can safely call the next function
        check_skips_in_df(updated_df)
    else:
        # Handle the case where the function exited early (e.g., by skipping the call or taking some other action)
        print("Function exited early due to equal date-time values. Skipping check_skips_in_df.")

    return updated_df