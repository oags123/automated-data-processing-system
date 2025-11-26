import numpy as np
import pandas as pd
import re
from datetime import datetime 

def process_dataframe(input_df):
    """
    Processes the input DataFrame by dropping specific columns and retaining others.
    
    Parameters:
    - input_df: pandas DataFrame to be processed.
    
    Returns:
    - A deep copy of the processed DataFrame with specified columns removed.
    """
    cols_to_drop = ['Start_X', 'Start_Y', 'End_X']  # 'Line' is not dropped here
    cols_to_drop = [col for col in cols_to_drop if col in input_df.columns]
    processed_df = input_df.drop(columns=cols_to_drop).copy(deep=True)
    
    return processed_df


def process_dataframe_2(df):
    """
    Process the dataframe by applying multiple cleaning and renaming operations:
    1. Drops columns based on specified suffixes.
    2. Renames columns by removing certain prefixes and suffixes.
    3. Extracts and remaps line IDs in column names.
    4. Converts all column names to lowercase.

    Parameters:
    df (pd.DataFrame): The original dataframe to process.

    Returns:
    pd.DataFrame: The processed dataframe.
    """

    # Drop columns that end with specified suffixes
    suffixes_to_drop = ['End_Y', '95_Line_Length_X', '95_Width_Start', '95_Width_End']
    cols_to_drop = [col for col in df.columns if any(col.endswith(suffix) for suffix in suffixes_to_drop)]
    df = df.drop(columns=cols_to_drop)

    # Rename columns by removing '05_' from those that end with specified suffixes
    suffixes_to_rename = ['05_Line_Length_X', '05_Width_Start', '05_Width_End']
    cols_to_rename = {col: col.replace('05_', '') for col in df.columns if any(col.endswith(suffix) for suffix in suffixes_to_rename)}
    df = df.rename(columns=cols_to_rename)

    # Remove 'QR_' from the start of each column name
    cols_remove_qr = {col: col.replace('QR_', '') for col in df.columns if col.startswith('QR_')}
    df = df.rename(columns=cols_remove_qr)

    # Extract leading numbers from column names as line IDs and remap
    line_ids = {col: int(re.match(r'\d+', col).group()) for col in df.columns if re.match(r'\d+', col)}
    unique_sorted_ids = sorted(set(line_ids.values()))
    id_mapping = {old_id: new_id for new_id, old_id in enumerate(unique_sorted_ids, start=1)}
    new_column_names = {}
    for col in df.columns:
        match = re.match(r'(\d+)(_.+)', col)
        if match:
            line_id, rest_of_name = match.groups()
            new_column_names[col] = f'{id_mapping[int(line_id)]}{rest_of_name}'
    df = df.rename(columns=new_column_names)

    # Change all column names to lowercase
    df.columns = df.columns.str.lower()

    return df

def calculate_and_add_coordinates_only(df):
    """
    Calculate the start and end coordinates (X and Y) for each line in the DataFrame,
    using the 'date' column or index for coordinates. Output the DataFrame in vertical format,
    where each line's start and end coordinates are in separate rows.
    
    Args:
    - df: DataFrame with a 'date' column or an index containing datetime values,
          and columns prefixed with 'QR_' representing line data.
    
    Returns:
    - Transformed DataFrame in vertical format, where each line's start and end coordinates
      are represented in separate rows with 'Line', 'time', and 'Y (price)' columns.
    """
    vertical_data = []

    # Ensure 'date' is accessible either as a column or as an index
    if 'date' in df.columns:
        date_column = 'date'
    elif df.index.name == 'date' or df.index.dtype.kind == 'M':  # Check if the index is datetime
        df = df.reset_index()  # Convert index to a column
        date_column = 'date'
    else:
        raise KeyError("The DataFrame does not have a 'date' column or a datetime index.")

    for col in df.columns:
        if col.startswith('QR_'):
            # Extract line data and 'date' column
            line_data = df[[col, date_column]].dropna()
            if not line_data.empty:
                # Use 'date' for start and end X coordinates
                start_x = line_data[date_column].iloc[0]
                start_y = round(line_data[col].iloc[0], 2)  # Round to 2 decimal places
                end_x = line_data[date_column].iloc[-1]
                end_y = round(line_data[col].iloc[-1], 2)  # Round to 2 decimal places

                # Append Start and End rows in vertical format
                vertical_data.append({'Line': f"{col}_Start", 'time': start_x, 'Y (price)': start_y})
                vertical_data.append({'Line': f"{col}_End", 'time': end_x, 'Y (price)': end_y})

    # Create the vertical DataFrame
    vertical_df = pd.DataFrame(vertical_data)

    return vertical_df



def calculate_and_add_coordinates_only_old(df):
    """
    Calculate the start and end coordinates (X and Y) for each line in the DataFrame,
    using the 'date' column or index for coordinates. Transform the DataFrame so that
    each line's coordinates are in a single row, with each property prefixed by the line's name.
    
    Args:
    - df: DataFrame with a 'date' column or an index containing datetime values,
          and columns prefixed with 'QR_' representing line data.
    
    Returns:
    - Transformed DataFrame with each line's start and end coordinates in a single row,
      with each property prefixed by the line's name.
    """
    lines_properties = []

    # Ensure 'date' is accessible either as a column or as an index
    if 'date' in df.columns:
        date_column = 'date'
    elif df.index.name == 'date' or df.index.dtype.kind == 'M':  # Check if the index is datetime
        df = df.reset_index()  # Convert index to a column
        date_column = 'date'
    else:
        raise KeyError("The DataFrame does not have a 'date' column or a datetime index.")

    for col in df.columns:
        if col.startswith('QR_'):
            # Extract line data and 'date' column
            line_data = df[[col, date_column]].dropna()
            if not line_data.empty:
                # Use 'date' for start and end X coordinates
                start_x = line_data[date_column].iloc[0]
                start_y = round(line_data[col].iloc[0], 2)  # Round to 2 decimal places
                end_x = line_data[date_column].iloc[-1]
                end_y = round(line_data[col].iloc[-1], 2)  # Round to 2 decimal places

                line_props = {
                    'Line': col,
                    'Start_X': start_x,
                    'Start_Y': start_y,
                    'End_X': end_x,
                    'End_Y': end_y
                }

                lines_properties.append(line_props)

    lines_df = pd.DataFrame(lines_properties)

    # Reshape the DataFrame to have one row with all properties
    final_df = pd.DataFrame()
    for _, row in lines_df.iterrows():
        line_name = row['Line']
        for col in lines_df.columns:
            if col != 'Line':  # Exclude the 'Line' column from the final DataFrame
                final_df[f"{line_name}_{col}"] = [row[col]]

    return final_df

def calculate_and_add_coordinates_only_old1(df):
    """
    Calculate the start and end coordinates (X and Y) for each line in the DataFrame,
    and transform the DataFrame so that each line's coordinates are in a single row, 
    with each property prefixed by the line's name.
    """
    lines_properties = []

    for col in df.columns:
        if col.startswith('QR_'):
            line_data = df[[col, 'time_count']].dropna()
            if not line_data.empty:
                start_x = line_data['time_count'].iloc[0]
                start_y = round(line_data[col].iloc[0], 2)  # Round to 2 decimal places
                end_x = line_data['time_count'].iloc[-1]
                end_y = round(line_data[col].iloc[-1], 2)  # Round to 2 decimal places

                line_props = {
                    'Line': col,
                    'Start_X': start_x,
                    'Start_Y': start_y,
                    'End_X': end_x,
                    'End_Y': end_y
                }

                lines_properties.append(line_props)

    lines_df = pd.DataFrame(lines_properties)

    # Reshape the DataFrame to have one row with all properties
    final_df = pd.DataFrame()
    for _, row in lines_df.iterrows():
        line_name = row['Line']
        for col in lines_df.columns:
            if col != 'Line':  # Ensure 'Line' column is not included in the final DataFrame
                final_df[f"{line_name}_{col}"] = [row[col]]

    # Convert the DataFrame to a single string formatted as required
    #result_string = '; '.join([f"{col}: {final_df[col].iat[0]}" for col in final_df.columns])
    #return result_string
    return final_df 

def calculate_and_add_properties(df):
    """
    Calculate the start and end coordinates, slope, angle in degrees for each line in the DataFrame,
    and also calculate Line_Length_X, Width_Start, and Width_End for each line pair. Then, transform
    the DataFrame so that each line's properties are in a single row, with each property prefixed by the line's name.
    """
    lines_properties = []

    for col in df.columns:
        if col.startswith('QR_'):
            line_data = df[[col, 'time_count']].dropna()
            if not line_data.empty:
                start_x = line_data['time_count'].iloc[0]
                start_y = line_data[col].iloc[0]
                end_x = line_data['time_count'].iloc[-1]
                end_y = line_data[col].iloc[-1]

                slope = (end_y - start_y) / (end_x - start_x) if (end_x - start_x) != 0 else np.inf
                angle = np.degrees(np.arctan(slope))

                line_props = {
                    'Line': col,
                    'Start_X': start_x,
                    'Start_Y': start_y,
                    'End_X': end_x,
                    'End_Y': np.ceil(end_y),
                    'Slope': round(slope, 2),
                    'Angle_degrees': round(angle, 2),
                    'Line_Length_X': end_x - start_x,
                    'Width_Start': np.nan,
                    'Width_End': np.nan
                }

                lines_properties.append(line_props)

    lines_df = pd.DataFrame(lines_properties)

    for i in range(0, len(lines_df), 2):
        if i + 1 < len(lines_df):
            lines_df.loc[i:i+1, 'Width_Start'] = round(abs(lines_df.loc[i+1, 'Start_Y'] - lines_df.loc[i, 'Start_Y']), 2)
            lines_df.loc[i:i+1, 'Width_End'] = np.ceil(abs(lines_df.loc[i+1, 'End_Y'] - lines_df.loc[i, 'End_Y']))

    # Process the DataFrame to remove unnecessary columns
    processed_df = process_dataframe(lines_df)

    # Reshape the DataFrame to have one row with all properties
    final_df = pd.DataFrame()
    for _, row in processed_df.iterrows():
        line_name = row['Line']
        for col in processed_df.columns:
            if col != 'Line':  # Ensure 'Line' column is not included in the final DataFrame
                final_df[f"{line_name}_{col}"] = [row[col]]
    final_df = process_dataframe_2(final_df)
    return final_df


#function to get the start time from the lower interval.

def format_datetime_intervals_old(filtered_segments, filtered_df, time_interval):
    """
    Formats the start and end datetime intervals based on the last value in filtered_segments and a given end datetime.

    Args:
        filtered_segments (list of tuples): A list containing tuples, where each tuple represents a segment.
        filtered_df (pd.DataFrame): A DataFrame containing a 'time_count' column to match with the last tuple's first value.
        end_date_time (str): The end datetime string in '%Y-%m-%d %H:%M:%S' format.

    Returns:
        tuple of str: A tuple containing formatted start and end datetime strings in '%Y-%m-%dT%H:%M:%SZ' format.
    """
    # Access the first value of the last tuple in filtered_segments
    first_value_of_last_tuple = filtered_segments[-1][0]

    # Find the index where 'time_count' matches 'first_value_of_last_tuple'
    matching_row = filtered_df[filtered_df['time_count'] == first_value_of_last_tuple]

    if not matching_row.empty:
        date_time_index = matching_row.index[0]  # Extract the first (or only) matching index.
    else:
        raise ValueError("No matching row found in the DataFrame.")

    # Ensure the 'date_time_index' is a datetime object before formatting
    if not isinstance(date_time_index, pd.Timestamp):
        date_time_index = pd.to_datetime(date_time_index)

    # Format start datetime
    formatted_start_date_time = date_time_index.strftime('%Y-%m-%dT%H:%M:%SZ')

    # Convert end_date_time to datetime object and format
    #dt_object_2 = datetime.strptime(end_date_time, '%Y-%m-%d %H:%M:%S')
    #formatted_end_date_time = dt_object_2.strftime('%Y-%m-%dT%H:%M:%SZ')


    #convert lower interval start time to pandas data frame to output
    
    # Input time interval
    #time_interval = '4h'
    # Define lower_interval with a default value
    lower_interval = None

    # Determine the lower interval
    if time_interval == '1h':
        lower_interval = '30m'
    elif time_interval == '30m':
        lower_interval = '5m'
    elif time_interval == '5m':
        lower_interval = '1m'
    else:
        lower_interval = None  # Optional: Handle cases where time_interval doesn't match
    
    # Output the result
    #print(f"Time interval: {time_interval}, Lower interval: {lower_interval}")
    
    # Create a DataFrame
    data1 = {'start_time': [formatted_start_date_time], 'interval': [lower_interval]}
    start_times_df = pd.DataFrame(data1)
    
    # Display the DataFrame
    #print(start_times_df)
    
    return start_times_df


def format_datetime_intervals(filtered_segments, filtered_df, time_interval):
    """
    Formats the start datetime for a given lower interval based on the last
    segment in filtered_segments and the matching entry in filtered_df.
    """

    # Find the last segment's first value
    first_value_of_last_tuple = filtered_segments[-1][0]
    matching_row = filtered_df[filtered_df['time_count'] == first_value_of_last_tuple]

    if matching_row.empty:
        raise ValueError("No matching row found in the DataFrame.")

    # Prefer the ISO datetime column if it exists
    if 'date_time_iso' in matching_row.columns:
        date_time_index = pd.to_datetime(matching_row['date_time_iso'].iloc[0])
    elif 'date' in matching_row.columns:
        date_time_index = pd.to_datetime(matching_row['date'].iloc[0], unit='ms')
    else:
        raise ValueError("No valid datetime column found (expected 'date_time_iso' or 'date').")

    formatted_start_date_time = date_time_index.strftime('%Y-%m-%dT%H:%M:%SZ')

    # Determine the lower interval
    lower_interval = None
    if time_interval == '1h':
        lower_interval = '30m'
    elif time_interval == '30m':
        lower_interval = '5m'
    elif time_interval == '5m':
        lower_interval = '1m'

    start_times_df = pd.DataFrame({
        'start_time': [formatted_start_date_time],
        'interval': [lower_interval]
    })

    return start_times_df
