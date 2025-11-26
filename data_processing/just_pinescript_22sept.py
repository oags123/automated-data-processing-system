#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from datetime import datetime, timezone
import time
import fcntl
import pandas as pd
import pyperclip

# In[ ]:


def convert_timestamp_to_iso8601(timestamp) -> str:
    try:
        # Attempt to convert the input to an integer (in case it's passed as a string, float, etc.)
        timestamp = int(timestamp)
        # Convert the timestamp from milliseconds to seconds
        timestamp_seconds = timestamp / 1000
        # Convert the timestamp to a datetime object
        dt = datetime.utcfromtimestamp(timestamp_seconds)
        # Format the datetime object to the desired ISO 8601 format
        formatted_time = dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        return formatted_time
    except (ValueError, TypeError):
        # Return an error message if the input is not a valid timestamp
        return "Invalid input: Please provide a valid timestamp in milliseconds."

# Example usage:
# print(convert_timestamp_to_iso8601(1718985660000))
# print(convert_timestamp_to_iso8601("1718985660000"))  # Works with a string
# print(convert_timestamp_to_iso8601("invalid"))  # Handles invalid input


# In[ ]:


#12Septiembre modificaciones para todos los timeframes

#El primer paso va a ser cargar las coordenadas y el start time voya comenzar con el de 1m primero 
#aqui carga el txt pero la info esta como text en string falta ponerle en orden en data frame


def load_txt_file_safely(file_path, max_retries=5, retry_delay=3):
    """
    Load the contents of a text file into a string, with a fail-safe mechanism and file locking.
    
    Arguments:
    file_path -- The path to the text file to load.
    max_retries -- Maximum number of retries if the file is locked or being modified.
    retry_delay -- Delay in seconds before each retry.
    
    Returns:
    A string containing the contents of the file, or None if reading fails after retries.
    """
    retries = 0

    while retries < max_retries:
        try:
            # Open the file in read mode
            with open(file_path, 'r') as file:
                # Try to acquire a shared lock (to read the file safely while preventing writes)
                fcntl.flock(file, fcntl.LOCK_SH)

                # Read the contents of the file
                file_contents = file.read()

                # Release the shared lock
                fcntl.flock(file, fcntl.LOCK_UN)

                # Return the file contents if successful
                return file_contents

        except BlockingIOError:
            # If the file is locked by another process (e.g., rsync is updating it)
            print(f"File is currently being updated. Retrying in {retry_delay} seconds...")
            retries += 1
            time.sleep(retry_delay)

        except Exception as e:
            print(f"An error occurred while reading the file: {e}")
            return None

    print(f"Failed to read the file after {max_retries} retries.")
    return None

# Define the path to the file
#file_path_lines_coordinates_1m = "/home/oscar/Documents/vps_output/lines_coordinates_1m.txt"
#file_path_lines_coordinates_lc_1m_start_time = "/home/oscar/Documents/vps_output/lc_1m_start_time.txt"

# Load the file as a string, with a fail-safe retry mechanism
#lines_coordinates_1m = load_txt_file_safely(file_path_lines_coordinates_1m)
#start_time_1m = load_txt_file_safely(file_path_lines_coordinates_lc_1m_start_time)

# Optionally, print the contents or use the variable as needed
#if lines_coordinates_1m:
#    print(lines_coordinates_1m)


# In[ ]:


#esta funcion es para generar el data frame original como estaba antes. 


def reconstruct_original_data(reshaped_string):
    # Split the string by semicolon to separate the properties
    properties = reshaped_string.split('; ')
    data_dict = {}
    
    # Extract the original line names and their respective properties
    for prop in properties:
        # Use partition to split only on the first occurrence of ': '
        key, _, value = prop.partition(': ')
        # Retrieve the line name and the coordinate type
        line_name, coord = key.rsplit('_', 1)
        if line_name not in data_dict:
            data_dict[line_name] = {}
        data_dict[line_name][coord] = float(value)
    
    # Convert the dictionary back to DataFrame
    lines_properties = []
    for line, coords in data_dict.items():
        line_props = {'Line': line}
        line_props.update(coords)
        lines_properties.append(line_props)
    
    return pd.DataFrame(lines_properties)

# Example usage
# reshaped_string = "QR_0_05_Start_X: 90; QR_0_05_Start_Y: 58048.75; QR_0_05_End_X: 620; QR_0_05_End_Y: 58338.17; QR_0_95_Start_X: 90; QR_0_95_Start_Y: 58574.85; QR_0_95_End_X: 620; QR_0_95_End_Y: 58695.12; QR_1_05_Start_X: 370; QR_1_05_Start_Y: 58524.9; QR_1_05_End_X: 620; QR_1_05_End_Y: 58168.82; QR_1_95_Start_X: 370; QR_1_95_Start_Y: 58726.06; QR_1_95_End_X: 620; QR_1_95_End_Y: 58522.8; QR_2_05_Start_X: 498; QR_2_05_Start_Y: 58251.47; QR_2_05_End_X: 620; QR_2_05_End_Y: 58471.91; QR_2_95_Start_X: 498; QR_2_95_Start_Y: 58372.09; QR_2_95_End_X: 620; QR_2_95_End_Y: 58632.66; QR_3_05_Start_X: 591; QR_3_05_Start_Y: 58464.84; QR_3_05_End_X: 620; QR_3_05_End_Y: 58441.48; QR_3_95_Start_X: 591; QR_3_95_Start_Y: 58642.49; QR_3_95_End_X: 620; QR_3_95_End_Y: 58509.5; QR_4_05_Start_X: 601; QR_4_05_Start_Y: 58467.61; QR_4_05_End_X: 620; QR_4_05_End_Y: 58439.45; QR_4_95_Start_X: 601; QR_4_95_Start_Y: 58500.56; QR_4_95_End_X: 620; QR_4_95_End_Y: 58550.2"
#original_df = reconstruct_original_data(lines_coordinates_1m)
#print(original_df)


# In[ ]:


#este codigo es para tener el tiempo mas reciente en el formato que estoy usando

def get_utc_now_iso():
    
    """Returns the current time in UTC as an ISO 8601 formatted string."""
    
    # Get the current time in UTC as a timezone-aware datetime object
    current_time_utc = datetime.now(timezone.utc)
    
    # Format the time in the ISO 8601 format with 'Z' to indicate UTC
    formatted_time = current_time_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    #print(formatted_time)

    return formatted_time

#formatted_time = get_utc_now_iso()


# In[ ]:


#esta function es para contar los intervalos de 0 hasta la ultima fecha, nomas hace un data frame con el tiempo y interval count

def count_intervals(start_time, end_time, interval_minutes):
    """
    Generates a DataFrame counting specified minute intervals between a start and end time.

    Parameters:
    - start_time: str or datetime-like, the beginning of the interval counting.
    - end_time: str or datetime-like, the end of the interval counting.
    - interval_minutes: int, the length of each interval in minutes.

    Returns:
    - pd.DataFrame, a DataFrame with each interval's start time and a count column.
    """

    # Convert start_time and end_time to datetime if they are given as strings
    start_time = pd.to_datetime(start_time)
    end_time = pd.to_datetime(end_time)
    
    # Generate a date range from start_time to end_time with the specified interval using 'min' instead of 'T'
    time_range = pd.date_range(start=start_time, end=end_time, freq=f'{interval_minutes}min')
    
    # Calculate the interval counts starting from 0
    interval_count = list(range(len(time_range)))
    
    # Create a DataFrame from the time range and the interval counts
    df = pd.DataFrame({
        'Time': time_range,
        'Interval Count': interval_count
    })
    
    return df


# In[ ]:


#este codigo es para juntar los dos data frames el del intervalo con el del las coordenadas x y le agrega la columna de intervalo


def map_interval_to_time(intervals, original_df):
    
    """Maps an interval count to the corresponding time from the intervals DataFrame."""
   
    # Rename columns
    original_df.rename(columns={'X': 'X (interval count)', 'Y': 'Y (price)'}, inplace=True)
    
    # Convert 'X (interval count)' from float to integer
    original_df['X (interval count)'] = original_df['X (interval count)'].astype(int)
    
    # Assuming 'intervals' DataFrame is defined somewhere else and looks something like this:
    # intervals = pd.DataFrame({
    #     'Time': ['2024-01-01 00:00', '2024-01-01 00:05', '2024-01-01 00:10'],
    #     'Interval Count': [0, 1, 2]
    # })
    
    # Function to get time by interval count
    def get_time_by_interval(df, interval_count_input):
        result = df[df['Interval Count'] == interval_count_input]
        if not result.empty:
            return result['Time'].iloc[0].strftime('%Y-%m-%dT%H:%M:%S')
        else:
            return None
    
    # Create new 'time' column by applying the function to each row
    original_df['time'] = original_df['X (interval count)'].apply(lambda x: get_time_by_interval(intervals, x))
    
    # Print the updated DataFrame
    #print(original_df)

    return original_df # regresa el data frame con las siguientes columnas : Line; X (interval count) ; Y (price) ; time


# In[ ]:


#este codigo genera ya el codigo de pine script para el trading view, nomas le tienes que pasar como input el 
#data frame de con las columnas Line; X (interval count) ; Y (price) ; time

def generate_pine_script(original_df, granularity, color_code, width):
    # Create a deep copy of the DataFrame
    df_copy = original_df.copy()

    # Initialize an empty string to store Pine Script code
    pine_script_code = ""

    # Function to parse datetime string and generate Pine Script timestamp
    def generate_timestamp(time_str):
        dt = datetime.strptime(time_str, '%Y-%m-%dT%H:%M:%S')
        return f'timestamp("GMT", {dt.year}, {dt.month:02}, {dt.day:02}, {dt.hour:02}, {dt.minute:02}, {dt.second:02})'

    # Generate Pine Script lines for each row
    for index, row in df_copy.iterrows():
        line_name = f"{row['Line']}_{granularity}"  # Include granularity in the line name
        pine_script_code += f'{line_name}_X = {generate_timestamp(row["time"])}\n'
        pine_script_code += f'{line_name}_Y = {row["Y (price)"]}\n'

        # Assuming that we have pairs of rows for each start and end
        if index % 2 == 1:  # This assumes the second line (index 1) is the end line
            start_line = df_copy.iloc[index - 1]
            end_line = row

            pine_script_code += (f'line.new(x1={start_line["Line"]}_{granularity}_X, y1={start_line["Line"]}_{granularity}_Y, '
                                 f'x2={end_line["Line"]}_{granularity}_X, y2={end_line["Line"]}_{granularity}_Y, '
                                 f'xloc=xloc.bar_time, width={width}, color={color_code})\n')

    # Print or return the Pine Script code
    return pine_script_code


# In[ ]:


def generate_total_pine_script_code():
    
    # Generate Pine Script
    #granularity = '1m'
    #pine_script = generate_pine_script(original_df, granularity)
    #print(pine_script)
    

    
    #codigo para los demas time frames 5m,30m y 4h
    
    
    #111111111111111111111111111111111111111111111111
    
    #primero pasar la info de txt a variable 
    
    #1.- cargar el archivo txt a variable, las coordenadas y el start time:
    #file path
    #function = load_txt_file_safely
    
    #####################################
    
    #estas son las de 1 minuto repetidas 
    
    # Define the path to the file
    file_path_lines_coordinates_1m = "/home/oags/Downloads/9sept_mods/coordinates_sync_vps/lines_coordinates_1m.txt"
    file_path_lines_coordinates_lc_1m_start_time = "/home/oags/Downloads/9sept_mods/coordinates_sync_vps/lc_1m_start_time.txt"
    
    # Load the file as a string, with a fail-safe retry mechanism
    lines_coordinates_1m = load_txt_file_safely(file_path_lines_coordinates_1m)
    start_time_1m = load_txt_file_safely(file_path_lines_coordinates_lc_1m_start_time)
    
    #####################################
    
    #estas son las de 5 minutos falta revisar los paths
    
    # Define the path to the file
    file_path_lines_coordinates_5m = "/home/oags/Downloads/9sept_mods/coordinates_sync_vps/lines_coordinates_5m.txt"
    file_path_lines_coordinates_lc_5m_start_time = "/home/oags/Downloads/9sept_mods/coordinates_sync_vps/lc_5m_start_time.txt"
    
    # Load the file as a string, with a fail-safe retry mechanism
    lines_coordinates_5m = load_txt_file_safely(file_path_lines_coordinates_5m)
    start_time_5m = load_txt_file_safely(file_path_lines_coordinates_lc_5m_start_time)
    
    #####################################
    
    #estas son las de 30 minutos falta revisar los paths
    
    # Define the path to the file
    file_path_lines_coordinates_30m = "/home/oags/Downloads/9sept_mods/coordinates_sync_vps/lines_coordinates_30m.txt"
    file_path_lines_coordinates_lc_30m_start_time = "/home/oags/Downloads/9sept_mods/coordinates_sync_vps/lc_30m_start_time.txt"
    
    # Load the file as a string, with a fail-safe retry mechanism
    lines_coordinates_30m = load_txt_file_safely(file_path_lines_coordinates_30m)
    start_time_30m = load_txt_file_safely(file_path_lines_coordinates_lc_30m_start_time)
    
    #####################################
    
    #estas son las de 4 horas falta revisar los paths
    
    # Define the path to the file
    file_path_lines_coordinates_4h = "/home/oags/Downloads/9sept_mods/coordinates_sync_vps/lines_coordinates_4h.txt"
    file_path_lines_coordinates_lc_4h_start_time = "/home/oags/Downloads/9sept_mods/coordinates_sync_vps/lc_4h_start_time.txt"
    
    # Load the file as a string, with a fail-safe retry mechanism
    lines_coordinates_4h = load_txt_file_safely(file_path_lines_coordinates_4h)
    start_time_4h_unix = load_txt_file_safely(file_path_lines_coordinates_lc_4h_start_time)
    
    #el start time de 4h es el unico que tengo que cambiar especialmente de formato unix al iso
    
    start_time_4h = convert_timestamp_to_iso8601(start_time_4h_unix)

    

    
    # 2222222222222222222222222222222222222222222
    
    #reconstruir el data frame del string text 
    
    #2.- reconstruir el data frame a como estaba
    #function = reconstruct_original_data
    
    
    #estas son las variables cargadas con las coordenadas en txt : 
    
    #lines_coordinates_1m 
    #lines_coordinates_5m 
    #lines_coordinates_30m 
    #lines_coordinates_4h
    
    #1m 
    lines_coordinates_1m_original_df = reconstruct_original_data(lines_coordinates_1m)
    
    #5m
    lines_coordinates_5m_original_df = reconstruct_original_data(lines_coordinates_5m)
    
    #30m
    lines_coordinates_30m_original_df = reconstruct_original_data(lines_coordinates_30m)
    
    #4h
    lines_coordinates_4h_original_df = reconstruct_original_data(lines_coordinates_4h)
    
    #AGARRAR EL TIEMPO ACTUAL
    formatted_time = get_utc_now_iso()

    
    # 44444444444444444444444444444
    
    #4.- contar los intervalos de 0 hasta ultimo, df con el tiempo y interval count
    #function count_intervals
    
    #este codigo es para tener todos los data frames de count intervals
    
    #4h times
    start_time_4h = start_time_4h
    end_time_4h = formatted_time
    intervals_4h = count_intervals(start_time_4h, end_time_4h, 240)
    
    #30m times
    start_date_time_30m = start_time_30m
    end_date_time_30m = formatted_time
    intervals_30m = count_intervals(start_date_time_30m, end_date_time_30m, 30)
    #print(intervals_30m)
    
    #5m times
    start_date_time_5m = start_time_5m
    end_date_time_5m = formatted_time
    intervals_5m = count_intervals(start_date_time_5m, end_date_time_5m, 5)
    
    #1m times
    start_date_time_1m =  start_time_1m
    end_date_time_1m = formatted_time
    intervals_1m = count_intervals(start_date_time_1m, end_date_time_1m, 1)
    
    #el data frame intervals_1m tien dos columnas una del tiempo y otra del interval count y el index
    
    #intervals = intervals_1m
    
    
    
    # 555555555555555555555555555555555
    
    #5.- juntar los dos dfs el del intervalo con el del las coordenadas x y le agrega la columna de intervalo
    #function map_interval_to_time
    
    #map_interval_to_time(intervals, original_df) 
    # regresa el data frame con las siguientes columnas : Line; X (interval count) ; Y (price) ; time
    
    #original_df # regresa el data frame con las siguientes columnas : Line; X (interval count) ; Y (price) ; time
    
    #1m interval
    df_coordinates_complete_1m = map_interval_to_time(intervals_1m, lines_coordinates_1m_original_df)
    
    #5m interval
    df_coordinates_complete_5m = map_interval_to_time(intervals_5m, lines_coordinates_5m_original_df)
    
    #30m interval
    df_coordinates_complete_30m = map_interval_to_time(intervals_30m, lines_coordinates_30m_original_df)
    
    #4h interval
    df_coordinates_complete_4h = map_interval_to_time(intervals_4h, lines_coordinates_4h_original_df)
    
    
    
    
    # 6666666666666666
    
    #generar el codigo de Pine Script de cada interval
    
    # Generate Pine Script code
    
    #1m interval
    
    pine_script_code_1m = generate_pine_script(df_coordinates_complete_1m, '1m', '#FFFF00', 0)
    
    #5m interval
    
    pine_script_code_5m = generate_pine_script(df_coordinates_complete_5m, '5m', '#00FFFF', 1)
    
    #30m interval
    
    pine_script_code_30m = generate_pine_script(df_coordinates_complete_30m, '30m', '#FF00FF', 3)
    
    #4h interval
    
    pine_script_code_4h = generate_pine_script(df_coordinates_complete_4h, '4h', '#0000FF', 6)
    
    
    
    # ahora voy a juntar los intervalos en 1 funcion
    
    
    head_code = '''
// This Pine Script™ code is subject to the terms of the Mozilla Public License 2.0 at https://mozilla.org/MPL/2.0/
// © oags2023
//@version=5
indicator("qr_lines", overlay = true)
    
//code start

'''
    
    #print(head_code)
    #print(pine_script_code_1m)
    #print(pine_script_code_5m)
    #print(pine_script_code_30m)
    #print(pine_script_code_4h)
    
    total_pine_script_code =  head_code + pine_script_code_1m + pine_script_code_5m + pine_script_code_30m + pine_script_code_4h
    
    # Open a file for writing
    #with open("/home/oscar/Documents/vps_output/pine_script_code.txt", "w") as file:
        # Use the print function to write the formatted variable to the file
        #print(total_pine_script_code, file=file)

    #print(total_pine_script_code)
    return total_pine_script_code


# In[ ]:


total_pine_script_code_final = generate_total_pine_script_code()



# Your string variable
# my_string = "This is the text I want to copy."

# Copy the string to the clipboard
pyperclip.copy(total_pine_script_code_final)

# Confirm that the text has been copied
print("Text has been copied to clipboard!")


