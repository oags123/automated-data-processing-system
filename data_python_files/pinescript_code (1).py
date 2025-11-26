

from datetime import datetime, timezone
import time
import fcntl
import pandas as pd
#import pyperclip

import pandas as pd
from sqlite_utils import dbtable_todf

#este codigo genera ya el codigo de pine script para el trading view, nomas le tienes que pasar como input el 
#data frame de con las columnas Line; X (interval count) ; Y (price) ; time

def generate_pine_script(original_df, granularity, color_code, width):
    # Create a deep copy of the DataFrame
    df_copy = original_df.copy()

    # Initialize an empty string to store Pine Script code
    pine_script_code = ""

    # Function to parse datetime string and generate Pine Script timestamp
    def generate_timestamp(time_str):
        # Update format to match your DataFrame's time format
        dt = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')  
        return f'timestamp("GMT", {dt.year}, {dt.month:02}, {dt.day:02}, {dt.hour:02}, {dt.minute:02}, {dt.second:02})'

    def generate_timestamp_old(time_str):
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


def generate_total_pine_script_code(symbol):
    
    short_symbol = symbol[:3]  # Extract the first three letters (BTC)

    table_name_4h = f"lines_coordinates_{short_symbol}_4h"
    table_name_30m = f"lines_coordinates_{short_symbol}_30m"
    table_name_5m = f"lines_coordinates_{short_symbol}_5m"
    table_name_1m = f"lines_coordinates_{short_symbol}_1m"
    
    #load dfs from sqlite
    lines_coordinates_BTC_4h = dbtable_todf(table_name_4h)
    lines_coordinates_BTC_30m = dbtable_todf(table_name_30m)
    lines_coordinates_BTC_5m = dbtable_todf(table_name_5m)
    lines_coordinates_BTC_1m = dbtable_todf(table_name_1m)
    
    #generate pinescript code
    pine_script_code_4h = generate_pine_script(lines_coordinates_BTC_4h, '4h', '#0000FF', 6)
    pine_script_code_30m = generate_pine_script(lines_coordinates_BTC_30m, '30m', '#FF00FF', 3)
    pine_script_code_5m = generate_pine_script(lines_coordinates_BTC_5m, '5m', '#00FFFF', 1)
    pine_script_code_1m = generate_pine_script(lines_coordinates_BTC_1m, '1m', '#FFFF00', 0)

    head_code = '''
// This Pine Script™ code is subject to the terms of the Mozilla Public License 2.0 at https://mozilla.org/MPL/2.0/
// © oags2023
//@version=5
indicator("qr_lines", overlay = true)
    
//code start

'''

    #merge total pinescript code
    total_pine_script_code = head_code + pine_script_code_4h + pine_script_code_30m + pine_script_code_5m + pine_script_code_1m
    
    #write pinescript to txt file
    # File path (replace 'output.txt' with your desired file path)

    file_path = f"pinescript_{short_symbol}.txt"

    # Save the string into the file, replacing the file if it exists
    with open(file_path, "w") as file:
        file.write(total_pine_script_code)
    
    #print(f"File saved successfully at {file_path}")


    
    
