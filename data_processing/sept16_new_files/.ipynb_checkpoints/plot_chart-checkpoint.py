
import pandas as pd        # for handling DataFrames
import matplotlib.pyplot as plt  # optional, but useful if you want extra customization
import mplfinance as mpf   # for candlestick and financial plotting


filename = 'temp_test'

def plot_chart(df_filtered, filename):

    #code to change the data frame to just works with the original code to plot 
    #the issue is that it is expecting a index with the timestamp
    df_filtered = df_filtered.copy()
    df_filtered.index = pd.to_datetime(df_filtered['date'], unit='ms', utc=True)
    
    # Make sure the index is sorted and unique (good practice for mplfinance)
    df_filtered = df_filtered.sort_index()
    df_filtered = df_filtered[~df_filtered.index.duplicated(keep='last')]

    #################PLOT
    #entra la varialbe output de process_quantile_regression que seria de QuantileRegressionLines.run_all

    # Identify the quantile regression columns that passed the correlation filter
    qr_columns = [col for col in df_filtered.columns if 'QR_' in col] # Get all QR columns

    # Create additional plots for the quantile regression lines
    add_plots = [mpf.make_addplot(df_filtered[qr_column], color='blue') for qr_column in qr_columns]

    # Current timestamp to append to the filename
    #filename = f'BTC_USDT_4h_QR_chart.png'

    # Plot the candlestick chart with the quantile regression lines
    mpf.plot(df_filtered, type='candle', addplot=add_plots, style='charles',
             title='BTC/USDT with Quantile Regression Lines', figratio=(35.0, 8.0), savefig=filename)


def plot_chart1(df_filtered, filename, interval):
    """
    Plot candlestick chart with quantile regression lines.

    Parameters
    ----------
    df_filtered : pd.DataFrame
        DataFrame with 'date' column in milliseconds and candlestick data.
    filename : str
        Output filename for saving the plot.
    interval : str
        Chart interval, e.g. '4h', '30m', '5m', '1m'.
    """

    # Ensure proper datetime index
    df_filtered = df_filtered.copy()
    df_filtered.index = pd.to_datetime(df_filtered['date'], unit='ms', utc=True)
    df_filtered = df_filtered.sort_index()
    df_filtered = df_filtered[~df_filtered.index.duplicated(keep='last')]

    # Identify quantile regression columns
    qr_columns = [col for col in df_filtered.columns if 'QR_' in col]
    add_plots = [mpf.make_addplot(df_filtered[qr_column], color='blue') for qr_column in qr_columns]

    # Dynamic title and filename
    title = f'BTC/USDT ({interval}) with Quantile Regression Lines'
    filename = f'{filename}_{interval}_QR_chart.png'

    # Plot
    mpf.plot(
        df_filtered,
        type='candle',
        addplot=add_plots,
        style='charles',
        title=title,
        figratio=(35.0, 8.0),
        savefig=filename
    )

import mplfinance as mpf
import pandas as pd
from datetime import datetime

def plot_chart2(df_filtered, interval):
    """
    Plot candlestick chart with quantile regression lines.

    Parameters
    ----------
    df_filtered : pd.DataFrame
        DataFrame with 'date' column in milliseconds and candlestick data.
    interval : str
        Chart interval, e.g. '4h', '30m', '5m', '1m'.
    """

    # Ensure proper datetime index
    df_filtered = df_filtered.copy()
    df_filtered.index = pd.to_datetime(df_filtered['date'], unit='ms', utc=True)
    df_filtered = df_filtered.sort_index()
    df_filtered = df_filtered[~df_filtered.index.duplicated(keep='last')]

    # Identify quantile regression columns
    qr_columns = [col for col in df_filtered.columns if 'QR_' in col]
    add_plots = [mpf.make_addplot(df_filtered[qr_column], color='blue') for qr_column in qr_columns]

    # Dynamic title
    title = f'BTC/USDT ({interval}) with Quantile Regression Lines'

    # Auto-generate filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'BTCUSDT_{interval}_{timestamp}_QR_chart.png'

    # Plot
    mpf.plot(
        df_filtered,
        type='candle',
        addplot=add_plots,
        style='charles',
        title=title,
        figratio=(35.0, 8.0),
        savefig=filename
    )

    print(f"Chart saved as: {filename}")



"""
original code :

#code to change the data frame to just works with the original code to plot 
#the issue is that it is expecting a index with the timestamp
import pandas as pd

def change_df_plot(df_filtered):
    # Convert 'date' from ms to datetime and set it as the index
    df_filtered = df_filtered.copy()
    df_filtered.index = pd.to_datetime(df_filtered['date'], unit='ms', utc=True)
    
    # Make sure the index is sorted and unique (good practice for mplfinance)
    df_filtered = df_filtered.sort_index()
    df_filtered = df_filtered[~df_filtered.index.duplicated(keep='last')]
    return df_filtered

df_filtered = change_df_plot(df_qr)


import pandas as pd        # for handling DataFrames
import matplotlib.pyplot as plt  # optional, but useful if you want extra customization
import mplfinance as mpf   # for candlestick and financial plotting


filename = 'temp_test'

def plot_chart(df_filtered, filename):
    #################PLOT
    #entra la varialbe output de process_quantile_regression que seria de QuantileRegressionLines.run_all

    # Identify the quantile regression columns that passed the correlation filter
    qr_columns = [col for col in df_filtered.columns if 'QR_' in col] # Get all QR columns

    # Create additional plots for the quantile regression lines
    add_plots = [mpf.make_addplot(df_filtered[qr_column], color='blue') for qr_column in qr_columns]

    # Current timestamp to append to the filename
    #filename = f'BTC_USDT_4h_QR_chart.png'

    # Plot the candlestick chart with the quantile regression lines
    mpf.plot(df_filtered, type='candle', addplot=add_plots, style='charles',
             title='BTC/USDT with Quantile Regression Lines', figratio=(35.0, 8.0))#, savefig=filename)


"""