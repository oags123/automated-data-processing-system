#only at start 


#THIS ONE IS ONLY FOR THE FIRST RUN 
def bootstrap_hierarchy(symbol):
    short_symbol = symbol[:3]  # Extract the first three letters (BTC)

    ### 1. Run 4h
    start_time_4h = get_past_time_4h_iso8601()  # Subtract 2000 hours
    df_4h = fetch_prices_get_df('4h', symbol, start_time_4h, start_time_count=0)
    regression_analysis('4h', symbol, df_4h)
    #start_time_30m =

    ### 2. Run 30m — use output of 4h

    df_30m = fetch_prices_get_df('30m', symbol, start_time_30m, start_time_count=0)
    regression_analysis('30m', symbol, df_30m)
    #start_time_5m =


    ### 3. Run 5m — use output of 30m

    df_5m = fetch_prices_get_df('5m', symbol, start_time_5m, start_time_count=0)
    regression_analysis('5m', symbol, df_5m)
    #start_time_1m =

    ### 4. Run 1m — use output of 5m

    start_time_1m = get_start_time(table_1m)
    df_1m = fetch_prices_get_df('1m', symbol, start_time_1m, start_time_count=0)
    regression_analysis('1m', symbol, df_1m)


