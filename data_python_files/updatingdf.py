


if __name__ == "__main__":
    while True:

        if now.hour % 4 == 0 and now.minute == 0:  
            print(f"4-hour at {datetime.now()}")

        # Run 30-minute task
        if now.minute % 30 == 0:  # This will also trigger at hour mark, so no separate check for 4-hour mark necessary for 30-minute tasks
            print(f"Executing 30-minutes task at {datetime.now()}")

            #update data frame 30 min interval, update every 30 minutes
            updated_df_30m = update_dataframe_current_time(df_with_30m_intervals,'30m', interval_minutes = 30)
            if updated_df_30m is not None:

        # Run 5-minute task
        if now.minute % 5 == 0:
            print(f"Executing 5-minutes task at {datetime.now()}")
            #update data frame 5 min interval, update every 5 minutes
            updated_df_5m = update_dataframe_current_time(df_with_5m_intervals,'5m', interval_minutes = 5)
            if updated_df_5m is not None:
 
        # Run 1-minute task
        print(f"Executing 1-minute update at {datetime.now()}")
        #update data frame 1 min interval, update every minute
        updated_df_1m = update_dataframe_current_time(df_with_1m_intervals,'1m', interval_minutes = 1)
        #df_with_1m_intervals
        if updated_df_1m is not None:
