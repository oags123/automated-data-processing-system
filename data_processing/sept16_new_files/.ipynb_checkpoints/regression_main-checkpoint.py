from datetime import datetime, timezone, timedelta
from datetime import datetime, timezone, timedelta
from segments import RegressionSegments
from quantreg import QuantileRegressionLines
import line_properties
import plot_chart

#run calculate segments, linear regression, quantile regression, plot lines and get the lines properties 
def run_regression_lines(df, time_interval):
    #linear regression segments process
    segments = RegressionSegments(df) 
    
    # Step 2: Call the run_all method
    regression_segmentation = segments.segmentation_processes()
    regression_segmentation2 = segments.remove_duplicates_preserve_order(regression_segmentation)
    #print(regression_segmentation2)
    
    #quantile regression process
    qr = QuantileRegressionLines(df, regression_segmentation2)
    
    df_qr = qr.run_all()
    #print(df_qr)
    # df_qr goes to plot chart
    
    #Line properties
    lines_properties = line_properties.calculate_and_add_properties(df_qr)
    lines_coordinates = line_properties.calculate_and_add_coordinates_only(df_qr)
    start_time_lower_interval = line_properties.format_datetime_intervals(regression_segmentation2, df, time_interval)
    start_time_str = start_time_lower_interval.loc[0, 'start_time']
    #filename = 'temp_test'
    plot_chart.plot_chart3(df_qr, time_interval)
    return start_time_str


def get_past_time_4h_iso8601(current_time):
    """
    Returns an ISO 8601 timestamp 2000 hours in the past
    from the current time in milliseconds.
    """
    # Current time in milliseconds
    #current_time = int(time.time() * 1000)
    
    # Convert to seconds
    current_datetime = datetime.fromtimestamp(current_time / 1000, tz=timezone.utc)
    
    # Subtract 2000 hours
    past_datetime = current_datetime - timedelta(hours=2000)
    
    # Return ISO 8601 formatted string
    return past_datetime.isoformat()

# Example usage:
#start_time = get_past_time_4h_iso8601()
#print("Start time:", start_time)
