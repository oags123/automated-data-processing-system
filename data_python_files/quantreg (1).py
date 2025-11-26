import pandas as pd
import warnings
from statsmodels.regression.quantile_regression import QuantReg
from statsmodels.tools.sm_exceptions import ConvergenceWarning
import statsmodels.api as sm
import statsmodels.formula.api as smf

class QuantileRegressionLines:
    def __init__(self, df, segments):
        self.df = df
        self.segments = segments
    
    # Modified function to perform quantile regression based on time_count
    def quantile_regression(self, df, start_minute, end_minute, quantile):
        df_timeframe = df[df['time_count'].between(start_minute, end_minute)]
        X = df_timeframe['time_count']
        y = df_timeframe['close']
        X_const = sm.add_constant(X)  # Add a constant to the independent variable
        model = sm.QuantReg(y, X_const)
        fitted_model = model.fit(q=quantile, max_iter=1000)
        return fitted_model.predict(X_const)  # Return the predictions
    
    # Function to perform the quantile regression for all segments and return the results
    def perform_quantile_regressions(self, df, segments):
        warnings.filterwarnings("ignore", category=ConvergenceWarning)
        warnings.filterwarnings("ignore", category=UserWarning, module="statsmodels.regression.quantile_regression")
    
        qr_results = []
        # Initialize an empty DataFrame to store summary of each quantile regression
        qr_summary_df = pd.DataFrame(columns=['QR_Name', 'Start_DateTime', 'End_DateTime', 'Start_Minute', 'End_Minute', 'Segment'])
    
        for i, (start_minute, end_minute) in enumerate(segments):
            fitted_values_05 = self.quantile_regression(df, start_minute, end_minute, 0.05)
            fitted_values_95 = self.quantile_regression(df, start_minute, end_minute, 0.95)
            qr_results.append(pd.DataFrame({f'QR_{i}_05': fitted_values_05, f'QR_{i}_95': fitted_values_95}))
    
            # Check if the time_count values exist in the DataFrame to avoid AttributeError
            if df['time_count'].isin([start_minute, end_minute]).any():
                summary = {
                    'QR_Name': f'QR_{i}_05 and QR_{i}_95',
                    'Start_DateTime': df[df['time_count'] == start_minute].index.min(),
                    'End_DateTime': df[df['time_count'] == end_minute].index.max(),
                    'Start_Minute': start_minute,
                    'End_Minute': end_minute,
                    'Segment': (start_minute, end_minute)  # Added new column for the segment
                }
    
    
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", FutureWarning)
                    # Place the code that causes warnings here
                    qr_summary_df = pd.concat([qr_summary_df, pd.DataFrame([summary])], ignore_index=True)
                
    
        # Concatenate all results and join with the original DataFrame
        return pd.concat([df] + qr_results, axis=1), qr_summary_df  # Also return the summary DataFrame
    
    #filtering QR lines
    def pearson_correlation_check(self, df_filtered):
        
        # Define your correlation threshold
        correlation_threshold = 0.50
        
        # List to hold the names of quantile regression line columns to drop
        columns_to_drop = []
        
        # Calculate correlation and filter columns
        for column in df_filtered.columns:
            if 'QR_' in column:  # Check if the column is a quantile regression line
                correlation = df_filtered['close'].corr(df_filtered[column])
    
                # Print the correlation if it is above the threshold
                if correlation >= correlation_threshold:
                    None
                    #print(f"{column}: {correlation}")
                else:
                    columns_to_drop.append(column)
                    # Optionally, print a message for columns below the threshold
                    #print(f"Column {column} has correlation {correlation} which is below the threshold.")
    
        # Drop the columns that do not meet the correlation threshold
        df_filtered.drop(columns=columns_to_drop, inplace=True)
        
        # Continue with your analysis or plotting
        return df_filtered
    
    #filtering QR lines
    def drop_QR_keep2(self, df):
        # Assuming df is your DataFrame
        # df = pd.read_csv('your_data.csv')  # Load your DataFrame
        
        # Identify all Quantile Regression columns
        qr_columns = [col for col in df.columns if 'QR_' in col]
        
        # Group the columns into pairs
        qr_pairs = {}
        for col in qr_columns:
            key = '_'.join(col.split('_')[:2])  # Group by the first two parts of the column name
            qr_pairs.setdefault(key, []).append(col)
        
        # While there are more than 2 pairs, drop the pair with the smallest Time_count span
        while len(qr_pairs) > 2:
            # Find the Time_count span for each pair
            time_span = {}
            for key, cols in qr_pairs.items():
                valid_index = df[cols].dropna().index
                if not valid_index.empty:
                    time_span[key] = (valid_index.min(), valid_index.max())
        
            # Identify the pair with the smallest Time_count span
            smallest_span_key = min(time_span, key=lambda k: time_span[k][1] - time_span[k][0])
        
            # Drop the pair with the smallest Time_count span from the DataFrame
            df.drop(qr_pairs[smallest_span_key], axis=1, inplace=True)
        
            # Update the pairs dictionary to reflect the dropped pair
            del qr_pairs[smallest_span_key]
        
        # Resulting DataFrame will have at most two pairs of Quantile Regression lines
        #print(df.head())
        return df
    
    def calculate_50th_quantile_regression(self, df):
        # Assume that each QR pair has start and end times defined in separate columns
        qr_pairs = set(col.split('_')[1] for col in df.columns if col.startswith('QR_') and col.endswith('05') or col.endswith('95'))
    
        for pair in qr_pairs:
            # Columns for the 5th and 95th quantiles
            qr_05_col = f'QR_{pair}_05'
            qr_95_col = f'QR_{pair}_95'
            
            # Check if both quantile columns exist in the dataframe
            if qr_05_col in df.columns and qr_95_col in df.columns:
                # Identify start and end times for the quantile regression pair
                start_time = df.loc[df[qr_05_col].first_valid_index(), 'time_count']
                end_time = df.loc[df[qr_95_col].last_valid_index(), 'time_count']
    
                # Create a subset of the dataframe based on the identified times
                subset_df = df[(df['time_count'] >= start_time) & (df['time_count'] <= end_time)]
    
                # Calculate the 50th quantile regression using the subset
                formula = 'close ~ time_count'
                model = smf.quantreg(formula, subset_df)
                res = model.fit(q=0.5)
    
                # Add the 50th quantile regression result to the original dataframe
                df.loc[subset_df.index, f'QR_{pair}_50'] = res.predict(subset_df['time_count'])
    
        return df
    
    def remove_50th_quantile_regression(self, df):
        # List all columns that are 50th quantile regressions
        qr_50_columns = [col for col in df.columns if col.endswith('_50') and col.startswith('QR_')]
        
        # Drop these columns from the dataframe
        df = df.drop(columns=qr_50_columns)
        
        return df
    
    def process_quantile_regression(self, df):
        df = self.calculate_50th_quantile_regression(df)
        qr_columns = [col for col in df.columns if col.startswith('QR_')]
        qr_pairs = {}
    
        # Pair the quantile columns
        for col in qr_columns:
            num = col.split('_')[1]  # Extract the number identifier
            if num not in qr_pairs:
                qr_pairs[num] = {'50': None, '05': None, '95': None}
            if col.endswith('50'):
                qr_pairs[num]['50'] = col
            elif col.endswith('05'):
                qr_pairs[num]['05'] = col
            elif col.endswith('95'):
                qr_pairs[num]['95'] = col
    
        drop_list = []
    
        # Process each pair of quantile columns
        for num, cols in qr_pairs.items():
            # For the 50th quantile
            if '50' in cols and cols['50'] in df.columns:
                col_name_50 = cols['50']
                # Calculate errors excluding the last 10 intervals
                errors_excl_last_10 = abs(df[col_name_50][:-10] - df['close'][:-10])          
                mae_excl_last_10 = errors_excl_last_10.max()
                # Calculate the max error for the last interval
                max_error_last_interval = abs(df[col_name_50].iloc[-1] - df['close'].iloc[-1])
                #print(f"max_error_last_interval: {max_error_last_interval} max error_excl_last_10: {mae_excl_last_10}")
                # Check the condition for dropping the pair
                if max_error_last_interval > 1.5 * mae_excl_last_10:
                    #print('Last interval more than 1.5 than max error excluding last10')
                    #print(f"max_error_last_interval: {max_error_last_interval} mae_excl_last_10: {mae_excl_last_10} dropping {num}")
                    drop_list.append(num)
    
            # For the 05 and 95 quantiles
            mae_values = {}
            for q in ['05', '95']:
                if q in cols and cols[q] in df.columns:
                    col_name = cols[q]
                    errors = abs(df[col_name] - df['close'])
                    mae = errors.mean()
                    mae_values[q] = mae
    
            # Compare the MAEs of 05 and 95 quantile columns and check drop condition
            if '05' in mae_values and '95' in mae_values:
                if mae_values['05'] > 2 * mae_values['95'] or mae_values['95'] > 2 * mae_values['05']:
                    #print('Compare the MAEs of 05 and 95 quantile columns and check drop condition')
                    #print(f"mae_values['05']: {mae_values['05']} mae_values['95']: {mae_values['95']} dropping {num}")
                    
                    drop_list.append(num)
    
        # Drop the marked columns
        for num in drop_list:
            for col in ['50', '05', '95']:
                if num in qr_pairs and col in qr_pairs[num]:
                    df.drop(columns=[qr_pairs[num][col]], inplace=True, errors='ignore')
        df = self.remove_50th_quantile_regression(df)
        return df

    def run_all(self):
        #order 
        df_filtered = self.df
        filtered_segments =  self.segments
        #0.- Input to class data frame with time counts and tuple list with segments from regression segments
        #1.- 
        df_filtered, qr_summary = self.perform_quantile_regressions(df_filtered, filtered_segments)
        #2.- 
        df_filtered = self.pearson_correlation_check(df_filtered) 
        #3.- 
        df_filtered = self.drop_QR_keep2(df_filtered)
        #4.- 
        df_quant_reg = self.process_quantile_regression(df_filtered)
        #get only the last row of the data frame to check latest QR prices 
        lastrow_df_filtered = df_quant_reg.tail(1).copy(deep=True)

        return df_quant_reg
        
        
