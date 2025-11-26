import numpy as np
from sklearn.linear_model import LinearRegression

class RegressionSegments:
    def __init__(self, df, min_segment_length=60):
        """
        Initialize the RegressionSegments class with a DataFrame.
        
        Parameters:
        - df: DataFrame containing the data
        """
        self.df = df
        self.min_segment_length = min_segment_length

    def calculate_ssr(self, model, X, y):
        """
        Calculate the sum of squared residuals (SSR) for a given model.
    
        Parameters:
        - model: The regression model
        - X: Independent variables
        - y: Dependent variable
    
        Returns:
        - ssr: Sum of squared residuals
        """
        predictions = model.predict(X)
        residuals = y - predictions
        ssr = np.sum(residuals ** 2)
        return ssr
    
    def find_optimal_segmentation(self, start=0, end=None):
        """
        Find the optimal segmentation point that minimizes the error within two segments.
        Ensures the final segment reaches the end of the DataFrame.
    
        Parameters:
        - df: DataFrame containing the data
        - start: Start index for segmentation
        - end: End index for segmentation
        - min_segment_length: Minimum length of a segment
    
        Returns:
        - A list of tuples representing segment start and end points
        - The minimum error of the segmentation
        """
        if end is None:
            end = self.df['time_count'].max()
        if end - start <= self.min_segment_length:  # Early stop condition
            return [(start, end)], float('inf')  # Ensure to return the final segment even if it's short
    
        min_error = float('inf')
        optimal_segmentation_point = None
    
        # Iterate through possible segmentation points
        for point in range(start + self.min_segment_length, end - self.min_segment_length + 1):
            first_segment = self.df.iloc[start:point]
            second_segment = self.df.iloc[point:end]
    
            X1 = first_segment[['time_count']]
            X2 = second_segment[['time_count']]
    
            model1 = LinearRegression().fit(X1, first_segment['close'])
            model2 = LinearRegression().fit(X2, second_segment['close'])
    
            error1 = self.calculate_ssr(model1, X1, first_segment['close'])
            error2 = self.calculate_ssr(model2, X2, second_segment['close'])
            total_error = error1 + error2
    
            if total_error < min_error:
                min_error = total_error
                optimal_segmentation_point = point
    
        # Always include the last segment
        if optimal_segmentation_point is not None:
            return [(start, optimal_segmentation_point), (optimal_segmentation_point, end)], min_error
        else:
            # In case no segmentation point improves the error, consider the whole range as one segment
            return [(start, end)], min_error
    
    def filter_segments(self, segments, max_time_count):
        """
        Filter out segments to ensure they cover the entire DataFrame without overlap or gaps.
    
        Parameters:
        - segments: List of tuples representing segment start and end points
        - max_time_count: Total length of the DataFrame
    
        Returns:
        - List of unique segments that cover the entire DataFrame
        """
        #print(f"Segments before filtering: {segments}")
    
        # Ensure last segment ends at the DataFrame's length
        filtered_segments = [seg for seg in segments if seg[1] == max_time_count]
    
        # Remove duplicates
        unique_segments = list(set(filtered_segments))
    
        #print(f"Segments after filtering: {unique_segments}")
        return unique_segments
    
    def find_deeper_segmentations(self, segments, depth):
        """
        Recursively find deeper segmentations, ensuring segments include the last data point.
    
        Parameters:
        - df: DataFrame containing the data
        - segments: Initial list of segments to further segment
        - depth: Depth of recursion
        - min_segment_length: Minimum length of a segment
    
        Returns:
        - List of segments after deeper segmentation
        """
        if depth == 0 or not segments:
            return segments
    
        new_segments = []
        for segment in segments:
            start, end = segment
            if end - start > self.min_segment_length:
                sub_segments, _ = self.find_optimal_segmentation(start, end)
                deeper_segments = self.find_deeper_segmentations(sub_segments, depth - 1)
                new_segments.extend(deeper_segments)
    
        final_segments = self.filter_segments(new_segments, self.df['time_count'].max())
        return final_segments

    def segmentation_processes(self):
    
        # Initial optimal segmentation
        primary_segments, _ = self.find_optimal_segmentation(0, (self.df['time_count'].max()))
        primary_segments = self.filter_segments(primary_segments, (self.df['time_count'].max()))  # Post-processing to filter segments
        #print(f"Primary optimal segmentations: {primary_segments}")
    
        # Further segmentation processes
        # Adjusting the depth as needed for deeper segmentations
        secondary_segments = self.find_deeper_segmentations(primary_segments, 1)
        #print(f"Secondary segmentations: {secondary_segments}")
    
        # Continue with tertiary, quaternary, quinary segmentation as needed
        # Continuing from the secondary segmentation
        # Tertiary Segmentation
        tertiary_segments = self.find_deeper_segmentations(secondary_segments, 1)
        #print(f"Tertiary segmentations: {tertiary_segments}")
    
        # Quaternary Segmentation
        quaternary_segments = self.find_deeper_segmentations(tertiary_segments, 1)
        #print(f"Quaternary segmentations: {quaternary_segments}")
    
        # Quinary Segmentation
        quinary_segments = self.find_deeper_segmentations(quaternary_segments, 1)
        #print(f"Quinary segmentations: {quinary_segments}")
    
        #_segments = four_hours_total_segment + primary_segments + secondary_segments + tertiary_segments + quaternary_segments + quinary_segments
        total_segments = primary_segments + secondary_segments + tertiary_segments + quaternary_segments + quinary_segments
    
        return total_segments

    @staticmethod
    def remove_duplicates_preserve_order(tuple_list):
        """
        Remove duplicate tuples from a list while preserving the order.
    
        Parameters:
        - tuple_list: List of tuples, where duplicates may exist.
    
        Returns:
        - A list of tuples with duplicates removed, order preserved.
        """
        seen = set()  # Set to store seen tuples
        result = []  # List to store the final result without duplicates
        for item in tuple_list:
            # Convert the tuple to a hashable type (itself, since tuples are hashable)
            # and check if it's not in seen set
            if item not in seen:
                # Add item to result and mark as seen
                result.append(item)
                seen.add(item)
        return result

