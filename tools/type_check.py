import ast
import numpy as np
import pandas as pd

def get_actual_type(value):
    """Get the actual type of a value, parsing strings if needed."""
    # Handle numpy arrays and other iterable types that cause issues with isna()
    if isinstance(value, np.ndarray):
        return f"ndarray{value.shape}"

    # Check for None and NaN values safely
    if value is None or (not isinstance(value, (list, dict, tuple)) and pd.isna(value)):
        return "NA"

    if isinstance(value, str):
        # Check if string represents list or dict
        if (value.startswith('[') and value.endswith(']')) or \
           (value.startswith('{') and value.endswith('}')):
            try:
                parsed = ast.literal_eval(value)
                return f"str→{type(parsed).__name__}"
            except:
                return "str"
        return "str"

    return type(value).__name__

def print_detailed_info(df):
    # Print header
    print(f"Data columns (total {len(df.columns)} columns):")
    print(f" #   {'Column':<36} {'Actual type':<15} {'Preview'}")
    print(f"---  {'-'*36} {'-'*16} {'-'*50}")

    # Print each row
    for i, col in enumerate(df.columns):
        # Get non-null count
        non_null_count = df[col].count()

        # Get dtype
        dtype = df[col].dtype

        # Get sample value and actual type
        if non_null_count > 0:
            # Get first non-null value safely
            sample_series = df[col].dropna()
            if len(sample_series) > 0:
                sample = sample_series.iloc[0]
                try:
                    actual_type = get_actual_type(sample)

                    # Get a preview of the value
                    if isinstance(sample, str):
                        preview = sample[:40] + "..." if len(sample) > 40 else sample
                    else:
                        preview = str(sample)[:40] + "..." if len(str(sample)) > 40 else str(sample)
                except Exception as e:
                    actual_type = f"Error: {type(e).__name__}"
                    preview = f"Could not process: {str(e)[:30]}"
            else:
                actual_type = "Unknown"
                preview = "No accessible samples"
        else:
            actual_type = "NA"
            preview = "NA"

        # Format the output
        print(f" {i:<3} {col:<36} {actual_type:<15} {preview}")
