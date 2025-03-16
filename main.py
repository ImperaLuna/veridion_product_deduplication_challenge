import pandas as pd
import numpy as np
from typing import Optional

from src.path import DataPaths
from src.merge import merge_dataframe_rows


def load_data() -> Optional[pd.DataFrame]:
    """
    Loads the data from the parquet file.

    Returns:
        Optional[pd.DataFrame]: The loaded DataFrame or None if an error occurred
    """
    try:
        df = pd.read_parquet(DataPaths.file_parquet_original)
        return df
    except FileNotFoundError:
        print(f"Error: The file at {DataPaths.file_parquet_original} was not found.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        return None





def main() -> None:
    """
    Main function.
    """
    raw_df = load_data()
    if raw_df is not None:
        cleaned_df = clean_data(raw_df)
        print(f"Data cleaned successfully. Shape: {cleaned_df.shape}")
        # Do something with cleaned_df here, e.g., save it or process it further
    else:
        print("Could not clean data because loading failed.")


if __name__ == "__main__":
    main()