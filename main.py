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


def clean_data(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """
    Cleans the provided DataFrame by processing energy_efficiency column.

    Args:
        df (Optional[pd.DataFrame]): The DataFrame to clean

    Returns:
        Optional[pd.DataFrame]: The cleaned DataFrame or None if input was None
    """
    if df is None:
        return None

    # Create a copy to avoid modifying the original DataFrame
    cleaned_df = df.copy()

    # Process the energy_efficiency column
    cleaned_df['energy_efficiency'] = cleaned_df['energy_efficiency'].apply(
        lambda x: (
            []
            if x is None or (isinstance(x, list) and x == [None])
            else np.array([x]) if not isinstance(x, list) else np.array(x)
        )
    )

    return cleaned_df


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