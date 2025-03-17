import pandas as pd
import numpy as np
from typing import Optional

from src.path import DataPaths
from src.merge import merge_dataframe_rows
from src.process_columns import clean_columns
from tools.save_data import export_dataframe


def optimized_merge(df: pd.DataFrame) -> pd.DataFrame:
    """
    Product-centric optimized merge that consolidates products regardless of vendor.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with merged rows
    """
    # Create a product-focused key using only product_title
    # You could include other product attributes here if needed
    df['product_key'] = df['product_title']

    # Identify duplicate products
    duplicates_mask = df.duplicated(subset=['product_key'], keep=False)

    # Split the dataframe
    duplicates_df = df[duplicates_mask].copy()
    unique_df = df[~duplicates_mask].copy()

    # Only merge the duplicates
    if len(duplicates_df) > 0:
        merged_df = merge_dataframe_rows(duplicates_df, key_column='product_key')
        merged_df = merged_df.drop(columns=['product_key'])
    else:
        merged_df = duplicates_df.drop(columns=['product_key'])

    # Combine with unique rows
    unique_df = unique_df.drop(columns=['product_key'])
    final_df = pd.concat([merged_df, unique_df])

    return final_df


def main() -> pd.DataFrame:
    """
    Main function to perform deduplication on the dataset using the optimized approach.

    This function:
    1. Loads the original data
    2. Cleans the columns using the clean_columns function
    3. Applies the optimized merge function to deduplicate the data
    4. Saves the final deduplicated dataset

    Returns:
        pd.DataFrame: The deduplicated DataFrame
    """
    # Load the original data
    df = pd.read_parquet(DataPaths.file_parquet_original)

    # Clean the columns
    df = clean_columns(df)

    # Apply the optimized merge
    result_df = optimized_merge(df)

    # Export the final data
    export_dataframe(result_df, DataPaths.parquet_final_dir, 'final_data', file_format='parquet')
    export_dataframe(result_df, DataPaths.visualization_final_dir, 'final_data', file_format='csv')
    return result_df


if __name__ == "__main__":
    main()