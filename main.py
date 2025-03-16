import pandas as pd
import numpy as np
from typing import Optional

from src.path import DataPaths
from src.merge import merge_dataframe_rows
from src.process_columns import clean_columns
from tools.save_data import export_dataframe


def optimized_merge(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optimized merge that pre-filters data to only process rows with potential duplicates.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with merged rows
    """
    # Identify duplicate candidates for URL+title first
    df['key1'] = df['page_url'] + '|' + df['product_title']
    duplicates_mask1 = df.duplicated(subset=['key1'], keep=False)

    # Split the dataframe
    duplicates1_df = df[duplicates_mask1].copy()
    unique1_df = df[~duplicates_mask1].copy()

    # Only merge the duplicates
    if len(duplicates1_df) > 0:
        merged1_df = merge_dataframe_rows(duplicates1_df, key_column='key1')
        merged1_df = merged1_df.drop(columns=['key1'])
    else:
        merged1_df = duplicates1_df.drop(columns=['key1'])

    # Combine with unique rows
    unique1_df = unique1_df.drop(columns=['key1'])
    df_step1 = pd.concat([merged1_df, unique1_df])

    # Now do the same for title+domain
    df_step1['key2'] = df_step1['product_title'] + '|' + df_step1['root_domain']
    duplicates_mask2 = df_step1.duplicated(subset=['key2'], keep=False)

    duplicates2_df = df_step1[duplicates_mask2].copy()
    unique2_df = df_step1[~duplicates_mask2].copy()

    if len(duplicates2_df) > 0:
        merged2_df = merge_dataframe_rows(duplicates2_df, key_column='key2')
        merged2_df = merged2_df.drop(columns=['key2'])
    else:
        merged2_df = duplicates2_df.drop(columns=['key2'])

    unique2_df = unique2_df.drop(columns=['key2'])
    final_df = pd.concat([merged2_df, unique2_df])

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
    df = pd.read_parquet(DataPaths.test_parquet)

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