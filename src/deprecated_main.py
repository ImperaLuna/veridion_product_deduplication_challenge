import pandas as pd
from typing import Optional

from src.path import DataPaths
from src.merge import merge_dataframe_rows
from tools.save_data import export_dataframe


def merge_by_url_and_title(df: pd.DataFrame) -> None:
    """
    Merges duplicate rows based on page_url and product_title, modifying the dataframe in place.

    Args:
        df: Input DataFrame to be modified
    """
    # Create a key by combining page_url and product_title
    df['key'] = df['page_url'] + '|' + df['product_title']

    # Merge the rows and assign back to the same variable
    df_merged = merge_dataframe_rows(df, key_column='key')

    # Update all rows in the original dataframe with the merged results
    df.drop(index=df.index, inplace=True)
    df._update_inplace(df_merged)

    # Drop the key column
    df.drop(columns=['key'], inplace=True)


def merge_by_title_and_domain(df: pd.DataFrame) -> None:
    """
    Merges duplicate rows based on product_title and root_domain, modifying the dataframe in place.

    Args:
        df: Input DataFrame to be modified
    """
    # Create a key by combining product_title and root_domain
    df['key'] = df['product_title'] + '|' + df['root_domain']

    # Merge the rows and assign back to the same variable
    df_merged = merge_dataframe_rows(df, key_column='key')

    # Update all rows in the original dataframe with the merged results
    df.drop(index=df.index, inplace=True)
    df._update_inplace(df_merged)

    # Drop the key column
    df.drop(columns=['key'], inplace=True)


def main() -> pd.DataFrame:
    """
    Main function to perform deduplication on the dataset.

    This function:
    1. Loads the original data
    2. Performs deduplication by merging rows with the same page_url and product_title
    3. Performs deduplication by merging rows with the same product_title and root_domain
    4. Saves the final deduplicated dataset

    Returns:
        pd.DataFrame: The deduplicated DataFrame
    """
    # Load the original data
    df = pd.read_parquet(DataPaths.file_parquet_original)

    # Step 1: Merging by common key, page_url and product_title
    merge_by_url_and_title(df)

    # Step 2: Merging by common key, product_title and root_domain
    merge_by_title_and_domain(df)

    # Export the final data
    export_dataframe(df, DataPaths.parquet_final_dir, 'final_data', file_format='parquet')

    return df


if __name__ == "__main__":
    main()