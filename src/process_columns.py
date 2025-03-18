"""
Column Cleaning Utilities
------------------------------
Usage:
   from src.process_column import clean_columns

   * Apply all cleaning transformations
   clean_df = clean_columns(product_dataframe)
"""

import numpy as np
import pandas as pd
from typing import List


def merge_and_drop_descriptions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge description and product_summary columns based on the longest string.

    Args:
        df: DataFrame with description, and product_summary columns

    Returns:
        df: DataFrame
    """
    # Create a new column with the longest text between description and product_summary
    df['product_description'] = df.apply(
        lambda row: row['description'] if len(str(row['description'])) >= len(str(row['product_summary']))
                                       else row['product_summary'],axis=1
    )

    df['product_description'] = df['product_description'].fillna('')

    df.drop(['description', 'product_summary'], axis=1, inplace=True)

    return df


def combine_materials_ingredients(df: pd.DataFrame) -> pd.DataFrame:
    """
    Combines 'materials' and 'ingredients' columns into a new 'components' column.

    Args:
        df: DataFrame with ingredients, and materials columns
    Returns:
        df: DataFrame with new column components dropping initial columns
    """
    df['components'] = df['materials']

    # Find rows where materials column is empty (length = 0)
    materials_empty = df['materials'].str.len() == 0

    # Find rows where ingredients column has content (length > 0)
    ingredients_nonempty = df['ingredients'].str.len() > 0

    # Create a mask for rows where we should use ingredients instead:
    # When materials is empty AND ingredients has content
    mask = materials_empty & ingredients_nonempty

    if mask.any():
        df.loc[mask, 'components'] = df.loc[mask, 'ingredients']

    df.drop(['materials', 'ingredients'], axis=1, inplace=True)
    return df


def drop_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    Drop specified columns from a DataFrame.

    Args:
        df: The input DataFrame
        columns (list): List of column names to drop

    Returns:
        df: Modified DataFrame with columns dropped
    """
    df.drop(columns, axis=1, inplace=True)
    return df


def clean_energy_efficiency(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform energy_efficiency column values from dictionary values to numpy arrays

    Args:
        df: The input DataFrame with an energy_efficiency column

    Returns:
        df: Modified DataFrame with energy_efficiency values converted to np.array
    """
    df['energy_efficiency'] = df['energy_efficiency'].apply(
        lambda x:
        [] if x is None or (isinstance(x, list) and x == [None]) else # Handle None values
        np.array([x]) if not isinstance(x, list) else                 # Wrap dict values into an array
        np.array(x)                                                   # Convert lists to arrays
    )

    return df

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Executes all column cleaning functions in a single operation:

    * Merges product_summary and description into product_description
    * Combines materials and ingredients into components
    * Cleans energy_efficiency data
    * Drops product_name and manufacturing_year columns

    Parameters:
        df : The input DataFrame with columns to clean

    Returns:
        df: The modified DataFrame with transformed columns
    """
    if 'product_summary' in df.columns and 'description' in df.columns:
        merge_and_drop_descriptions(df)
    if 'materials' in df.columns and 'ingredients' in df.columns:
        combine_materials_ingredients(df)
    if 'energy_efficiency' in df.columns:
        clean_energy_efficiency(df)

    # Create a list of columns to drop, but only include columns that actually exist in the DataFrame
    columns_to_drop = [col for col in ['product_name', 'manufacturing_year']
                       if col in df.columns]
    if columns_to_drop:
        drop_columns(df, columns_to_drop)

    return df