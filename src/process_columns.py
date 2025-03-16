import numpy as np
import pandas as pd


def merge_and_drop_descriptions(df):
    """
    Merge description and product_summary columns based on the longest string,
    modifying the DataFrame in place.

    Args:
        df: DataFrame with product_title, description, and product_summary columns

    Returns:
        None (modifies the DataFrame in place)
    """
    # Create a new column with the longest text between description and product_summary
    df['product_description'] = df.apply(
        lambda row: row['description'] if len(str(row['description'])) >= len(str(row['product_summary']))
                   else row['product_summary'],
        axis=1
    )

    # Handle any None/NaN values
    df['product_description'] = df['product_description'].fillna('')

    # Drop the original description and product_summary columns
    df.drop(['description', 'product_summary'], axis=1, inplace=True)

    return df  # Return the modified DataFrame for convenience


def combine_materials_ingredients(df):
    """
    Combines 'materials' and 'ingredients' columns into a new 'components' column.
    """
    # Initialize with materials column directly
    df['components'] = df['materials']

    # Replace apply with vectorized operations
    materials_empty = df['materials'].str.len() == 0
    ingredients_nonempty = df['ingredients'].str.len() > 0
    mask = materials_empty & ingredients_nonempty

    # Conditional assignment only where needed
    if mask.any():
        df.loc[mask, 'components'] = df.loc[mask, 'ingredients']

    # Drop columns
    df.drop(['materials', 'ingredients'], axis=1, inplace=True)
    return df


def drop_columns(df, columns):
    """
    Drop specified columns from a DataFrame.

    Parameters:
    df (pandas.DataFrame): The input DataFrame
    columns (list): List of column names to drop

    Returns:
    pandas.DataFrame: Modified DataFrame
    """
    df.drop(columns, axis=1, inplace=True)
    return df


def clean_energy_efficiency(df):

    df['energy_efficiency'] = df['energy_efficiency'].apply(
        lambda x:
        [] if x is None or (isinstance(x, list) and x == [None]) else
        np.array([x]) if not isinstance(x, list) else
        np.array(x)
    )

    return df

def clean_columns(df):
    """
    Executes all column cleaning functions in a single operation:
    1. Merges product_summary and description into product_description
    2. Combines materials and ingredients into components
    3. Cleans energy_efficiency data
    4. Drops product_name and manufacturing_year columns

    Parameters:
    df (pandas.DataFrame): The input DataFrame with columns to clean

    Returns:
    pandas.DataFrame: The modified DataFrame with transformed columns
    """
    if 'product_summary' in df.columns and 'description' in df.columns:
        merge_and_drop_descriptions(df)
    if 'materials' in df.columns and 'ingredients' in df.columns:
        combine_materials_ingredients(df)

    clean_energy_efficiency(df)

    columns_to_drop = [col for col in ['product_name', 'manufacturing_year']
                       if col in df.columns]
    if columns_to_drop:
        drop_columns(df, columns_to_drop)

    return df