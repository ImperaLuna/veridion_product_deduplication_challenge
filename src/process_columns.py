import numpy as np
import pandas as pd

def merge_and_drop_descriptions(df):
    """
    Merges product_summary and description columns using vectorized operations.
    Keeps the longest string between the two columns and drops the original columns.
    """
    # Use vectorized string operations instead of astype(str)
    summary = df['product_summary'].fillna('')
    description = df['description'].fillna('')

    # Use NumPy for faster comparison
    summary_len = summary.str.len().values
    description_len = description.str.len().values

    # Create boolean array directly with NumPy
    use_summary = (summary_len >= description_len) & (summary_len > 0)

    # Use NumPy where directly with Series values for better performance
    df['product_description'] = np.where(
        use_summary,
        summary.values,
        np.where(description_len > 0, description.values, None)
    )

    # Drop columns with a single operation
    df.drop(['product_summary', 'description'], axis=1, inplace=True)
    return df


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
    """
    Cleans the provided DataFrame by processing energy_efficiency column.

    Args:
        df (Optional[pd.DataFrame]): The DataFrame to clean

    Returns:
        Optional[pd.DataFrame]: The cleaned DataFrame or None if input was None
    """
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