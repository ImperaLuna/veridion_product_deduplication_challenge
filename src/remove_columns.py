import numpy as np

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


def clean_columns(df):
    """
    Executes all column cleaning functions in a single operation.
    """
    # Pre-check if columns exist to avoid errors
    required_columns = ['product_summary', 'description', 'materials',
                        'ingredients', 'product_name', 'manufacturing_year']
    existing_columns = [col for col in required_columns if col in df.columns]

    if 'product_summary' in df.columns and 'description' in df.columns:
        merge_and_drop_descriptions(df)

    if 'materials' in df.columns and 'ingredients' in df.columns:
        combine_materials_ingredients(df)

    # Drop remaining columns in a single operation
    columns_to_drop = [col for col in ['product_name', 'manufacturing_year']
                       if col in df.columns]
    if columns_to_drop:
        drop_columns(df, columns_to_drop)

    return df