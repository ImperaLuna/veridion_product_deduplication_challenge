"""
Data Merging Module
------------------------------
Merges rows in a DataFrame that share the same key value,
combining column values using appropriate aggregation functions.

Usage:
  from src.merge import merge_dataframe_rows

  * Merge rows with the same key value
  merged_df = merge_dataframe_rows(dataframe, key_column="product_id")
"""

import pandas as pd
import numpy as np
import json

from urllib.parse import urlparse

from typing import Dict, Callable, List, Union, Set, Optional, Any, TypeVar

from src.path import DataPaths

# Type aliases for better readability
ArrayLike = Union[np.ndarray, List[Any]]
ValueSeries = pd.Series
ScalarValue = Union[str, int, float, bool, None]
T = TypeVar('T')

# ========== Scalar Column Handling Functions ==========

def merge_unspsc(values: ValueSeries) -> Optional[str]:
    """
    Merge UNSPSC values with '|' separator.

    Handles values that may already contain '|' by splitting them,
    then combines all unique values with the same separator.

    Parameters:
        values: Series of UNSPSC values to merge

    Returns:
        String of unique UNSPSC codes separated by '|', or None if no valid values
    """
    if values.empty:
        return ""

    # Split any values that already contain ' | '
    all_values: List[str] = []
    for val in values:
        if pd.notna(val):
            if isinstance(val, str) and ' | ' in val:
                all_values.extend([v.strip() for v in val.split(' | ')])
            else:
                all_values.append(str(val).strip())

    # Remove duplicates and empty values
    unique_values: List[str] = sorted(set(v for v in all_values if v and v != 'nan'))
    return ' | '.join(unique_values) if unique_values else ""

def merge_root_domain(values: ValueSeries) -> str:
    """
    Collect all unique root_domain values associated with a product
    and join them with a pipe separator.

    Parameters:
        values: Series of domain values to collect

    Returns:
        String of unique domain values separated by " | ", empty string if no valid values
    """
    if values.empty:
        return ""

    # Filter out nulls and get unique values
    non_null: List[str] = [v for v in values if pd.notna(v) and v]

    if not non_null:
        return ""

    # Return unique values joined with pipe separator
    return " | ".join(sorted(set(non_null)))

def merge_page_url(values: ValueSeries) -> str:
    """
       Merge page URLs by selecting the shortest URL for each domain.

       Parameters:
           values: Series of URL values to merge

       Returns:
           String of unique shortest URLs separated by " | ", empty string if no valid values
       """
    if values.empty:
        return ""

    # Filter out nulls
    non_null = [v for v in values if pd.notna(v) and v]

    if not non_null:
        return ""

    # Dictionary to store shortest URL for each domain
    domain_to_shortest_url = {}

    for url in non_null:
        try:
            parsed = urlparse(url)
            domain = parsed.netloc

            # Skip URLs that can't be parsed correctly
            if not domain:
                continue

            # If we haven't seen this domain yet, or if this URL is shorter
            if (domain not in domain_to_shortest_url or
                    len(url) < len(domain_to_shortest_url[domain])):
                domain_to_shortest_url[domain] = url
        except:
            # Skip any URLs that cause parsing errors
            continue

    # Return unique values joined with pipe separator
    if not domain_to_shortest_url:
        return ""

    return " | ".join(sorted(domain_to_shortest_url.values()))

def merge_text_longest(values: ValueSeries) -> Optional[str]:
    """
    Return the longest valid text string from a series.

    Used for fields where preserving the most detailed information is preferred.

    Parameters:
        values: Series of text values to compare

    Returns:
        The longest text string, or None if no valid strings
    """
    if values.empty:
        return None

    valid_strings: List[str] = [s for s in values if pd.notna(s) and isinstance(s, str) and s]
    return max(valid_strings, key=len) if valid_strings else None

def merge_text_shortest(values: ValueSeries) -> Optional[str]:
    """
    Return the shortest valid text string from a series.

    Used for fields like URLs where the shortest/canonical form is preferred.

    Parameters:
        values: Series of text values to compare

    Returns:
        The shortest text string, or None if no valid strings
    """
    if values.empty:
        return None

    valid_strings: List[str] = [s for s in values if pd.notna(s) and isinstance(s, str) and s]
    return min(valid_strings, key=len) if valid_strings else None

def merge_eco_friendly(values: ValueSeries) -> Optional[bool]:
    """
    Handle eco_friendly: preserve if unique, don't merge if conflicting

    Parameters:
        values: Series of boolean values indicating eco-friendly status

    Returns:
        True if all non-null values are True
        False if all non-null values are False
        None if all values are null or empty
        Raises ValueError if both True and False values are present
    """
    if values.empty:
        return None

    # Filter out nulls
    non_null = [v for v in values if v is not None and not pd.isna(v)]

    if not non_null:
        return None

    # Check for both True and False values
    has_true = any(v is True for v in non_null)
    has_false = any(v is False for v in non_null)

    # If we have both True and False, raise ValueError
    if has_true and has_false:
        raise ValueError('Different eco_friendly values')

    # Return the single value type we have
    if has_true:
        return True
    if has_false:
        return False

    return None

def merge_max_year(values: ValueSeries) -> Optional[int]:
    """
    Return the maximum year value from a series.

    Used for fields where the latest/most recent year is preferred.

    Parameters:
        values: Series of year values

    Returns:
        The maximum year value, or None if no valid years
    """
    if values.empty:
        return None

    non_null: List[int] = [y for y in values if pd.notna(y)]
    return max(non_null) if non_null else None


def get_scalar_aggregation_dict() -> Dict[str, Callable[[ValueSeries], Optional[ScalarValue]]]:
    """
    Create a dictionary mapping scalar columns to their aggregation functions.

    Maps each known scalar column name to the appropriate specialized
    aggregation function based on the column's data type and merging requirements.

    The function will include all potential columns, and the calling function
    will filter out those that don't exist in the actual DataFrame.

    Returns:
        Dictionary mapping column names to aggregation functions
    """
    # Predefined scalar columns from the dataset
    _scalar_columns: List[str] = [
        'product_description',
        'unspsc',  # column 0
        'root_domain',  # column 1
        'page_url',  # column 2
        'product_title',  # column 3
        'product_summary',  # column 4
        'product_name',  # column 5
        'brand',  # column 7
        'eco_friendly',  # column 10
        'manufacturing_year',  # column 17
        'description',  # column 30
    ]

    # Create a dictionary mapping column names (strings) to their specific aggregation functions
    agg_dict: Dict[str, Callable[[ValueSeries], Optional[ScalarValue]]] = {}
    for col in _scalar_columns:
        if col == 'unspsc':
            agg_dict[col] = merge_unspsc
        elif col == 'root_domain':
            agg_dict[col] = merge_root_domain
        elif col == 'page_url':
            agg_dict[col] = merge_page_url
        elif col in ['product_title', 'product_description', 'brand']:
            agg_dict[col] = merge_text_longest
        elif col in ['eco_friendly']:
            agg_dict[col] = merge_eco_friendly
        elif col == 'manufacturing_year':
            agg_dict[col] = merge_max_year
    return agg_dict

# ========== Array Column Handling Functions ==========

def merge_array_simple(values: ValueSeries) -> List[Any]:
    """
    Merge arrays by concatenating all elements and removing duplicates.

    Returns a list instead of numpy array to avoid PyArrow conversion issues.

    Parameters:
        values: Series containing arrays (numpy arrays or lists)

    Returns:
        List with all unique elements merged
    """
    if values.empty:
        return []

    # Collect all non-empty arrays
    all_elements: List[Any] = []
    for arr in values:
        # Handle numpy arrays - convert to list before extending
        if isinstance(arr, np.ndarray) and len(arr) > 0:
            all_elements.extend(arr.tolist())  # Convert numpy array to list
        # Handle Python lists directly
        elif isinstance(arr, list) and len(arr) > 0:
            all_elements.extend(arr)

    # Remove duplicates by converting to set (if elements are hashable)
    try:
        unique_elements: List[Any] = list(set(all_elements))
    except TypeError:
        # If set conversion fails (elements are unhashable like lists or dicts),
        # fallback to manual deduplication
        unique_elements = []
        for item in all_elements:
            if item not in unique_elements:
                unique_elements.append(item)

    return unique_elements  # Return as list instead of numpy array

def merge_arrays_dictionary(values: ValueSeries) -> List[Any]:
    """
    Merge arrays containing dictionaries by combining all unique dictionaries.

    This function handles numpy arrays or lists containing dictionaries, and
    combines them while removing duplicates based on dictionary content.

    Parameters:
        values: Series containing arrays of dictionaries (numpy arrays or lists)

    Returns:
        List with all unique dictionaries merged
    """
    if values.empty:
        return []

    # Collect all non-empty arrays of dictionaries
    all_dictionaries: List[Dict[str, Any]] = []
    for arr in values:
        if arr is None or (hasattr(arr, '__len__') and len(arr) == 0):
            continue

        if isinstance(arr, np.ndarray):
            all_dictionaries.extend(arr.tolist())
        elif isinstance(arr, list):
            all_dictionaries.extend(arr)

    # To identify unique dictionaries, we'll convert each to a JSON string for comparison
    unique_dicts: List[Dict[str, Any]] = []
    seen_json_strings: Set[str] = set()

    for dictionary in all_dictionaries:
        if not isinstance(dictionary, dict):
            continue

        # Convert dictionary to a standardized JSON string for comparison
        # Sort keys to ensure consistent string representation
        json_str: str = json.dumps(dictionary, sort_keys=True)

        if json_str not in seen_json_strings:
            seen_json_strings.add(json_str)
            unique_dicts.append(dictionary)

    return unique_dicts  # Return as list instead of numpy array

def get_array_aggregation_dict() -> Dict[str, Callable[[ValueSeries], List[Any]]]:
    """
    Create a dictionary mapping array columns to their appropriate aggregation functions.

    Maps each known array column to either simple array merging or
    dictionary array merging based on the column's data structure.

    Returns:
        Dictionary mapping column names to aggregation functions
    """
    # Predefined lists of array columns
    _simple_arrays: List[str] = [
        'product_identifier',          # column 6
        'intended_industries',         # column 8
        'applicability',               # column 9
        'ethical_and_sustainability_practices',  # column 11
        'components',
        'materials',                   # column 14
        'ingredients',                 # column 15
        'manufacturing_countries',     # column 16
        'manufacturing_type',          # column 18
        'customization',               # column 19
        'packaging_type',              # column 20
        'form',                        # column 21
        'quality_standards_and_certifications',  # column 28
        'miscellaneous_features'       # column 29
    ]

    _dictionary_arrays: List[str] = [
        'production_capacity',         # column 12
        'price',                       # column 13
        'size',                        # column 22
        'color',                       # column 23
        'purity',                      # column 24
        'energy_efficiency',           # column 25
        'pressure_rating',             # column 26
        'power_rating',                # column 27
    ]

    agg_dict: Dict[str, Callable[[ValueSeries], List[Any]]] = {}

    for col in _simple_arrays:
        agg_dict[col] = merge_array_simple

    for col in _dictionary_arrays:
        agg_dict[col] = merge_arrays_dictionary

    return agg_dict


def merge_dataframe_rows(df: pd.DataFrame, key_column: str) -> pd.DataFrame:
    """
    Merge rows in a DataFrame that share the same key value.
    Logs any merging errors to a CSV file in the error folder for later analysis.
    Appends to the existing error log if one exists.
    Handles missing columns by skipping them.

    Parameters:
        df: DataFrame to merge
        key_column: Column to use as the grouping key

    Returns:
        DataFrame with merged rows (problematic groups excluded)
    """
    # Check if key_column exists in DataFrame
    if key_column not in df.columns:
        raise ValueError(f"Key column '{key_column}' not found in DataFrame")

    # Handle empty DataFrame
    if df.empty:
        return df.copy()

    # Set up error log path
    error_log_path = DataPaths.error_folder / "merge_errors.csv"

    # Get aggregation dictionaries
    scalar_agg_dict = get_scalar_aggregation_dict()
    array_agg_dict = get_array_aggregation_dict()

    # Combine aggregation dictionaries
    all_agg_dict = {**scalar_agg_dict, **array_agg_dict}

    # Filter aggregation dictionary to only include columns that exist in the dataframe
    agg_dict = {col: func for col, func in all_agg_dict.items() if col in df.columns}

    # Add entry for merged_description if it exists
    if 'merged_description' in df.columns and 'merged_description' not in agg_dict:
        agg_dict['merged_description'] = merge_text_longest

    # Remove the key column from aggregation if needed
    if key_column in agg_dict:
        del agg_dict[key_column]

    # For any columns without an aggregation function, use first() aggregation
    for col in df.columns:
        if col != key_column and col not in agg_dict:
            agg_dict[col] = lambda x: x.iloc[0] if not x.empty else None

    # Process each group individually
    groups = df.groupby(key_column)
    result_rows = []
    error_groups = []

    for key, group in groups:
        row_data = {key_column: key}
        error_found = False
        error_info = None

        for col, agg_func in agg_dict.items():
            try:
                # Apply the aggregation function
                row_data[col] = agg_func(group[col])
            except ValueError as e:
                error_message = str(e)
                if error_message in ['Different brand values', 'Different eco_friendly values']:
                    # Create error metadata
                    error_info = {
                        'error_message': error_message,
                        'error_column': col,
                        'group_size': len(group),
                        'timestamp': pd.Timestamp.now(),
                        'conflicting_values': '|'.join(str(v) for v in group[col].unique() if pd.notna(v))
                    }

                    error_found = True
                    break
                else:
                    # Re-raise unexpected errors
                    raise

        if error_found and error_info is not None:
            # For error groups, save all original rows with additional error info columns
            group_copy = group.copy()

            # Add error metadata columns at the beginning
            for col_name in ['error_message', 'error_column', 'group_size', 'timestamp', 'conflicting_values']:
                group_copy.insert(0, col_name, error_info[col_name])

            error_groups.append(group_copy)
        else:
            result_rows.append(row_data)

    # Save errors to CSV if any were found
    if error_groups:
        # Add a warning message
        print(f"WARNING: Found {len(error_groups)} groups with merge conflicts!")

        # Combine all error groups into one DataFrame
        error_df = pd.concat(error_groups, ignore_index=True)

        # Check if error log already exists
        mode = 'a' if error_log_path.exists() else 'w'
        header = not error_log_path.exists()

        # Append to the error log file if it exists, otherwise create new file
        error_df.to_csv(error_log_path, mode=mode, header=header, index=False)

        print(f"Logged {len(error_df)} rows with merge errors to {error_log_path}")

    # Convert the result rows to a DataFrame
    result_df = pd.DataFrame(result_rows) if result_rows else pd.DataFrame(columns=df.columns)

    # Handle potential None values in array columns
    for col in array_agg_dict:
        if col in result_df.columns:
            result_df[col] = result_df[col].apply(
                lambda x: np.array([]) if x is None else x
            )

    return result_df