"""
DataFrame Export Utility
-----------------------
Provides functionality to export pandas DataFrames to CSV or Parquet format
with consistent naming and compression settings.

This module handles directory creation, appropriate file extensions,
and standardizes the export process across the project.

Usage:
   from tools.save_data import export_dataframe

   * Export as CSV (default)
   csv_path = export_dataframe(df, output_dir, "my_dataset")

   * Export as Snappy-compressed Parquet
   parquet_path = export_dataframe(df, output_dir, "my_dataset", file_format="parquet")
"""

import pandas as pd
from pathlib import Path


def export_dataframe(
        df: pd.DataFrame,
        output_dir: Path,
        filename: str,
        file_format: str = 'csv'
) -> Path:
    """
    Export a DataFrame to CSV or Parquet (Snappy) format

    Args:
        df: DataFrame to export
        output_dir: Path object pointing to the output directory
        filename: Name for the output file (without extension)
        file_format: 'csv' or 'parquet' (default: 'csv')

    Returns:
        Path to the saved file
    """

    if not isinstance(output_dir, Path):
        output_dir = Path(output_dir)

    if file_format not in ['csv', 'parquet']:
        raise ValueError("file_format must be either 'csv' or 'parquet'")

    output_dir.mkdir(exist_ok=True, parents=True)

    if file_format == 'csv':
        output_path = output_dir / f"{filename}.csv"
        df.to_csv(output_path, index=False)
    else:
        output_path = output_dir / f"{filename}.snappy.parquet"
        df.to_parquet(output_path, compression='snappy')

    print(f"Exported data to: {output_path}")
    return output_path