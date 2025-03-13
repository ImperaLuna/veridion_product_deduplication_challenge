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

    # Validate inputs
    if not isinstance(output_dir, Path):
        output_dir = Path(output_dir)

    if file_format not in ['csv', 'parquet']:
        raise ValueError("file_format must be either 'csv' or 'parquet'")

    # Create output directory if it doesn't exist
    output_dir.mkdir(exist_ok=True, parents=True)

    # Create output path
    if file_format == 'csv':
        output_path = output_dir / f"{filename}.csv"
        df.to_csv(output_path, index=False)
    else:  # parquet
        output_path = output_dir / f"{filename}.snappy.parquet"
        df.to_parquet(output_path, compression='snappy')

    print(f"Exported data to: {output_path}")
    return output_path