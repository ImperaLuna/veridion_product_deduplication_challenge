from pathlib import Path

# Define project root
SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = SCRIPT_PATH.parent.parent

# Define data directories
data_dir = PROJECT_ROOT / 'data'

# Original raw data (snappy parquet)
parquet_dir = data_dir / 'parquet'
parquet_raw_dir = parquet_dir / 'raw'
parquet_final_dir = parquet_dir / 'final'

parquet_processed_dir = parquet_dir / 'processed'
parquet_clean_data_dir = parquet_processed_dir / '1_clean'



# Visualization data (CSV)
visualization_dir = data_dir / 'visualization'
visualization_raw_dir = visualization_dir / 'raw'
visualization_final_dir = visualization_dir / 'final'

visualization_processed_dir = visualization_dir / 'processed'
visualization_clean_data_dir = visualization_processed_dir / '1_clean'


# Ensure directories exist
for dir_path in [
    parquet_dir, parquet_raw_dir, parquet_processed_dir, parquet_final_dir,parquet_clean_data_dir,
    visualization_dir, visualization_raw_dir, visualization_processed_dir, visualization_final_dir, visualization_clean_data_dir
]:
    dir_path.mkdir(exist_ok=True, parents=True)

# File paths
parquet_original = parquet_raw_dir / 'veridion_product_deduplication_challenge.snappy.parquet'

# Output paths for CSV files
csv_original_path = visualization_raw_dir / 'original_data.csv'