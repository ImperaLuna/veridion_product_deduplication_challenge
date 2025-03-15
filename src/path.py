from pathlib import Path
import types

class DataPaths(types.SimpleNamespace):
    SCRIPT_PATH = Path(__file__)
    PROJECT_ROOT = SCRIPT_PATH.parent.parent

    # Define data directories
    data_dir = PROJECT_ROOT / 'data'

    # Original raw data (snappy parquet)
    parquet_dir = data_dir / 'parquet'
    parquet_raw_dir = parquet_dir / 'raw'
    parquet_final_dir = parquet_dir / 'final'

    parquet_processed_dir = parquet_dir / 'processed'
    parquet_clean_data_dir = parquet_processed_dir / '1_clean'
    parquet_merge_url_title_dir = parquet_processed_dir / '2_merge_url_title'
    parquet_merge_title_domain_dir = parquet_processed_dir / '3_merge_title_domain'


    # Visualization data (CSV)
    visualization_dir = data_dir / 'visualization'
    visualization_raw_dir = visualization_dir / 'raw'
    visualization_final_dir = visualization_dir / 'final'

    visualization_processed_dir = visualization_dir / 'processed'
    visualization_clean_data_dir = visualization_processed_dir / '1_clean'
    visualization_merge_url_title_dir = visualization_processed_dir / '2_merge_url_title'
    visualization_merge_title_domain_dir = visualization_processed_dir / '3_merge_title_domain'

    # Test folder
    test_folder = data_dir / 'test'
    test_parquet = test_folder / 'test_merged_columns.snappy.parquet'

    # Error Folder
    error_folder = data_dir / 'error'

    # File paths
    file_parquet_original = parquet_raw_dir / 'veridion_product_deduplication_challenge.snappy.parquet'
    file_parquet_clean = parquet_clean_data_dir / 'clean_data.snappy.parquet'
    file_parquet_final = parquet_final_dir / 'final_data.snappy.parquet'



    @classmethod
    def ensure_dirs_exist(cls):
        """Ensure directories exist by creating them if necessary."""
        all_dirs = [
            value for key, value in vars(cls).items()
            if isinstance(value, Path) and not value.suffix  # Only directories (no file extensions)
        ]
        for dir_path in all_dirs:
            dir_path.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    DataPaths.ensure_dirs_exist()

