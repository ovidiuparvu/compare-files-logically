"""
Script to generate test data files for testing the compare_files_logically package.

This script generates parquet files from existing CSV files for testing.
"""

import os
from pathlib import Path

import polars as pl
from polars import DataFrame


def generate_parquet_files() -> None:
    """
    Generate parquet files from CSV files for testing.
    
    This function scans the data directory for CSV files, converts them to parquet format,
    and creates an additional parquet file with different metadata but the same data.
    """
    # Get the directory of this script
    script_dir: Path = Path(__file__).parent
    data_dir: Path = script_dir / "data"
    
    # Create data directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)
    
    # Generate parquet files from CSV files
    csv_files: list[Path] = list(data_dir.glob("*.csv"))
    for csv_file in csv_files:
        output_filename: str = f"{csv_file.stem}.parquet"
        parquet_file: Path = data_dir / output_filename
        
        # Read CSV and write to parquet
        csv_data: DataFrame = pl.read_csv(csv_file)
        csv_data.write_parquet(parquet_file)
        print(f"Generated {parquet_file}")
    
    # Create a parquet file with different metadata but same data as file1.parquet
    reference_file: Path = data_dir / "file1.parquet"
    if reference_file.exists():
        # Read the reference file
        reference_data: DataFrame = pl.read_parquet(reference_file)
        
        # Add a new column with default values, then remove it
        # This changes the metadata but keeps the data the same
        modified_data: DataFrame = reference_data.with_columns(pl.lit(0).alias("temp"))
        modified_data = modified_data.drop("temp")
        
        # Write the modified data to a new file
        output_file: Path = data_dir / "file5.parquet"
        modified_data.write_parquet(output_file)
        print(f"Generated {output_file}")

if __name__ == "__main__":
    generate_parquet_files()
    print("Test data generation complete.")