"""
Script to generate test data files for testing the compare_files_logically package.

This script generates parquet files from existing CSV files for testing.
"""

import os
from pathlib import Path
from typing import List

import polars as pl
from polars import DataFrame


def generate_parquet_files() -> None:
    """Generate parquet files from CSV files for testing."""
    # Get the directory of this script
    script_dir: Path = Path(__file__).parent
    data_dir: Path = script_dir / "data"
    
    # Create data directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)
    
    # Generate parquet files from CSV files
    csv_files: List[Path] = [f for f in data_dir.glob("*.csv")]
    for csv_file in csv_files:
        parquet_file: Path = data_dir / f"{csv_file.stem}.parquet"
        df: DataFrame = pl.read_csv(csv_file)
        df.write_parquet(parquet_file)
        print(f"Generated {parquet_file}")
    
    # Create a parquet file with different metadata but same data as file1.parquet
    file1_parquet: Path = data_dir / "file1.parquet"
    if file1_parquet.exists():
        df = pl.read_parquet(file1_parquet)
        # Add a new column with default values, then remove it
        # This changes the metadata but keeps the data the same
        df = df.with_columns(pl.lit(0).alias("temp"))
        df = df.drop("temp")
        df.write_parquet(data_dir / "file5.parquet")
        print(f"Generated {data_dir / 'file5.parquet'}")

if __name__ == "__main__":
    generate_parquet_files()
    print("Test data generation complete.")