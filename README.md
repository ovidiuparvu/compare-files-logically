# Compare Files Logically

A Python library for comparing files logically, with special handling for CSV and parquet files.

## Features

- Compare any two files for logical equality
- Special handling for CSV files:
  - Ignores formatting differences (whitespace, order of rows)
- Special handling for parquet files:
  - Compares data content regardless of metadata differences
- Automatic handling of special cases:
  - Empty files are considered equal
  - A file is equal to itself
  - Files with different extensions are considered not equal
- Raises FileNotFoundError for nonexistent files

## Installation

```bash
pip install compare-files-logically
```

Since polars is required for both CSV and parquet file support, it's recommended to install with:

```bash
pip install compare-files-logically[parquet]
```

## Usage

### Basic Usage

```python
from compare_files_logically import files_are_logically_equal

# Compare two files
result = files_are_logically_equal("file1.txt", "file2.txt")
print(f"Files are equal: {result}")

# Compare CSV files
result = files_are_logically_equal("data1.csv", "data2.csv")
print(f"CSV files are equal: {result}")

# Compare parquet files
result = files_are_logically_equal("data1.parquet", "data2.parquet")
print(f"Parquet files are equal: {result}")
```

## How It Works

- For regular files: Performs a binary comparison
- For CSV files: Uses polars to read and compare the data, ignoring formatting differences and row order
- For parquet files: Uses polars to read and compare the data, ignoring metadata differences
- Special cases:
  - Empty files are always considered equal
  - A file is always equal to itself
  - Files with different extensions are always considered not equal
  - Nonexistent files will raise FileNotFoundError

## Requirements

- Python 3.12+
- Polars (>=0.20.0) for CSV and parquet file support

## License

MIT