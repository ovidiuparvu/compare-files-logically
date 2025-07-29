# Compare Files Logically

A Python library for comparing files logically, with special handling for CSV and parquet files.

## Features

- Compare any two files for logical equality
- Special handling for CSV files:
  - Ignores formatting differences (whitespace, order of rows)
- Special handling for parquet files:
  - Compares data content regardless of metadata differences
- Graceful handling when optional dependencies are not available

## Installation

```bash
pip install compare-files-logically
```

For parquet file support, install with polars:

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
```


## How It Works

- For regular files: Performs a binary comparison
- For CSV files: Parses the CSV content and compares the data, ignoring formatting differences
- For parquet files: Uses polars to read the data and compares the content

## Requirements

- Python 3.12+
- Optional: polars (for parquet file support)

## License

MIT