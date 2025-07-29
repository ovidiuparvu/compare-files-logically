# Compare Files Logically

A Python library for comparing files logically, with special handling for CSV and parquet files.

## Features

- Compare any two files for logical equality
- Special handling for CSV files:
  - Ignores formatting differences (whitespace, order of rows)
  - Option to ignore headers
- Special handling for parquet files:
  - Compares data content regardless of metadata differences
- Support for comparing files with different extensions (e.g., CSV vs parquet)
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
from compare_files_logically import are_files_equal

# Compare two files
result = are_files_equal("file1.txt", "file2.txt")
print(f"Files are equal: {result}")

# Compare CSV files, ignoring headers
result = are_files_equal("data1.csv", "data2.csv", ignore_headers=True)
print(f"CSV files are equal (ignoring headers): {result}")
```

### Comparing Different File Types

The library can compare files with different extensions if they contain logically equivalent data:

```python
# Compare CSV and parquet files (requires polars)
result = are_files_equal("data.csv", "data.parquet")
print(f"CSV and parquet files are equal: {result}")
```

## How It Works

- For regular files: Performs a binary comparison
- For CSV files: Parses the CSV content and compares the data, ignoring formatting differences
- For parquet files: Uses polars to read the data and compares the content
- For mixed file types: Converts both to a common format (polars DataFrame) for comparison

## Requirements

- Python 3.6+
- Optional: polars (for parquet file support)

## License

MIT