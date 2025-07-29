"""
Module for comparing files logically.
"""

import csv
import filecmp
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Union

# Check if polars is available for parquet support
try:
    import polars as pl
    POLARS_AVAILABLE = True
    if TYPE_CHECKING:
        from polars import DataFrame
except ImportError:
    POLARS_AVAILABLE = False
    if TYPE_CHECKING:
        # Define a stub for type checking when polars is not available
        class DataFrame:  # type: ignore
            """Stub class for polars DataFrame when not available."""
            pass


def are_files_equal(file1: Union[str, Path], file2: Union[str, Path], 
                    ignore_headers: bool = False) -> bool:
    """
    Check if two files are logically equal.
    
    This function compares two files and determines if they are logically equal,
    with special handling for CSV and parquet files.
    
    Args:
        file1: Path to the first file
        file2: Path to the second file
        ignore_headers: For CSV files, whether to ignore headers when comparing
    
    Returns:
        bool: True if files are logically equal, False otherwise
    
    Raises:
        FileNotFoundError: If either file doesn't exist
        ValueError: If parquet comparison is requested but polars is not available
    """
    # Convert to Path objects
    path1 = Path(file1)
    path2 = Path(file2)
    
    # Check if files exist
    if not path1.exists():
        raise FileNotFoundError(f"File not found: {path1}")
    if not path2.exists():
        raise FileNotFoundError(f"File not found: {path2}")
    
    # If they're the same file, they're equal
    if path1.samefile(path2):
        return True
    
    # Check file sizes first (quick check)
    if path1.stat().st_size == 0 and path2.stat().st_size == 0:
        return True  # Both empty files
    
    # Get file extensions
    ext1 = path1.suffix.lower()
    ext2 = path2.suffix.lower()
    
    # If extensions don't match, use different comparison logic
    if ext1 != ext2:
        # For different extensions, we need to check content more carefully
        return _compare_different_extensions(path1, path2, ext1, ext2, ignore_headers)
    
    # Handle specific file types
    if ext1 == '.csv':
        return _compare_csv_files(path1, path2, ignore_headers)
    elif ext1 == '.parquet':
        return _compare_parquet_files(path1, path2)
    else:
        # For other file types, use binary comparison
        return filecmp.cmp(path1, path2, shallow=False)


def _compare_different_extensions(path1: Path, path2: Path, 
                                 ext1: str, ext2: str,
                                 ignore_headers: bool) -> bool:
    """
    Compare files with different extensions.
    
    Args:
        path1: Path to the first file
        path2: Path to the second file
        ext1: Extension of the first file
        ext2: Extension of the second file
        ignore_headers: Whether to ignore headers for CSV files
        
    Returns:
        bool: True if files are logically equal, False otherwise
        
    Raises:
        ValueError: If polars is not available for comparing tabular files
        FileNotFoundError: If either file doesn't exist
        csv.Error: If there's an error reading CSV files
        Exception: If there's an error reading the files
    """
    # Handle CSV and parquet comparison
    csv_exts: List[str] = ['.csv', '.tsv', '.txt']
    
    # If both are tabular data formats
    if ((ext1 in csv_exts or ext1 == '.parquet') and 
        (ext2 in csv_exts or ext2 == '.parquet')):
        
        # Check if polars is available
        if not POLARS_AVAILABLE:
            raise ValueError("Polars is required for comparing CSV and parquet files")
        
        # Read both files into DataFrames
        try:
            df1 = _read_tabular_file(path1)
            df2 = _read_tabular_file(path2)
            
            # Compare DataFrames
            return _compare_dataframes(df1, df2)
        except (ValueError, FileNotFoundError, csv.Error):
            # Re-raise these specific exceptions
            raise
        except Exception as e:
            # Wrap other exceptions
            msg = f"Error comparing files with different extensions: {e}"
            raise Exception(msg) from e
    
    # For other different extensions, files are not considered equal
    return False


def _read_tabular_file(file_path: Path) -> "DataFrame":
    """
    Read a tabular file (CSV or parquet) into a polars DataFrame.
    
    Args:
        file_path: Path to the file
        
    Returns:
        DataFrame: DataFrame containing the file data
        
    Raises:
        ValueError: If polars is not available or file extension is not supported
        csv.Error: If CSV dialect detection fails
    """
    if not POLARS_AVAILABLE:
        raise ValueError("Polars is required for reading tabular files")
    
    ext = file_path.suffix.lower()
    
    if ext == '.parquet':
        return pl.read_parquet(file_path)
    elif ext == '.csv':
        return pl.read_csv(file_path)
    elif ext == '.tsv':
        return pl.read_csv(file_path, separator='\t')
    elif ext == '.txt':
        # Try to detect the delimiter
        try:
            with open(file_path, 'r') as f:
                dialect = csv.Sniffer().sniff(f.read(4096))
                f.seek(0)
                return pl.read_csv(file_path, separator=dialect.delimiter)
        except csv.Error as e:
            raise csv.Error(f"Failed to detect CSV dialect: {e}") from e
    else:
        raise ValueError(f"Unsupported file extension: {ext}")


def _compare_csv_files(file1: Path, file2: Path, ignore_headers: bool = False) -> bool:
    """
    Compare two CSV files for logical equality.
    
    Args:
        file1: Path to the first CSV file
        file2: Path to the second CSV file
        ignore_headers: Whether to ignore headers when comparing
        
    Returns:
        bool: True if CSV files are logically equal, False otherwise
    """
    # Read CSV files
    rows1 = _read_csv(file1)
    rows2 = _read_csv(file2)
    
    # Extract headers if present
    headers1 = rows1[0] if rows1 else []
    headers2 = rows2[0] if rows2 else []
    
    # Skip headers if requested or process them separately
    if rows1 and rows2:
        if ignore_headers:
            data_rows1 = rows1[1:]
            data_rows2 = rows2[1:]
        else:
            # Check if headers match (ignoring whitespace)
            if len(headers1) != len(headers2):
                return False
            
            for h1, h2 in zip(headers1, headers2):
                if _normalize_csv_value(h1) != _normalize_csv_value(h2):
                    return False
            
            data_rows1 = rows1[1:]
            data_rows2 = rows2[1:]
    else:
        data_rows1 = rows1
        data_rows2 = rows2
    
    # Check if number of data rows match
    if len(data_rows1) != len(data_rows2):
        return False
    
    # Normalize all values in the rows
    normalized_rows1 = []
    for row in data_rows1:
        if len(row) != len(headers1) and headers1:
            return False  # Row length doesn't match header length
        normalized_rows1.append([_normalize_csv_value(cell) for cell in row])
    
    normalized_rows2 = []
    for row in data_rows2:
        if len(row) != len(headers2) and headers2:
            return False  # Row length doesn't match header length
        normalized_rows2.append([_normalize_csv_value(cell) for cell in row])
    
    # Sort the normalized rows for comparison (to handle different row orders)
    normalized_rows1.sort()
    normalized_rows2.sort()
    
    # Compare the sorted rows
    return normalized_rows1 == normalized_rows2


def _normalize_csv_value(value: Optional[str]) -> str:
    """
    Normalize CSV values for comparison.
    
    Args:
        value: CSV cell value
        
    Returns:
        str: Normalized value
    """
    if value is None:
        return ""
    return str(value).strip()


def _read_csv(file_path: Path) -> List[List[str]]:
    """
    Read a CSV file into a list of rows.
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        List[List[str]]: List of rows, where each row is a list of cell values
        
    Raises:
        FileNotFoundError: If the file doesn't exist
        csv.Error: If there's an error reading the CSV file
        UnicodeDecodeError: If there's an error decoding the file
    """
    rows: List[List[str]] = []
    try:
        with open(file_path, 'r', newline='') as f:
            # Try to detect the dialect
            try:
                sample = f.read(4096)
                f.seek(0)
                dialect = csv.Sniffer().sniff(sample)
                reader = csv.reader(f, dialect)
            except csv.Error:
                # If detection fails, use default dialect
                f.seek(0)
                reader = csv.reader(f)
            
            for row in reader:
                rows.append(row)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Error reading CSV file {file_path}: {e}") from e
    except UnicodeDecodeError as e:
        # UnicodeDecodeError requires specific arguments
        error_reason = f"Error reading CSV file {file_path}: {e.reason}"
        raise UnicodeDecodeError(
            e.encoding, e.object, e.start, e.end, error_reason
        ) from e
    
    return rows


def _compare_parquet_files(file1: Path, file2: Path) -> bool:
    """
    Compare two parquet files for logical equality.
    
    Args:
        file1: Path to the first parquet file
        file2: Path to the second parquet file
        
    Returns:
        bool: True if parquet files are logically equal, False otherwise
        
    Raises:
        ValueError: If polars is not available
        FileNotFoundError: If either file doesn't exist
        Exception: If there's an error reading the parquet files
    """
    if not POLARS_AVAILABLE:
        raise ValueError("Polars is required for comparing parquet files")
    
    # Read parquet files
    try:
        df1 = pl.read_parquet(file1)
        df2 = pl.read_parquet(file2)
    except Exception as e:
        raise Exception(f"Error reading parquet files: {e}") from e
    
    return _compare_dataframes(df1, df2)


def _compare_dataframes(df1: "DataFrame", df2: "DataFrame") -> bool:
    """
    Compare two polars DataFrames for logical equality.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
        
    Returns:
        bool: True if DataFrames are logically equal, False otherwise
    """
    # Check if shapes match
    if df1.shape != df2.shape:
        return False
    
    # Check if column names match (ignoring order)
    if set(df1.columns) != set(df2.columns):
        return False
    
    # Reorder columns to match
    df2 = df2.select(df1.columns)
    
    # Sort both DataFrames if possible (to handle row order differences)
    try:
        # Use sort method and drop_nulls to ensure consistent ordering
        df1 = df1.sort(df1.columns)
        df2 = df2.sort(df2.columns)
        
        # Note: We don't use reset_index as it's not needed for comparison
        # and mypy doesn't recognize it on the DataFrame type
    except Exception:
        # If sorting fails (e.g., due to non-sortable data), continue with current order
        pass
    
    # Compare data
    # In polars, we need to compare the frames differently than pandas
    # Convert to dict of lists for comparison
    return df1.to_dict(as_series=False) == df2.to_dict(as_series=False)