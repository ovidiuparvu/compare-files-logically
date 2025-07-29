import tempfile
from pathlib import Path
from typing import Generator, Tuple, Dict

import pytest
import polars as pl

from compare_files_logically import files_are_logically_equal


@pytest.fixture
def data_dir() -> Generator[Path, None, None]:
    """Fixture providing a temporary data directory path."""
    with tempfile.TemporaryDirectory() as temp_dir:
        data_dir_path = Path(temp_dir)
        yield data_dir_path


@pytest.fixture
def empty_files(data_dir: Path) -> Generator[Tuple[Path, Path], None, None]:
    """Fixture creating empty files for testing."""
    empty_file1 = data_dir / "empty1.txt"
    empty_file2 = data_dir / "empty2.txt"
    
    # Create empty files
    open(empty_file1, "w").close()
    open(empty_file2, "w").close()
    
    yield empty_file1, empty_file2


@pytest.fixture
def parquet_files(data_dir: Path) -> Dict[str, Path]:
    # Create base DataFrame
    data = {
        "id": [1, 2, 3],
        "name": ["apple", "banana", "cherry"],
        "value": [10.5, 5.2, 8.7]
    }
    df1 = pl.DataFrame(data)
    
    # File 1 - Base file
    file1_path = data_dir / "file1.parquet"
    df1.write_parquet(file1_path)
    
    # File 2 - Identical to file1
    file2_path = data_dir / "file2.parquet"
    df1.write_parquet(file2_path)
    
    # File 3 - Same data as file1 but with rows in different order
    df3 = pl.DataFrame({
        "id": [3, 1, 2],
        "name": ["cherry", "apple", "banana"],
        "value": [8.7, 10.5, 5.2]
    })
    file3_path = data_dir / "file3.parquet"
    df3.write_parquet(file3_path)
    
    # File 4 - Different data (additional row)
    df4 = pl.DataFrame({
        "id": [1, 2, 3, 4],
        "name": ["apple", "banana", "cherry", "date"],
        "value": [10.5, 5.2, 8.7, 12.3]
    })
    file4_path = data_dir / "file4.parquet"
    df4.write_parquet(file4_path)
    
    # File 5 - Same data as file1 but with different metadata
    # Add a temporary column and then remove it to change metadata
    df5 = df1.with_columns(pl.lit(0).alias("temp"))
    df5 = df5.drop("temp")
    file5_path = data_dir / "file5.parquet"
    df5.write_parquet(file5_path)
    
    return {
        "file1": file1_path,
        "file2": file2_path,
        "file3": file3_path,
        "file4": file4_path,
        "file5": file5_path
    }


def test_identical_files(parquet_files: Dict[str, Path]) -> None:
    """Test that identical files are considered equal."""
    file1 = parquet_files["file1"]
    file2 = parquet_files["file2"]
    
    assert files_are_logically_equal(file1, file2)




def test_different_files(parquet_files: Dict[str, Path]) -> None:
    """Test that different files are not considered equal."""
    file1 = parquet_files["file1"]
    file4 = parquet_files["file4"]
    
    assert not files_are_logically_equal(file1, file4)


def test_empty_files(empty_files: Tuple[Path, Path]) -> None:
    """Test that empty files are considered equal."""
    empty_file1, empty_file2 = empty_files
    assert files_are_logically_equal(empty_file1, empty_file2)


def test_same_file(parquet_files: Dict[str, Path]) -> None:
    """Test that a file is equal to itself."""
    file1 = parquet_files["file1"]
    
    assert files_are_logically_equal(file1, file1)


def test_nonexistent_file(parquet_files: Dict[str, Path], data_dir: Path) -> None:
    """Test that a FileNotFoundError is raised for nonexistent files."""
    file1 = parquet_files["file1"]
    nonexistent_file = data_dir / "nonexistent.parquet"
    
    with pytest.raises(FileNotFoundError):
        files_are_logically_equal(file1, nonexistent_file)


def test_logically_equal_files_with_different_metadata(parquet_files: Dict[str, Path]) -> None:
    """Test parquet files with different metadata but same data.
    
    This test verifies that parquet files with different metadata
    but the same data are considered logically equal.
    """
    file1 = parquet_files["file1"]
    file5 = parquet_files["file5"]
    
    assert files_are_logically_equal(file1, file5)