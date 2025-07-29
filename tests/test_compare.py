"""
Tests for the compare_files_logically package.
"""

import os
from pathlib import Path
from typing import Generator, Tuple

import pytest

from compare_files_logically import are_files_equal

# Check if polars is available
try:
    import polars  # noqa: F401
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False


@pytest.fixture
def data_dir() -> Path:
    """Fixture providing the data directory path."""
    return Path(__file__).parent / "data"


@pytest.fixture
def empty_files(data_dir: Path) -> Generator[Tuple[Path, Path], None, None]:
    """Fixture creating empty files for testing."""
    empty_file1 = data_dir / "empty1.txt"
    empty_file2 = data_dir / "empty2.txt"
    
    # Create empty files
    open(empty_file1, "w").close()
    open(empty_file2, "w").close()
    
    yield empty_file1, empty_file2
    
    # Cleanup
    if empty_file1.exists():
        os.remove(empty_file1)
    
    if empty_file2.exists():
        os.remove(empty_file2)


@pytest.fixture
def temp_csv_file(data_dir: Path) -> Generator[Path, None, None]:
    """Fixture creating a temporary CSV file with different headers but same data."""
    temp_file = data_dir / "temp.csv"
    
    with open(data_dir / "file1.csv", "r") as f:
        lines = f.readlines()
    
    with open(temp_file, "w") as f:
        f.write("ID,FRUIT,PRICE\n")  # Different header
        f.writelines(lines[1:])  # Same data
    
    yield temp_file
    
    # Cleanup
    if temp_file.exists():
        os.remove(temp_file)


@pytest.fixture
def generate_parquet() -> None:
    """Fixture to generate parquet files for testing."""
    try:
        from tests.generate_test_data import generate_parquet_files
        generate_parquet_files()
    except Exception as e:
        pytest.skip(f"Failed to generate parquet files: {e}")


def test_identical_files(data_dir: Path) -> None:
    """Test that identical files are considered equal."""
    file1 = data_dir / "file1.csv"
    file2 = data_dir / "file2.csv"
    
    assert are_files_equal(file1, file2)


def test_logically_equal_csv_files(data_dir: Path) -> None:
    """Test that logically equal CSV files with different formatting are considered equal.
    
    This test verifies that CSV files with the same data but different formatting
    (whitespace, row order) are considered logically equal.
    """
    file1 = data_dir / "file1.csv"
    file3 = data_dir / "file3.csv"
    
    assert are_files_equal(file1, file3)


def test_different_csv_files(data_dir: Path) -> None:
    """Test that different CSV files are not considered equal."""
    file1 = data_dir / "file1.csv"
    file4 = data_dir / "file4.csv"
    
    assert not are_files_equal(file1, file4)


def test_empty_files(empty_files: Tuple[Path, Path]) -> None:
    """Test that empty files are considered equal."""
    empty_file1, empty_file2 = empty_files
    assert are_files_equal(empty_file1, empty_file2)


def test_same_file(data_dir: Path) -> None:
    """Test that a file is equal to itself."""
    file1 = data_dir / "file1.csv"
    
    assert are_files_equal(file1, file1)


def test_nonexistent_file(data_dir: Path) -> None:
    """Test that a FileNotFoundError is raised for nonexistent files."""
    file1 = data_dir / "file1.csv"
    nonexistent_file = data_dir / "nonexistent.csv"
    
    with pytest.raises(FileNotFoundError):
        are_files_equal(file1, nonexistent_file)


def test_ignore_headers(data_dir: Path, temp_csv_file: Path) -> None:
    """Test CSV files with different headers but same data.
    
    This test verifies that CSV files with different headers but the same data
    are considered equal when the ignore_headers option is set to True.
    """
    # Should be different with headers
    assert not are_files_equal(data_dir / "file1.csv", temp_csv_file)
    
    # Should be equal when ignoring headers
    assert are_files_equal(data_dir / "file1.csv", temp_csv_file, ignore_headers=True)


@pytest.mark.skipif(not POLARS_AVAILABLE, reason="Polars not available")
class TestParquetCompare:
    """Test cases for parquet file comparison (requires polars)."""
    
    def test_identical_parquet_files(
        self, data_dir: Path, generate_parquet: None
    ) -> None:
        """Test that identical parquet files are considered equal."""
        file1 = data_dir / "file1.parquet"
        file2 = data_dir / "file2.parquet"
        
        if not file1.exists() or not file2.exists():
            pytest.skip("Parquet files not available")
        
        assert are_files_equal(file1, file2)
    
    def test_logically_equal_parquet_files(
        self, data_dir: Path, generate_parquet: None
    ) -> None:
        """Test parquet files with different metadata but same data.
        
        This test verifies that parquet files with different metadata
        but the same data are considered logically equal.
        """
        file1 = data_dir / "file1.parquet"
        file5 = data_dir / "file5.parquet"
        
        if not file1.exists() or not file5.exists():
            pytest.skip("Parquet files not available")
        
        assert are_files_equal(file1, file5)
    
    def test_different_parquet_files(
        self, data_dir: Path, generate_parquet: None
    ) -> None:
        """Test that different parquet files are not considered equal."""
        file1 = data_dir / "file1.parquet"
        file4 = data_dir / "file4.parquet"
        
        if not file1.exists() or not file4.exists():
            pytest.skip("Parquet files not available")
        
        assert not are_files_equal(file1, file4)
    
    def test_csv_vs_parquet(
        self, data_dir: Path, generate_parquet: None
    ) -> None:
        """Test comparison between CSV and parquet files."""
        csv_file = data_dir / "file1.csv"
        parquet_file = data_dir / "file1.parquet"
        
        if not parquet_file.exists():
            pytest.skip("Parquet file not available")
        
        assert are_files_equal(csv_file, parquet_file)