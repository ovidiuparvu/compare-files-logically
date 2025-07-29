"""
Tests for the compare_files_logically package.
"""

import os
from pathlib import Path
from typing import Generator, Tuple

import pytest

from compare_files_logically import files_are_logically_equal

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
    
    assert files_are_logically_equal(file1, file2)


def test_logically_equal_csv_files(data_dir: Path) -> None:
    """Test CSV files with different formatting.
    
    This test verifies that CSV files with the same data but different formatting
    (whitespace, row order) are considered logically equal.
    """
    file1 = data_dir / "file1.csv"
    file3 = data_dir / "file3.csv"
    
    assert files_are_logically_equal(file1, file3)


def test_different_csv_files(data_dir: Path) -> None:
    """Test that different CSV files are not considered equal."""
    file1 = data_dir / "file1.csv"
    file4 = data_dir / "file4.csv"
    
    assert not files_are_logically_equal(file1, file4)


def test_empty_files(empty_files: Tuple[Path, Path]) -> None:
    """Test that empty files are considered equal."""
    empty_file1, empty_file2 = empty_files
    assert files_are_logically_equal(empty_file1, empty_file2)


def test_same_file(data_dir: Path) -> None:
    """Test that a file is equal to itself."""
    file1 = data_dir / "file1.csv"
    
    assert files_are_logically_equal(file1, file1)


def test_nonexistent_file(data_dir: Path) -> None:
    """Test that a FileNotFoundError is raised for nonexistent files."""
    file1 = data_dir / "file1.csv"
    nonexistent_file = data_dir / "nonexistent.csv"
    
    with pytest.raises(FileNotFoundError):
        files_are_logically_equal(file1, nonexistent_file)


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
        
        assert files_are_logically_equal(file1, file2)
    
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
        
        assert files_are_logically_equal(file1, file5)
    
    def test_different_parquet_files(
        self, data_dir: Path, generate_parquet: None
    ) -> None:
        """Test that different parquet files are not considered equal."""
        file1 = data_dir / "file1.parquet"
        file4 = data_dir / "file4.parquet"
        
        if not file1.exists() or not file4.exists():
            pytest.skip("Parquet files not available")
        
        assert not files_are_logically_equal(file1, file4)