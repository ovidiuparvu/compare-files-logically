"""
Compare Files Logically package.

This package provides functionality to compare files logically,
with special handling for CSV and parquet files.
"""

from .compare import are_files_equal

__all__ = ["are_files_equal"]