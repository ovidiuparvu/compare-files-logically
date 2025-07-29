"""
Compare Files Logically package.

This package provides functionality to compare files logically,
with special handling for CSV and parquet files.
"""

from .compare import files_are_logically_equal

__all__ = ["files_are_logically_equal"]