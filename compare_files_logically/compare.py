import filecmp
from pathlib import Path
from typing import TypeAlias

import polars as pl

FilePath: TypeAlias = str | Path


def files_are_logically_equal(file1: FilePath, file2: FilePath) -> bool:
    path1 = Path(file1) if isinstance(file1, str) else file1
    path2 = Path(file2) if isinstance(file2, str) else file2
    
    if not path1.exists():
        raise FileNotFoundError(f"File not found: {path1}")
    if not path2.exists():
        raise FileNotFoundError(f"File not found: {path2}")
    
    if path1.samefile(path2):
        return True
    
    if path1.stat().st_size == 0 and path2.stat().st_size == 0:
        return True
    
    ext1 = path1.suffix.lower()
    ext2 = path2.suffix.lower()
    
    if ext1 != ext2:
        return False
    
    match ext1:
        case '.csv':
            return pl.scan_csv(path1).collect(new_streaming=True).equals(pl.scan_csv(path2).collect(new_streaming=True))
        case '.parquet':
            return pl.scan_parquet(path1).collect(new_streaming=True).equals(pl.scan_parquet(path2).collect(new_streaming=True))
        case _:
            return filecmp.cmp(path1, path2, shallow=False)
