import filecmp
from pathlib import Path
from typing import TypeAlias

import polars as pl

FilePath: TypeAlias = str | Path


def ensure_existing_file_path(file_path: FilePath) -> Path:
    path = Path(file_path) if isinstance(file_path, str) else file_path
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"{path} should point to an existing file and it does not.")
    return path


def files_are_logically_equal(file1: FilePath, file2: FilePath) -> bool:
    path1 = ensure_existing_file_path(file1)
    path2 = ensure_existing_file_path(file2)

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
            return pl.scan_csv(path1).collect(engine="streaming").equals(pl.scan_csv(path2).collect(engine="streaming"))
        case '.parquet':
            return pl.scan_parquet(path1).collect(engine="streaming").equals(pl.scan_parquet(path2).collect(engine="streaming"))
        case _:
            return filecmp.cmp(path1, path2, shallow=False)
