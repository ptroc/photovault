"""Local filesystem operations for staging copy."""

import os
import shutil
from pathlib import Path


def build_staged_path(staging_root: Path, job_id: int, file_id: int, source_path: str) -> Path:
    source_name = Path(source_path).name
    return staging_root / f"job-{job_id}" / f"{file_id}-{source_name}"


def copy_with_fsync(source_path: str, staged_path: Path) -> int:
    staged_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, staged_path)

    file_size = staged_path.stat().st_size
    with staged_path.open("rb") as fh:
        os.fsync(fh.fileno())

    dir_fd = os.open(str(staged_path.parent), os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)

    return file_size
