"""Hashing primitives for staged files."""

import hashlib
from pathlib import Path


def compute_sha256(path: Path, chunk_size: int = 1024 * 1024) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0

    with path.open("rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)

    return digest.hexdigest(), size
