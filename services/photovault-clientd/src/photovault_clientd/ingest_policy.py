"""Mounted-media ingest policy helpers for v1."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

EXCLUDED_DIRECTORY_NAMES = {
    ".spotlight-v100",
    ".fseventsd",
    ".trashes",
    "__macosx",
}

EXCLUDED_FILE_NAMES = {
    ".ds_store",
    "thumbs.db",
    "desktop.ini",
}

ALLOWED_MEDIA_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".tif",
    ".tiff",
    ".bmp",
    ".gif",
    ".webp",
    ".heic",
    ".heif",
    ".dng",
    ".cr2",
    ".cr3",
    ".nef",
    ".nrw",
    ".arw",
    ".srf",
    ".sr2",
    ".raf",
    ".orf",
    ".rw2",
    ".pef",
    ".iiq",
    ".3fr",
    ".fff",
    ".rwl",
    ".mef",
    ".mos",
    ".mp4",
    ".mov",
    ".m4v",
    ".avi",
    ".mkv",
    ".mts",
    ".m2ts",
    ".mpg",
    ".mpeg",
    ".wmv",
}

MAX_FILTERED_EXAMPLES = 10


@dataclass(frozen=True)
class FilteredSource:
    source_path: str
    reason: str


@dataclass(frozen=True)
class DirectoryDiscoveryResult:
    discovered_files: list[str]
    filtered_files: list[FilteredSource]

    @property
    def filtered_count(self) -> int:
        return len(self.filtered_files)

    def to_examples(self, limit: int = MAX_FILTERED_EXAMPLES) -> list[dict[str, str]]:
        return [
            {"source_path": item.source_path, "reason": item.reason}
            for item in self.filtered_files[:limit]
        ]


def _normalized_name(path: Path) -> str:
    return path.name.strip().lower()


def is_path_excluded(path: Path) -> bool:
    name = _normalized_name(path)
    if path.is_dir():
        return name in EXCLUDED_DIRECTORY_NAMES
    return name in EXCLUDED_FILE_NAMES


def is_allowed_media_file(path: Path) -> bool:
    return path.suffix.strip().lower() in ALLOWED_MEDIA_EXTENSIONS


def enumerate_directory_media_files(path: Path) -> DirectoryDiscoveryResult:
    discovered: list[str] = []
    filtered: list[FilteredSource] = []
    walk_errors: list[OSError] = []

    def _on_walk_error(exc: OSError) -> None:
        walk_errors.append(exc)

    for root, dirnames, filenames in os.walk(path, onerror=_on_walk_error):
        if walk_errors:
            break

        dirnames[:] = sorted(
            dirname
            for dirname in dirnames
            if _normalized_name(Path(dirname)) not in EXCLUDED_DIRECTORY_NAMES
        )
        filenames.sort()

        for filename in filenames:
            file_path = Path(root) / filename
            normalized_name = _normalized_name(file_path)
            if normalized_name in EXCLUDED_FILE_NAMES:
                filtered.append(
                    FilteredSource(
                        source_path=str(file_path),
                        reason=f"Excluded by ingest policy: file name {file_path.name}",
                    )
                )
                continue

            try:
                if not file_path.is_file():
                    continue
            except OSError as exc:
                raise OSError(
                    f"failed to stat {file_path}: {exc.strerror or exc.__class__.__name__}"
                ) from exc

            if not is_allowed_media_file(file_path):
                filtered.append(
                    FilteredSource(
                        source_path=str(file_path),
                        reason=(
                            "Skipped by ingest policy: unsupported file extension "
                            f"{file_path.suffix or '(none)'}"
                        ),
                    )
                )
                continue

            discovered.append(str(file_path))

    if walk_errors:
        first_error = walk_errors[0]
        raise OSError(
            f"failed to read directory {path}: {first_error.strerror or first_error}"
        ) from first_error

    return DirectoryDiscoveryResult(discovered_files=discovered, filtered_files=filtered)


def build_disallowed_file_reason(path: Path) -> str:
    return (
        "File is not allowed by the v1 ingest policy. "
        f"Supported extensions include common photo, RAW, and video formats; got {path.suffix or '(none)'}."
    )
