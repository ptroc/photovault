"""Upload/storage helpers for InMemoryUploadStateStore."""

from __future__ import annotations

from .records import StoredFileRecord, TempUploadRecord


def has_sha(self, sha256_hex: str) -> bool:
    with self._lock:
        return sha256_hex in self.known_sha256

def has_shas(self, sha256_hex_values: list[str]) -> set[str]:
    with self._lock:
        return {sha256_hex for sha256_hex in sha256_hex_values if sha256_hex in self.known_sha256}

def upsert_temp_upload(
    self,
    *,
    sha256_hex: str,
    size_bytes: int,
    temp_relative_path: str,
    job_name: str,
    original_filename: str,
    received_at_utc: str,
) -> None:
    with self._lock:
        self.upload_temp[sha256_hex] = TempUploadRecord(
            sha256_hex=sha256_hex,
            size_bytes=size_bytes,
            temp_relative_path=temp_relative_path,
            job_name=job_name,
            original_filename=original_filename,
            received_at_utc=received_at_utc,
        )

def get_temp_upload(self, sha256_hex: str) -> TempUploadRecord | None:
    with self._lock:
        return self.upload_temp.get(sha256_hex)

def mark_sha_verified(self, sha256_hex: str) -> bool:
    with self._lock:
        is_new = sha256_hex not in self.known_sha256
        self.known_sha256.add(sha256_hex)
        return is_new

def upsert_stored_file(
    self,
    *,
    relative_path: str,
    sha256_hex: str,
    size_bytes: int,
    source_kind: str,
    seen_at_utc: str,
) -> None:
    with self._lock:
        existing = self.stored_files.get(relative_path)
        first_seen = existing.first_seen_at_utc if existing is not None else seen_at_utc
        self.stored_files[relative_path] = StoredFileRecord(
            relative_path=relative_path,
            sha256_hex=sha256_hex,
            size_bytes=size_bytes,
            source_kind=source_kind,
            first_seen_at_utc=first_seen,
            last_seen_at_utc=seen_at_utc,
        )

def get_stored_file_by_path(self, relative_path: str) -> StoredFileRecord | None:
    with self._lock:
        return self.stored_files.get(relative_path)

def list_stored_files(self, *, limit: int, offset: int) -> tuple[int, list[StoredFileRecord]]:
    with self._lock:
        ordered = sorted(self.stored_files.values(), key=lambda item: item.relative_path)
        ordered = sorted(ordered, key=lambda item: item.last_seen_at_utc, reverse=True)
        total = len(ordered)
        return total, ordered[offset : offset + limit]

def delete_stored_file(self, relative_path: str) -> bool:
    """Remove a stored-file row and mirror FK cascade cleanup."""

    with self._lock:
        if relative_path not in self.stored_files:
            return False
        del self.stored_files[relative_path]
        self.media_assets.pop(relative_path, None)
        self.media_asset_extractions.pop(relative_path, None)
        self.media_asset_previews.pop(relative_path, None)
        self.media_asset_rejects.pop(relative_path, None)
        return True

def remove_temp_upload(self, sha256_hex: str) -> None:
    with self._lock:
        self.upload_temp.pop(sha256_hex, None)
