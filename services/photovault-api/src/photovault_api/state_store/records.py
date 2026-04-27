"""Dataclass records and media-type helpers for the photovault-api state store."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

_MEDIA_TYPE_SUFFIXES: dict[str, tuple[str, ...]] = {
    "jpeg": (".jpg", ".jpeg"),
    "png": (".png",),
    "heic": (".heic", ".heif"),
    "raw": (".arw", ".cr2", ".cr3", ".dng", ".nef", ".orf", ".raf", ".rw2"),
    "video": (".mp4", ".mov", ".avi", ".mkv", ".m4v", ".mts"),
}
_PREVIEWABLE_SUFFIXES = frozenset(
    suffix
    for media_type in ("jpeg", "png", "heic", "raw")
    for suffix in _MEDIA_TYPE_SUFFIXES[media_type]
)


def _media_type_for_path(relative_path: str) -> str:
    lowered = relative_path.lower()
    for media_type, suffixes in _MEDIA_TYPE_SUFFIXES.items():
        if lowered.endswith(suffixes):
            return media_type
    return "other"


def _preview_capability_for_path(relative_path: str) -> str:
    lowered = relative_path.lower()
    if lowered.endswith(tuple(_PREVIEWABLE_SUFFIXES)):
        return "previewable"
    return "not_previewable"


@dataclass(frozen=True)
class TempUploadRecord:
    sha256_hex: str
    size_bytes: int
    temp_relative_path: str
    job_name: str
    original_filename: str
    received_at_utc: str


@dataclass(frozen=True)
class StoredFileRecord:
    relative_path: str
    sha256_hex: str
    size_bytes: int
    source_kind: str
    first_seen_at_utc: str
    last_seen_at_utc: str


@dataclass(frozen=True)
class MediaAssetRecord:
    relative_path: str
    sha256_hex: str
    size_bytes: int
    origin_kind: str
    last_observed_origin_kind: str
    provenance_job_name: str | None
    provenance_original_filename: str | None
    first_cataloged_at_utc: str
    last_cataloged_at_utc: str
    extraction_status: str
    extraction_last_attempted_at_utc: str | None
    extraction_last_succeeded_at_utc: str | None
    extraction_last_failed_at_utc: str | None
    extraction_failure_detail: str | None
    preview_status: str
    preview_relative_path: str | None
    preview_last_attempted_at_utc: str | None
    preview_last_succeeded_at_utc: str | None
    preview_last_failed_at_utc: str | None
    preview_failure_detail: str | None
    capture_timestamp_utc: str | None
    camera_make: str | None
    camera_model: str | None
    image_width: int | None
    image_height: int | None
    orientation: int | None
    lens_model: str | None
    exposure_time_s: float | None = None
    f_number: float | None = None
    iso_speed: int | None = None
    focal_length_mm: float | None = None
    focal_length_35mm_mm: int | None = None
    is_favorite: bool = False
    is_archived: bool = False


@dataclass(frozen=True)
class MediaExtractionRecord:
    relative_path: str
    extraction_status: str
    extraction_last_attempted_at_utc: str | None
    extraction_last_succeeded_at_utc: str | None
    extraction_last_failed_at_utc: str | None
    extraction_failure_detail: str | None
    capture_timestamp_utc: str | None
    camera_make: str | None
    camera_model: str | None
    image_width: int | None
    image_height: int | None
    orientation: int | None
    lens_model: str | None
    exposure_time_s: float | None = None
    f_number: float | None = None
    iso_speed: int | None = None
    focal_length_mm: float | None = None
    focal_length_35mm_mm: int | None = None


@dataclass(frozen=True)
class MediaPreviewRecord:
    relative_path: str
    preview_status: str
    preview_relative_path: str | None
    preview_last_attempted_at_utc: str | None
    preview_last_succeeded_at_utc: str | None
    preview_last_failed_at_utc: str | None
    preview_failure_detail: str | None


@dataclass(frozen=True)
class RejectedAssetRecord:
    """Catalog reject-queue row (Phase 3.B).

    The reject queue is a global, single-operator list of assets slated for
    deletion. ``sha256_hex`` is duplicated from ``api_media_assets`` so the
    execute phase still has it after the source asset row is deleted.
    """

    relative_path: str
    sha256_hex: str
    marked_at_utc: str
    marked_reason: str | None


@dataclass(frozen=True)
class TombstoneRecord:
    """Catalog tombstone row (Phase 3.C).

    Records a SHA that has been deleted and moved to trash. Prevents
    re-uploading the same content after deletion. Stays indefinitely unless
    explicitly cleared by an admin.
    """

    relative_path: str
    sha256_hex: str
    trashed_at_utc: str
    marked_reason: str | None
    trash_relative_path: str
    original_size_bytes: int


@dataclass(frozen=True)
class DuplicateShaGroup:
    sha256_hex: str
    file_count: int
    first_seen_at_utc: str
    last_seen_at_utc: str
    relative_paths: tuple[str, ...]


@dataclass(frozen=True)
class PathConflictRecord:
    relative_path: str
    previous_sha256_hex: str
    current_sha256_hex: str
    detected_at_utc: str


@dataclass(frozen=True)
class StorageIndexRunRecord:
    scanned_files: int
    indexed_files: int
    new_sha_entries: int
    existing_sha_matches: int
    path_conflicts: int
    errors: int
    completed_at_utc: str


@dataclass(frozen=True)
class CatalogBackfillRunRecord:
    backfill_kind: str
    requested_statuses: tuple[str, ...]
    selected_count: int
    processed_count: int
    succeeded_count: int
    failed_count: int
    remaining_pending_count: int
    remaining_failed_count: int
    filter_origin_kind: str | None
    filter_media_type: str | None
    filter_preview_capability: str | None
    filter_cataloged_since_utc: str | None
    filter_cataloged_before_utc: str | None
    limit_count: int
    completed_at_utc: str


@dataclass(frozen=True)
class StorageSummary:
    total_known_sha256: int
    total_stored_files: int
    indexed_files: int
    uploaded_files: int
    duplicate_file_paths: int
    recent_indexed_files_24h: int
    recent_uploaded_files_24h: int
    last_indexed_at_utc: str | None
    last_uploaded_at_utc: str | None


@dataclass(frozen=True)
class ClientRecord:
    client_id: str
    display_name: str
    enrollment_status: str
    first_seen_at_utc: str
    last_enrolled_at_utc: str
    approved_at_utc: str | None
    revoked_at_utc: str | None
    auth_token: str | None


@dataclass(frozen=True)
class ClientHeartbeatRecord:
    client_id: str
    last_seen_at_utc: str
    daemon_state: str
    workload_status: str
    active_job_id: int | None
    active_job_label: str | None
    active_job_status: str | None
    active_job_ready_to_upload: int | None
    active_job_uploaded: int | None
    active_job_retrying: int | None
    active_job_total_files: int | None
    active_job_non_terminal_files: int | None
    active_job_error_files: int | None
    active_job_blocking_reason: str | None
    retry_pending_count: int | None
    retry_next_at_utc: str | None
    retry_reason: str | None
    auth_block_reason: str | None
    recent_error_category: str | None
    recent_error_message: str | None
    recent_error_at_utc: str | None
    updated_at_utc: str
