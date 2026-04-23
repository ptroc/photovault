"""Persistence backends for upload dedup and upload file metadata."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from threading import Lock
from typing import Protocol

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


class UploadStateStore(Protocol):
    def initialize(self) -> None: ...

    def has_sha(self, sha256_hex: str) -> bool: ...

    def has_shas(self, sha256_hex_values: list[str]) -> set[str]: ...

    def upsert_temp_upload(
        self,
        *,
        sha256_hex: str,
        size_bytes: int,
        temp_relative_path: str,
        job_name: str,
        original_filename: str,
        received_at_utc: str,
    ) -> None: ...

    def get_temp_upload(self, sha256_hex: str) -> TempUploadRecord | None: ...

    def mark_sha_verified(self, sha256_hex: str) -> bool: ...

    def upsert_stored_file(
        self,
        *,
        relative_path: str,
        sha256_hex: str,
        size_bytes: int,
        source_kind: str,
        seen_at_utc: str,
    ) -> None: ...

    def get_stored_file_by_path(self, relative_path: str) -> StoredFileRecord | None: ...

    def list_stored_files(self, *, limit: int, offset: int) -> tuple[int, list[StoredFileRecord]]: ...

    def upsert_media_asset(
        self,
        *,
        relative_path: str,
        sha256_hex: str,
        size_bytes: int,
        origin_kind: str,
        observed_at_utc: str,
        provenance_job_name: str | None = None,
        provenance_original_filename: str | None = None,
    ) -> None: ...

    def list_media_assets(
        self,
        *,
        limit: int,
        offset: int,
        extraction_status: str | None = None,
        preview_status: str | None = None,
        origin_kind: str | None = None,
        media_type: str | None = None,
        preview_capability: str | None = None,
        is_favorite: bool | None = None,
        is_archived: bool | None = None,
        cataloged_since_utc: str | None = None,
        cataloged_before_utc: str | None = None,
        relative_path_prefix: str | None = None,
    ) -> tuple[int, list[MediaAssetRecord]]: ...

    def list_media_asset_folders(self) -> list[tuple[str, int, int, int]]: ...

    def get_media_asset_by_path(self, relative_path: str) -> MediaAssetRecord | None: ...

    def set_media_asset_favorite(
        self, *, relative_path: str, is_favorite: bool, updated_at_utc: str
    ) -> MediaAssetRecord | None: ...

    def set_media_asset_archived(
        self, *, relative_path: str, is_archived: bool, updated_at_utc: str
    ) -> MediaAssetRecord | None: ...

    def list_media_assets_for_extraction(
        self,
        *,
        extraction_statuses: list[str],
        limit: int,
        origin_kind: str | None = None,
        media_type: str | None = None,
        preview_capability: str | None = None,
        cataloged_since_utc: str | None = None,
        cataloged_before_utc: str | None = None,
    ) -> list[MediaAssetRecord]: ...

    def list_media_assets_for_preview(
        self,
        *,
        preview_statuses: list[str],
        limit: int,
        origin_kind: str | None = None,
        media_type: str | None = None,
        preview_capability: str | None = None,
        cataloged_since_utc: str | None = None,
        cataloged_before_utc: str | None = None,
    ) -> list[MediaAssetRecord]: ...

    def ensure_media_asset_extraction_row(self, *, relative_path: str, recorded_at_utc: str) -> None: ...

    def upsert_media_asset_extraction(
        self,
        *,
        relative_path: str,
        extraction_status: str,
        attempted_at_utc: str | None,
        succeeded_at_utc: str | None,
        failed_at_utc: str | None,
        failure_detail: str | None,
        capture_timestamp_utc: str | None,
        camera_make: str | None,
        camera_model: str | None,
        image_width: int | None,
        image_height: int | None,
        orientation: int | None,
        lens_model: str | None,
        exposure_time_s: float | None = None,
        f_number: float | None = None,
        iso_speed: int | None = None,
        focal_length_mm: float | None = None,
        focal_length_35mm_mm: int | None = None,
        recorded_at_utc: str,
    ) -> None: ...

    def ensure_media_asset_preview_row(self, *, relative_path: str, recorded_at_utc: str) -> None: ...

    def upsert_media_asset_preview(
        self,
        *,
        relative_path: str,
        preview_status: str,
        preview_relative_path: str | None,
        attempted_at_utc: str | None,
        succeeded_at_utc: str | None,
        failed_at_utc: str | None,
        failure_detail: str | None,
        recorded_at_utc: str,
    ) -> None: ...

    def list_duplicate_sha_groups(
        self, *, limit: int, offset: int
    ) -> tuple[int, list[DuplicateShaGroup]]: ...

    def record_path_conflict(
        self,
        *,
        relative_path: str,
        previous_sha256_hex: str,
        current_sha256_hex: str,
        detected_at_utc: str,
    ) -> None: ...

    def list_path_conflicts(self, *, limit: int, offset: int) -> tuple[int, list[PathConflictRecord]]: ...

    def record_storage_index_run(self, record: StorageIndexRunRecord) -> None: ...

    def get_latest_storage_index_run(self) -> StorageIndexRunRecord | None: ...

    def record_catalog_backfill_run(self, record: CatalogBackfillRunRecord) -> None: ...

    def get_latest_catalog_backfill_run(self, backfill_kind: str) -> CatalogBackfillRunRecord | None: ...

    def summarize_storage(self) -> StorageSummary: ...

    def upsert_client_pending(
        self,
        *,
        client_id: str,
        display_name: str,
        enrolled_at_utc: str,
    ) -> ClientRecord: ...

    def get_client(self, client_id: str) -> ClientRecord | None: ...

    def list_clients(self, *, limit: int, offset: int) -> tuple[int, list[ClientRecord]]: ...

    def approve_client(
        self,
        *,
        client_id: str,
        approved_at_utc: str,
        auth_token: str,
    ) -> ClientRecord | None: ...

    def revoke_client(
        self,
        *,
        client_id: str,
        revoked_at_utc: str,
    ) -> ClientRecord | None: ...

    def upsert_client_heartbeat(
        self,
        *,
        client_id: str,
        last_seen_at_utc: str,
        daemon_state: str,
        workload_status: str,
        active_job_id: int | None,
        active_job_label: str | None,
        active_job_status: str | None,
        active_job_ready_to_upload: int | None,
        active_job_uploaded: int | None,
        active_job_retrying: int | None,
        active_job_total_files: int | None,
        active_job_non_terminal_files: int | None,
        active_job_error_files: int | None,
        active_job_blocking_reason: str | None,
        retry_pending_count: int | None,
        retry_next_at_utc: str | None,
        retry_reason: str | None,
        auth_block_reason: str | None,
        recent_error_category: str | None,
        recent_error_message: str | None,
        recent_error_at_utc: str | None,
        updated_at_utc: str,
    ) -> ClientHeartbeatRecord: ...

    def get_client_heartbeat(self, client_id: str) -> ClientHeartbeatRecord | None: ...

    def remove_temp_upload(self, sha256_hex: str) -> None: ...


@dataclass
class InMemoryUploadStateStore:
    """In-memory store used for local tests and fallback development."""

    known_sha256: set[str] = field(default_factory=set)
    upload_temp: dict[str, TempUploadRecord] = field(default_factory=dict)
    stored_files: dict[str, StoredFileRecord] = field(default_factory=dict)
    media_assets: dict[str, MediaAssetRecord] = field(default_factory=dict)
    media_asset_extractions: dict[str, MediaExtractionRecord] = field(default_factory=dict)
    media_asset_previews: dict[str, MediaPreviewRecord] = field(default_factory=dict)
    clients: dict[str, ClientRecord] = field(default_factory=dict)
    client_heartbeats: dict[str, ClientHeartbeatRecord] = field(default_factory=dict)
    path_conflicts: list[PathConflictRecord] = field(default_factory=list)
    latest_index_run: StorageIndexRunRecord | None = None
    latest_catalog_backfill_runs: dict[str, CatalogBackfillRunRecord] = field(default_factory=dict)
    _lock: Lock = field(default_factory=Lock)

    def initialize(self) -> None:
        return

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

    def upsert_media_asset(
        self,
        *,
        relative_path: str,
        sha256_hex: str,
        size_bytes: int,
        origin_kind: str,
        observed_at_utc: str,
        provenance_job_name: str | None = None,
        provenance_original_filename: str | None = None,
    ) -> None:
        with self._lock:
            existing = self.media_assets.get(relative_path)
            first_cataloged = existing.first_cataloged_at_utc if existing is not None else observed_at_utc
            self.media_assets[relative_path] = MediaAssetRecord(
                relative_path=relative_path,
                sha256_hex=sha256_hex,
                size_bytes=size_bytes,
                origin_kind=existing.origin_kind if existing is not None else origin_kind,
                last_observed_origin_kind=origin_kind,
                provenance_job_name=(
                    provenance_job_name
                    if provenance_job_name is not None
                    else (existing.provenance_job_name if existing is not None else None)
                ),
                provenance_original_filename=(
                    provenance_original_filename
                    if provenance_original_filename is not None
                    else (existing.provenance_original_filename if existing is not None else None)
                ),
                first_cataloged_at_utc=first_cataloged,
                last_cataloged_at_utc=observed_at_utc,
                extraction_status=existing.extraction_status if existing is not None else "pending",
                extraction_last_attempted_at_utc=(
                    existing.extraction_last_attempted_at_utc if existing is not None else None
                ),
                extraction_last_succeeded_at_utc=(
                    existing.extraction_last_succeeded_at_utc if existing is not None else None
                ),
                extraction_last_failed_at_utc=(
                    existing.extraction_last_failed_at_utc if existing is not None else None
                ),
                extraction_failure_detail=(
                    existing.extraction_failure_detail if existing is not None else None
                ),
                preview_status=existing.preview_status if existing is not None else "pending",
                preview_relative_path=existing.preview_relative_path if existing is not None else None,
                preview_last_attempted_at_utc=(
                    existing.preview_last_attempted_at_utc if existing is not None else None
                ),
                preview_last_succeeded_at_utc=(
                    existing.preview_last_succeeded_at_utc if existing is not None else None
                ),
                preview_last_failed_at_utc=(
                    existing.preview_last_failed_at_utc if existing is not None else None
                ),
                preview_failure_detail=existing.preview_failure_detail if existing is not None else None,
                capture_timestamp_utc=existing.capture_timestamp_utc if existing is not None else None,
                camera_make=existing.camera_make if existing is not None else None,
                camera_model=existing.camera_model if existing is not None else None,
                image_width=existing.image_width if existing is not None else None,
                image_height=existing.image_height if existing is not None else None,
                orientation=existing.orientation if existing is not None else None,
                lens_model=existing.lens_model if existing is not None else None,
                is_favorite=existing.is_favorite if existing is not None else False,
                is_archived=existing.is_archived if existing is not None else False,
            )
            self.media_asset_extractions.setdefault(
                relative_path,
                MediaExtractionRecord(
                    relative_path=relative_path,
                    extraction_status="pending",
                    extraction_last_attempted_at_utc=None,
                    extraction_last_succeeded_at_utc=None,
                    extraction_last_failed_at_utc=None,
                    extraction_failure_detail=None,
                    capture_timestamp_utc=None,
                    camera_make=None,
                    camera_model=None,
                    image_width=None,
                    image_height=None,
                    orientation=None,
                    lens_model=None,
                ),
            )
            self.media_asset_previews.setdefault(
                relative_path,
                MediaPreviewRecord(
                    relative_path=relative_path,
                    preview_status="pending",
                    preview_relative_path=None,
                    preview_last_attempted_at_utc=None,
                    preview_last_succeeded_at_utc=None,
                    preview_last_failed_at_utc=None,
                    preview_failure_detail=None,
                ),
            )

    def list_media_assets(
        self,
        *,
        limit: int,
        offset: int,
        extraction_status: str | None = None,
        preview_status: str | None = None,
        origin_kind: str | None = None,
        media_type: str | None = None,
        preview_capability: str | None = None,
        is_favorite: bool | None = None,
        is_archived: bool | None = None,
        cataloged_since_utc: str | None = None,
        cataloged_before_utc: str | None = None,
        relative_path_prefix: str | None = None,
    ) -> tuple[int, list[MediaAssetRecord]]:
        with self._lock:
            ordered = sorted(self.media_assets.values(), key=lambda item: item.relative_path)
            ordered = sorted(ordered, key=lambda item: item.last_cataloged_at_utc, reverse=True)
            if extraction_status is not None:
                ordered = [item for item in ordered if item.extraction_status == extraction_status]
            if preview_status is not None:
                ordered = [item for item in ordered if item.preview_status == preview_status]
            if origin_kind is not None:
                ordered = [item for item in ordered if item.origin_kind == origin_kind]
            if media_type is not None:
                ordered = [
                    item for item in ordered if _media_type_for_path(item.relative_path) == media_type
                ]
            if preview_capability is not None:
                ordered = [
                    item
                    for item in ordered
                    if _preview_capability_for_path(item.relative_path) == preview_capability
                ]
            if is_favorite is not None:
                ordered = [item for item in ordered if item.is_favorite == is_favorite]
            if is_archived is not None:
                ordered = [item for item in ordered if item.is_archived == is_archived]
            if cataloged_since_utc is not None:
                ordered = [item for item in ordered if item.last_cataloged_at_utc >= cataloged_since_utc]
            if cataloged_before_utc is not None:
                ordered = [item for item in ordered if item.last_cataloged_at_utc <= cataloged_before_utc]
            if relative_path_prefix:
                # Match both direct folder (prefix/child) and the folder itself.
                # We normalize by stripping trailing '/' so the operator can
                # pass either form and get the same result.
                normalized = relative_path_prefix.rstrip("/") + "/"
                ordered = [
                    item for item in ordered if item.relative_path.startswith(normalized)
                ]
            total = len(ordered)
            return total, ordered[offset : offset + limit]

    def list_media_asset_folders(self) -> list[tuple[str, int, int, int]]:
        # Aggregate folders from the in-memory asset dict. "Folder" is the
        # directory portion of relative_path; depth is the number of
        # path segments. direct_count counts assets whose folder equals
        # this path exactly; total_count includes assets in sub-folders.
        with self._lock:
            direct: dict[str, int] = {}
            total: dict[str, int] = {}
            for record in self.media_assets.values():
                parts = record.relative_path.split("/")
                if len(parts) <= 1:
                    continue
                folder_parts = parts[:-1]
                direct_path = "/".join(folder_parts)
                direct[direct_path] = direct.get(direct_path, 0) + 1
                for depth in range(1, len(folder_parts) + 1):
                    ancestor = "/".join(folder_parts[:depth])
                    total[ancestor] = total.get(ancestor, 0) + 1
            rows: list[tuple[str, int, int, int]] = []
            for path in sorted(total.keys()):
                depth = path.count("/") + 1
                rows.append((path, depth, direct.get(path, 0), total[path]))
            return rows

    def get_media_asset_by_path(self, relative_path: str) -> MediaAssetRecord | None:
        with self._lock:
            return self.media_assets.get(relative_path)

    def set_media_asset_favorite(
        self, *, relative_path: str, is_favorite: bool, updated_at_utc: str
    ) -> MediaAssetRecord | None:
        del updated_at_utc
        with self._lock:
            existing = self.media_assets.get(relative_path)
            if existing is None:
                return None
            updated = MediaAssetRecord(
                relative_path=existing.relative_path,
                sha256_hex=existing.sha256_hex,
                size_bytes=existing.size_bytes,
                origin_kind=existing.origin_kind,
                last_observed_origin_kind=existing.last_observed_origin_kind,
                provenance_job_name=existing.provenance_job_name,
                provenance_original_filename=existing.provenance_original_filename,
                first_cataloged_at_utc=existing.first_cataloged_at_utc,
                last_cataloged_at_utc=existing.last_cataloged_at_utc,
                extraction_status=existing.extraction_status,
                extraction_last_attempted_at_utc=existing.extraction_last_attempted_at_utc,
                extraction_last_succeeded_at_utc=existing.extraction_last_succeeded_at_utc,
                extraction_last_failed_at_utc=existing.extraction_last_failed_at_utc,
                extraction_failure_detail=existing.extraction_failure_detail,
                preview_status=existing.preview_status,
                preview_relative_path=existing.preview_relative_path,
                preview_last_attempted_at_utc=existing.preview_last_attempted_at_utc,
                preview_last_succeeded_at_utc=existing.preview_last_succeeded_at_utc,
                preview_last_failed_at_utc=existing.preview_last_failed_at_utc,
                preview_failure_detail=existing.preview_failure_detail,
                capture_timestamp_utc=existing.capture_timestamp_utc,
                camera_make=existing.camera_make,
                camera_model=existing.camera_model,
                image_width=existing.image_width,
                image_height=existing.image_height,
                orientation=existing.orientation,
                lens_model=existing.lens_model,
                exposure_time_s=existing.exposure_time_s,
                f_number=existing.f_number,
                iso_speed=existing.iso_speed,
                focal_length_mm=existing.focal_length_mm,
                focal_length_35mm_mm=existing.focal_length_35mm_mm,
                is_favorite=is_favorite,
                is_archived=existing.is_archived,
            )
            self.media_assets[relative_path] = updated
            return updated

    def set_media_asset_archived(
        self, *, relative_path: str, is_archived: bool, updated_at_utc: str
    ) -> MediaAssetRecord | None:
        del updated_at_utc
        with self._lock:
            existing = self.media_assets.get(relative_path)
            if existing is None:
                return None
            updated = MediaAssetRecord(
                relative_path=existing.relative_path,
                sha256_hex=existing.sha256_hex,
                size_bytes=existing.size_bytes,
                origin_kind=existing.origin_kind,
                last_observed_origin_kind=existing.last_observed_origin_kind,
                provenance_job_name=existing.provenance_job_name,
                provenance_original_filename=existing.provenance_original_filename,
                first_cataloged_at_utc=existing.first_cataloged_at_utc,
                last_cataloged_at_utc=existing.last_cataloged_at_utc,
                extraction_status=existing.extraction_status,
                extraction_last_attempted_at_utc=existing.extraction_last_attempted_at_utc,
                extraction_last_succeeded_at_utc=existing.extraction_last_succeeded_at_utc,
                extraction_last_failed_at_utc=existing.extraction_last_failed_at_utc,
                extraction_failure_detail=existing.extraction_failure_detail,
                preview_status=existing.preview_status,
                preview_relative_path=existing.preview_relative_path,
                preview_last_attempted_at_utc=existing.preview_last_attempted_at_utc,
                preview_last_succeeded_at_utc=existing.preview_last_succeeded_at_utc,
                preview_last_failed_at_utc=existing.preview_last_failed_at_utc,
                preview_failure_detail=existing.preview_failure_detail,
                capture_timestamp_utc=existing.capture_timestamp_utc,
                camera_make=existing.camera_make,
                camera_model=existing.camera_model,
                image_width=existing.image_width,
                image_height=existing.image_height,
                orientation=existing.orientation,
                lens_model=existing.lens_model,
                exposure_time_s=existing.exposure_time_s,
                f_number=existing.f_number,
                iso_speed=existing.iso_speed,
                focal_length_mm=existing.focal_length_mm,
                focal_length_35mm_mm=existing.focal_length_35mm_mm,
                is_favorite=existing.is_favorite,
                is_archived=is_archived,
            )
            self.media_assets[relative_path] = updated
            return updated

    def _filter_assets_for_backfill(
        self,
        *,
        assets: list[MediaAssetRecord],
        statuses: set[str],
        status_field: str,
        origin_kind: str | None,
        media_type: str | None,
        preview_capability: str | None,
        cataloged_since_utc: str | None,
        cataloged_before_utc: str | None,
    ) -> list[MediaAssetRecord]:
        filtered = [item for item in assets if str(getattr(item, status_field)) in statuses]
        if origin_kind is not None:
            filtered = [item for item in filtered if item.origin_kind == origin_kind]
        if media_type is not None:
            filtered = [item for item in filtered if _media_type_for_path(item.relative_path) == media_type]
        if preview_capability is not None:
            filtered = [
                item
                for item in filtered
                if _preview_capability_for_path(item.relative_path) == preview_capability
            ]
        if cataloged_since_utc is not None:
            filtered = [item for item in filtered if item.last_cataloged_at_utc >= cataloged_since_utc]
        if cataloged_before_utc is not None:
            filtered = [item for item in filtered if item.last_cataloged_at_utc <= cataloged_before_utc]
        return filtered

    def list_media_assets_for_extraction(
        self,
        *,
        extraction_statuses: list[str],
        limit: int,
        origin_kind: str | None = None,
        media_type: str | None = None,
        preview_capability: str | None = None,
        cataloged_since_utc: str | None = None,
        cataloged_before_utc: str | None = None,
    ) -> list[MediaAssetRecord]:
        if limit <= 0 or not extraction_statuses:
            return []
        status_filter = set(extraction_statuses)
        with self._lock:
            ordered = sorted(self.media_assets.values(), key=lambda item: item.relative_path)
            ordered = sorted(ordered, key=lambda item: item.last_cataloged_at_utc, reverse=True)
            filtered = self._filter_assets_for_backfill(
                assets=ordered,
                statuses=status_filter,
                status_field="extraction_status",
                origin_kind=origin_kind,
                media_type=media_type,
                preview_capability=preview_capability,
                cataloged_since_utc=cataloged_since_utc,
                cataloged_before_utc=cataloged_before_utc,
            )
            return filtered[:limit]

    def list_media_assets_for_preview(
        self,
        *,
        preview_statuses: list[str],
        limit: int,
        origin_kind: str | None = None,
        media_type: str | None = None,
        preview_capability: str | None = None,
        cataloged_since_utc: str | None = None,
        cataloged_before_utc: str | None = None,
    ) -> list[MediaAssetRecord]:
        if limit <= 0 or not preview_statuses:
            return []
        status_filter = set(preview_statuses)
        with self._lock:
            ordered = sorted(self.media_assets.values(), key=lambda item: item.relative_path)
            ordered = sorted(ordered, key=lambda item: item.last_cataloged_at_utc, reverse=True)
            filtered = self._filter_assets_for_backfill(
                assets=ordered,
                statuses=status_filter,
                status_field="preview_status",
                origin_kind=origin_kind,
                media_type=media_type,
                preview_capability=preview_capability,
                cataloged_since_utc=cataloged_since_utc,
                cataloged_before_utc=cataloged_before_utc,
            )
            return filtered[:limit]

    def ensure_media_asset_extraction_row(self, *, relative_path: str, recorded_at_utc: str) -> None:
        del recorded_at_utc
        with self._lock:
            self.media_asset_extractions.setdefault(
                relative_path,
                MediaExtractionRecord(
                    relative_path=relative_path,
                    extraction_status="pending",
                    extraction_last_attempted_at_utc=None,
                    extraction_last_succeeded_at_utc=None,
                    extraction_last_failed_at_utc=None,
                    extraction_failure_detail=None,
                    capture_timestamp_utc=None,
                    camera_make=None,
                    camera_model=None,
                    image_width=None,
                    image_height=None,
                    orientation=None,
                    lens_model=None,
                ),
            )

    def upsert_media_asset_extraction(
        self,
        *,
        relative_path: str,
        extraction_status: str,
        attempted_at_utc: str | None,
        succeeded_at_utc: str | None,
        failed_at_utc: str | None,
        failure_detail: str | None,
        capture_timestamp_utc: str | None,
        camera_make: str | None,
        camera_model: str | None,
        image_width: int | None,
        image_height: int | None,
        orientation: int | None,
        lens_model: str | None,
        exposure_time_s: float | None = None,
        f_number: float | None = None,
        iso_speed: int | None = None,
        focal_length_mm: float | None = None,
        focal_length_35mm_mm: int | None = None,
        recorded_at_utc: str,
    ) -> None:
        del recorded_at_utc
        with self._lock:
            self.media_asset_extractions[relative_path] = MediaExtractionRecord(
                relative_path=relative_path,
                extraction_status=extraction_status,
                extraction_last_attempted_at_utc=attempted_at_utc,
                extraction_last_succeeded_at_utc=succeeded_at_utc,
                extraction_last_failed_at_utc=failed_at_utc,
                extraction_failure_detail=failure_detail,
                capture_timestamp_utc=capture_timestamp_utc,
                camera_make=camera_make,
                camera_model=camera_model,
                image_width=image_width,
                image_height=image_height,
                orientation=orientation,
                lens_model=lens_model,
                exposure_time_s=exposure_time_s,
                f_number=f_number,
                iso_speed=iso_speed,
                focal_length_mm=focal_length_mm,
                focal_length_35mm_mm=focal_length_35mm_mm,
            )
            existing_asset = self.media_assets.get(relative_path)
            if existing_asset is None:
                return
            self.media_assets[relative_path] = MediaAssetRecord(
                relative_path=existing_asset.relative_path,
                sha256_hex=existing_asset.sha256_hex,
                size_bytes=existing_asset.size_bytes,
                origin_kind=existing_asset.origin_kind,
                last_observed_origin_kind=existing_asset.last_observed_origin_kind,
                provenance_job_name=existing_asset.provenance_job_name,
                provenance_original_filename=existing_asset.provenance_original_filename,
                first_cataloged_at_utc=existing_asset.first_cataloged_at_utc,
                last_cataloged_at_utc=existing_asset.last_cataloged_at_utc,
                extraction_status=extraction_status,
                extraction_last_attempted_at_utc=attempted_at_utc,
                extraction_last_succeeded_at_utc=succeeded_at_utc,
                extraction_last_failed_at_utc=failed_at_utc,
                extraction_failure_detail=failure_detail,
                preview_status=existing_asset.preview_status,
                preview_relative_path=existing_asset.preview_relative_path,
                preview_last_attempted_at_utc=existing_asset.preview_last_attempted_at_utc,
                preview_last_succeeded_at_utc=existing_asset.preview_last_succeeded_at_utc,
                preview_last_failed_at_utc=existing_asset.preview_last_failed_at_utc,
                preview_failure_detail=existing_asset.preview_failure_detail,
                capture_timestamp_utc=capture_timestamp_utc,
                camera_make=camera_make,
                camera_model=camera_model,
                image_width=image_width,
                image_height=image_height,
                orientation=orientation,
                lens_model=lens_model,
                exposure_time_s=exposure_time_s,
                f_number=f_number,
                iso_speed=iso_speed,
                focal_length_mm=focal_length_mm,
                focal_length_35mm_mm=focal_length_35mm_mm,
                is_favorite=existing_asset.is_favorite,
                is_archived=existing_asset.is_archived,
            )

    def ensure_media_asset_preview_row(self, *, relative_path: str, recorded_at_utc: str) -> None:
        del recorded_at_utc
        with self._lock:
            self.media_asset_previews.setdefault(
                relative_path,
                MediaPreviewRecord(
                    relative_path=relative_path,
                    preview_status="pending",
                    preview_relative_path=None,
                    preview_last_attempted_at_utc=None,
                    preview_last_succeeded_at_utc=None,
                    preview_last_failed_at_utc=None,
                    preview_failure_detail=None,
                ),
            )

    def upsert_media_asset_preview(
        self,
        *,
        relative_path: str,
        preview_status: str,
        preview_relative_path: str | None,
        attempted_at_utc: str | None,
        succeeded_at_utc: str | None,
        failed_at_utc: str | None,
        failure_detail: str | None,
        recorded_at_utc: str,
    ) -> None:
        del recorded_at_utc
        with self._lock:
            self.media_asset_previews[relative_path] = MediaPreviewRecord(
                relative_path=relative_path,
                preview_status=preview_status,
                preview_relative_path=preview_relative_path,
                preview_last_attempted_at_utc=attempted_at_utc,
                preview_last_succeeded_at_utc=succeeded_at_utc,
                preview_last_failed_at_utc=failed_at_utc,
                preview_failure_detail=failure_detail,
            )
            existing_asset = self.media_assets.get(relative_path)
            if existing_asset is None:
                return
            self.media_assets[relative_path] = MediaAssetRecord(
                relative_path=existing_asset.relative_path,
                sha256_hex=existing_asset.sha256_hex,
                size_bytes=existing_asset.size_bytes,
                origin_kind=existing_asset.origin_kind,
                last_observed_origin_kind=existing_asset.last_observed_origin_kind,
                provenance_job_name=existing_asset.provenance_job_name,
                provenance_original_filename=existing_asset.provenance_original_filename,
                first_cataloged_at_utc=existing_asset.first_cataloged_at_utc,
                last_cataloged_at_utc=existing_asset.last_cataloged_at_utc,
                extraction_status=existing_asset.extraction_status,
                extraction_last_attempted_at_utc=existing_asset.extraction_last_attempted_at_utc,
                extraction_last_succeeded_at_utc=existing_asset.extraction_last_succeeded_at_utc,
                extraction_last_failed_at_utc=existing_asset.extraction_last_failed_at_utc,
                extraction_failure_detail=existing_asset.extraction_failure_detail,
                preview_status=preview_status,
                preview_relative_path=preview_relative_path,
                preview_last_attempted_at_utc=attempted_at_utc,
                preview_last_succeeded_at_utc=succeeded_at_utc,
                preview_last_failed_at_utc=failed_at_utc,
                preview_failure_detail=failure_detail,
                capture_timestamp_utc=existing_asset.capture_timestamp_utc,
                camera_make=existing_asset.camera_make,
                camera_model=existing_asset.camera_model,
                image_width=existing_asset.image_width,
                image_height=existing_asset.image_height,
                orientation=existing_asset.orientation,
                lens_model=existing_asset.lens_model,
                exposure_time_s=existing_asset.exposure_time_s,
                f_number=existing_asset.f_number,
                iso_speed=existing_asset.iso_speed,
                focal_length_mm=existing_asset.focal_length_mm,
                focal_length_35mm_mm=existing_asset.focal_length_35mm_mm,
                is_favorite=existing_asset.is_favorite,
                is_archived=existing_asset.is_archived,
            )

    def list_duplicate_sha_groups(
        self, *, limit: int, offset: int
    ) -> tuple[int, list[DuplicateShaGroup]]:
        with self._lock:
            grouped: dict[str, list[StoredFileRecord]] = {}
            for record in self.stored_files.values():
                grouped.setdefault(record.sha256_hex, []).append(record)
            groups = [
                DuplicateShaGroup(
                    sha256_hex=sha256_hex,
                    file_count=len(records),
                    first_seen_at_utc=min(record.first_seen_at_utc for record in records),
                    last_seen_at_utc=max(record.last_seen_at_utc for record in records),
                    relative_paths=tuple(sorted(record.relative_path for record in records)),
                )
                for sha256_hex, records in grouped.items()
                if len(records) > 1
            ]
            ordered = sorted(
                groups,
                key=lambda item: (-item.file_count, item.last_seen_at_utc, item.sha256_hex),
                reverse=False,
            )
            total = len(ordered)
            return total, ordered[offset : offset + limit]

    def record_path_conflict(
        self,
        *,
        relative_path: str,
        previous_sha256_hex: str,
        current_sha256_hex: str,
        detected_at_utc: str,
    ) -> None:
        with self._lock:
            self.path_conflicts.append(
                PathConflictRecord(
                    relative_path=relative_path,
                    previous_sha256_hex=previous_sha256_hex,
                    current_sha256_hex=current_sha256_hex,
                    detected_at_utc=detected_at_utc,
                )
            )

    def list_path_conflicts(self, *, limit: int, offset: int) -> tuple[int, list[PathConflictRecord]]:
        with self._lock:
            ordered = sorted(
                self.path_conflicts,
                key=lambda item: (item.detected_at_utc, item.relative_path),
                reverse=True,
            )
            total = len(ordered)
            return total, ordered[offset : offset + limit]

    def record_storage_index_run(self, record: StorageIndexRunRecord) -> None:
        with self._lock:
            self.latest_index_run = record

    def get_latest_storage_index_run(self) -> StorageIndexRunRecord | None:
        with self._lock:
            return self.latest_index_run

    def record_catalog_backfill_run(self, record: CatalogBackfillRunRecord) -> None:
        with self._lock:
            self.latest_catalog_backfill_runs[record.backfill_kind] = record

    def get_latest_catalog_backfill_run(self, backfill_kind: str) -> CatalogBackfillRunRecord | None:
        with self._lock:
            return self.latest_catalog_backfill_runs.get(backfill_kind)

    def summarize_storage(self) -> StorageSummary:
        now = datetime.now(UTC)
        threshold = now - timedelta(hours=24)
        with self._lock:
            records = list(self.stored_files.values())
            duplicate_file_paths = len(records) - len({record.sha256_hex for record in records})
            indexed_records = [record for record in records if record.source_kind == "index_scan"]
            uploaded_records = [record for record in records if record.source_kind == "upload_verify"]
            recent_indexed = 0
            recent_uploaded = 0
            last_indexed: str | None = None
            last_uploaded: str | None = None

            for record in indexed_records:
                try:
                    seen_at = datetime.fromisoformat(record.last_seen_at_utc)
                except ValueError:
                    continue
                if seen_at >= threshold:
                    recent_indexed += 1
                if last_indexed is None or record.last_seen_at_utc > last_indexed:
                    last_indexed = record.last_seen_at_utc

            for record in uploaded_records:
                try:
                    seen_at = datetime.fromisoformat(record.last_seen_at_utc)
                except ValueError:
                    continue
                if seen_at >= threshold:
                    recent_uploaded += 1
                if last_uploaded is None or record.last_seen_at_utc > last_uploaded:
                    last_uploaded = record.last_seen_at_utc

            return StorageSummary(
                total_known_sha256=len(self.known_sha256),
                total_stored_files=len(records),
                indexed_files=len(indexed_records),
                uploaded_files=len(uploaded_records),
                duplicate_file_paths=duplicate_file_paths,
                recent_indexed_files_24h=recent_indexed,
                recent_uploaded_files_24h=recent_uploaded,
                last_indexed_at_utc=last_indexed,
                last_uploaded_at_utc=last_uploaded,
            )

    def remove_temp_upload(self, sha256_hex: str) -> None:
        with self._lock:
            self.upload_temp.pop(sha256_hex, None)

    def upsert_client_pending(
        self,
        *,
        client_id: str,
        display_name: str,
        enrolled_at_utc: str,
    ) -> ClientRecord:
        with self._lock:
            existing = self.clients.get(client_id)
            first_seen = existing.first_seen_at_utc if existing is not None else enrolled_at_utc
            enrollment_status = "pending" if existing is None else existing.enrollment_status
            keep_existing_identity = existing is not None and existing.enrollment_status != "pending"
            approved_at_utc = (
                existing.approved_at_utc if keep_existing_identity and existing is not None else None
            )
            revoked_at_utc = (
                existing.revoked_at_utc if keep_existing_identity and existing is not None else None
            )
            auth_token = existing.auth_token if keep_existing_identity and existing is not None else None
            updated = ClientRecord(
                client_id=client_id,
                display_name=display_name,
                enrollment_status=enrollment_status,
                first_seen_at_utc=first_seen,
                last_enrolled_at_utc=enrolled_at_utc,
                approved_at_utc=approved_at_utc,
                revoked_at_utc=revoked_at_utc,
                auth_token=auth_token,
            )
            self.clients[client_id] = updated
            return updated

    def get_client(self, client_id: str) -> ClientRecord | None:
        with self._lock:
            return self.clients.get(client_id)

    def list_clients(self, *, limit: int, offset: int) -> tuple[int, list[ClientRecord]]:
        with self._lock:
            rows = sorted(
                self.clients.values(),
                key=lambda client: (client.first_seen_at_utc, client.client_id),
                reverse=True,
            )
            total = len(rows)
            return total, rows[offset : offset + limit]

    def approve_client(
        self,
        *,
        client_id: str,
        approved_at_utc: str,
        auth_token: str,
    ) -> ClientRecord | None:
        with self._lock:
            existing = self.clients.get(client_id)
            if existing is None:
                return None
            updated = ClientRecord(
                client_id=existing.client_id,
                display_name=existing.display_name,
                enrollment_status="approved",
                first_seen_at_utc=existing.first_seen_at_utc,
                last_enrolled_at_utc=existing.last_enrolled_at_utc,
                approved_at_utc=approved_at_utc,
                revoked_at_utc=None,
                auth_token=auth_token,
            )
            self.clients[client_id] = updated
            return updated

    def revoke_client(
        self,
        *,
        client_id: str,
        revoked_at_utc: str,
    ) -> ClientRecord | None:
        with self._lock:
            existing = self.clients.get(client_id)
            if existing is None:
                return None
            updated = ClientRecord(
                client_id=existing.client_id,
                display_name=existing.display_name,
                enrollment_status="revoked",
                first_seen_at_utc=existing.first_seen_at_utc,
                last_enrolled_at_utc=existing.last_enrolled_at_utc,
                approved_at_utc=existing.approved_at_utc,
                revoked_at_utc=revoked_at_utc,
                auth_token=existing.auth_token,
            )
            self.clients[client_id] = updated
            return updated

    def upsert_client_heartbeat(
        self,
        *,
        client_id: str,
        last_seen_at_utc: str,
        daemon_state: str,
        workload_status: str,
        active_job_id: int | None,
        active_job_label: str | None,
        active_job_status: str | None,
        active_job_ready_to_upload: int | None,
        active_job_uploaded: int | None,
        active_job_retrying: int | None,
        active_job_total_files: int | None,
        active_job_non_terminal_files: int | None,
        active_job_error_files: int | None,
        active_job_blocking_reason: str | None,
        retry_pending_count: int | None,
        retry_next_at_utc: str | None,
        retry_reason: str | None,
        auth_block_reason: str | None,
        recent_error_category: str | None,
        recent_error_message: str | None,
        recent_error_at_utc: str | None,
        updated_at_utc: str,
    ) -> ClientHeartbeatRecord:
        with self._lock:
            record = ClientHeartbeatRecord(
                client_id=client_id,
                last_seen_at_utc=last_seen_at_utc,
                daemon_state=daemon_state,
                workload_status=workload_status,
                active_job_id=active_job_id,
                active_job_label=active_job_label,
                active_job_status=active_job_status,
                active_job_ready_to_upload=active_job_ready_to_upload,
                active_job_uploaded=active_job_uploaded,
                active_job_retrying=active_job_retrying,
                active_job_total_files=active_job_total_files,
                active_job_non_terminal_files=active_job_non_terminal_files,
                active_job_error_files=active_job_error_files,
                active_job_blocking_reason=active_job_blocking_reason,
                retry_pending_count=retry_pending_count,
                retry_next_at_utc=retry_next_at_utc,
                retry_reason=retry_reason,
                auth_block_reason=auth_block_reason,
                recent_error_category=recent_error_category,
                recent_error_message=recent_error_message,
                recent_error_at_utc=recent_error_at_utc,
                updated_at_utc=updated_at_utc,
            )
            self.client_heartbeats[client_id] = record
            return record

    def get_client_heartbeat(self, client_id: str) -> ClientHeartbeatRecord | None:
        with self._lock:
            return self.client_heartbeats.get(client_id)


@dataclass
class PostgresUploadStateStore:
    """PostgreSQL-backed state store for durable SHA dedup and file metadata."""

    database_url: str

    def _connect(self):
        import psycopg

        return psycopg.connect(self.database_url)

    def initialize(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS api_known_sha256 (
                        sha256_hex TEXT PRIMARY KEY,
                        created_at_utc TEXT NOT NULL
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS api_temp_uploads (
                        sha256_hex TEXT PRIMARY KEY,
                        size_bytes BIGINT NOT NULL,
                        temp_relative_path TEXT,
                        job_name TEXT,
                        original_filename TEXT,
                        received_at_utc TEXT,
                        created_at_utc TEXT NOT NULL
                    );
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE api_temp_uploads
                    ADD COLUMN IF NOT EXISTS temp_relative_path TEXT;
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE api_temp_uploads
                    ADD COLUMN IF NOT EXISTS job_name TEXT;
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE api_temp_uploads
                    ADD COLUMN IF NOT EXISTS original_filename TEXT;
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE api_temp_uploads
                    ADD COLUMN IF NOT EXISTS received_at_utc TEXT;
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS api_stored_files (
                        relative_path TEXT PRIMARY KEY,
                        sha256_hex TEXT NOT NULL,
                        size_bytes BIGINT NOT NULL,
                        source_kind TEXT NOT NULL,
                        first_seen_at_utc TEXT NOT NULL,
                        last_seen_at_utc TEXT NOT NULL
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS api_media_assets (
                        relative_path TEXT PRIMARY KEY REFERENCES api_stored_files(relative_path)
                            ON DELETE CASCADE,
                        sha256_hex TEXT NOT NULL,
                        size_bytes BIGINT NOT NULL,
                        origin_kind TEXT NOT NULL,
                        last_observed_origin_kind TEXT NOT NULL,
                        provenance_job_name TEXT,
                        provenance_original_filename TEXT,
                        first_cataloged_at_utc TEXT NOT NULL,
                        last_cataloged_at_utc TEXT NOT NULL,
                        is_favorite BOOLEAN NOT NULL DEFAULT FALSE,
                        is_archived BOOLEAN NOT NULL DEFAULT FALSE
                    );
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE api_media_assets
                    ADD COLUMN IF NOT EXISTS is_favorite BOOLEAN NOT NULL DEFAULT FALSE;
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE api_media_assets
                    ADD COLUMN IF NOT EXISTS is_archived BOOLEAN NOT NULL DEFAULT FALSE;
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS api_media_asset_extractions (
                        relative_path TEXT PRIMARY KEY REFERENCES api_media_assets(relative_path)
                            ON DELETE CASCADE,
                        extraction_status TEXT NOT NULL,
                        last_attempted_at_utc TEXT,
                        last_succeeded_at_utc TEXT,
                        last_failed_at_utc TEXT,
                        failure_detail TEXT,
                        capture_timestamp_utc TEXT,
                        camera_make TEXT,
                        camera_model TEXT,
                        image_width INTEGER,
                        image_height INTEGER,
                        orientation INTEGER,
                        lens_model TEXT,
                        updated_at_utc TEXT NOT NULL
                    );
                    """
                )
                # Phase 3.A: additive exposure-metadata columns.
                cur.execute(
                    """
                    ALTER TABLE api_media_asset_extractions
                    ADD COLUMN IF NOT EXISTS exposure_time_s DOUBLE PRECISION;
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE api_media_asset_extractions
                    ADD COLUMN IF NOT EXISTS f_number DOUBLE PRECISION;
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE api_media_asset_extractions
                    ADD COLUMN IF NOT EXISTS iso_speed INTEGER;
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE api_media_asset_extractions
                    ADD COLUMN IF NOT EXISTS focal_length_mm DOUBLE PRECISION;
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE api_media_asset_extractions
                    ADD COLUMN IF NOT EXISTS focal_length_35mm_mm INTEGER;
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS api_media_asset_previews (
                        relative_path TEXT PRIMARY KEY REFERENCES api_media_assets(relative_path)
                            ON DELETE CASCADE,
                        preview_status TEXT NOT NULL,
                        preview_relative_path TEXT,
                        last_attempted_at_utc TEXT,
                        last_succeeded_at_utc TEXT,
                        last_failed_at_utc TEXT,
                        failure_detail TEXT,
                        updated_at_utc TEXT NOT NULL
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS api_storage_path_conflicts (
                        id BIGSERIAL PRIMARY KEY,
                        relative_path TEXT NOT NULL,
                        previous_sha256_hex TEXT NOT NULL,
                        current_sha256_hex TEXT NOT NULL,
                        detected_at_utc TEXT NOT NULL
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS api_storage_index_runs (
                        singleton_key BOOLEAN PRIMARY KEY DEFAULT TRUE,
                        scanned_files INTEGER NOT NULL,
                        indexed_files INTEGER NOT NULL,
                        new_sha_entries INTEGER NOT NULL,
                        existing_sha_matches INTEGER NOT NULL,
                        path_conflicts INTEGER NOT NULL,
                        errors INTEGER NOT NULL,
                        completed_at_utc TEXT NOT NULL
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS api_catalog_backfill_runs (
                        backfill_kind TEXT PRIMARY KEY,
                        requested_statuses TEXT[] NOT NULL,
                        selected_count INTEGER NOT NULL,
                        processed_count INTEGER NOT NULL,
                        succeeded_count INTEGER NOT NULL,
                        failed_count INTEGER NOT NULL,
                        remaining_pending_count INTEGER NOT NULL,
                        remaining_failed_count INTEGER NOT NULL,
                        filter_origin_kind TEXT,
                        filter_media_type TEXT,
                        filter_preview_capability TEXT,
                        filter_cataloged_since_utc TEXT,
                        filter_cataloged_before_utc TEXT,
                        limit_count INTEGER NOT NULL,
                        completed_at_utc TEXT NOT NULL
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS api_clients (
                        client_id TEXT PRIMARY KEY,
                        display_name TEXT NOT NULL,
                        enrollment_status TEXT NOT NULL,
                        first_seen_at_utc TEXT NOT NULL,
                        last_enrolled_at_utc TEXT NOT NULL,
                        approved_at_utc TEXT,
                        revoked_at_utc TEXT,
                        auth_token TEXT
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS api_client_heartbeats (
                        client_id TEXT PRIMARY KEY REFERENCES api_clients(client_id) ON DELETE CASCADE,
                        last_seen_at_utc TEXT NOT NULL,
                        daemon_state TEXT NOT NULL,
                        workload_status TEXT NOT NULL,
                        active_job_id BIGINT,
                        active_job_label TEXT,
                        active_job_status TEXT,
                        active_job_ready_to_upload INTEGER,
                        active_job_uploaded INTEGER,
                        active_job_retrying INTEGER,
                        retry_pending_count INTEGER,
                        retry_next_at_utc TEXT,
                        retry_reason TEXT,
                        auth_block_reason TEXT,
                        recent_error_category TEXT,
                        recent_error_message TEXT,
                        recent_error_at_utc TEXT,
                        updated_at_utc TEXT NOT NULL
                    );
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE api_client_heartbeats
                    ADD COLUMN IF NOT EXISTS active_job_total_files INTEGER;
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE api_client_heartbeats
                    ADD COLUMN IF NOT EXISTS active_job_non_terminal_files INTEGER;
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE api_client_heartbeats
                    ADD COLUMN IF NOT EXISTS active_job_error_files INTEGER;
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE api_client_heartbeats
                    ADD COLUMN IF NOT EXISTS active_job_blocking_reason TEXT;
                    """
                )
            conn.commit()

    def has_sha(self, sha256_hex: str) -> bool:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM api_known_sha256 WHERE sha256_hex = %s LIMIT 1;",
                    (sha256_hex,),
                )
                return cur.fetchone() is not None

    def has_shas(self, sha256_hex_values: list[str]) -> set[str]:
        if not sha256_hex_values:
            return set()

        # Preserve deterministic semantics for callers while reducing round-trips to PostgreSQL.
        unique_values = list(dict.fromkeys(sha256_hex_values))
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT sha256_hex
                    FROM api_known_sha256
                    WHERE sha256_hex = ANY(%s);
                    """,
                    (unique_values,),
                )
                return {str(row[0]) for row in cur.fetchall()}

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
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO api_temp_uploads (
                        sha256_hex, size_bytes, temp_relative_path, job_name,
                        original_filename, received_at_utc, created_at_utc
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (sha256_hex) DO UPDATE
                    SET size_bytes = EXCLUDED.size_bytes,
                        temp_relative_path = EXCLUDED.temp_relative_path,
                        job_name = EXCLUDED.job_name,
                        original_filename = EXCLUDED.original_filename,
                        received_at_utc = EXCLUDED.received_at_utc,
                        created_at_utc = EXCLUDED.created_at_utc;
                    """,
                    (
                        sha256_hex,
                        size_bytes,
                        temp_relative_path,
                        job_name,
                        original_filename,
                        received_at_utc,
                        now,
                    ),
                )
            conn.commit()

    def get_temp_upload(self, sha256_hex: str) -> TempUploadRecord | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT size_bytes, temp_relative_path, job_name, original_filename, received_at_utc
                    FROM api_temp_uploads
                    WHERE sha256_hex = %s
                    LIMIT 1;
                    """,
                    (sha256_hex,),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                temp_relative_path = str(row[1] or "")
                job_name = str(row[2] or "")
                original_filename = str(row[3] or "")
                received_at_utc = str(row[4] or "")
                if not temp_relative_path or not job_name or not original_filename or not received_at_utc:
                    return None
                return TempUploadRecord(
                    sha256_hex=sha256_hex,
                    size_bytes=int(row[0]),
                    temp_relative_path=temp_relative_path,
                    job_name=job_name,
                    original_filename=original_filename,
                    received_at_utc=received_at_utc,
                )

    def mark_sha_verified(self, sha256_hex: str) -> bool:
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO api_known_sha256 (sha256_hex, created_at_utc)
                    VALUES (%s, %s)
                    ON CONFLICT (sha256_hex) DO NOTHING;
                    """,
                    (sha256_hex, now),
                )
                inserted = cur.rowcount > 0
            conn.commit()
            return inserted

    def upsert_stored_file(
        self,
        *,
        relative_path: str,
        sha256_hex: str,
        size_bytes: int,
        source_kind: str,
        seen_at_utc: str,
    ) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO api_stored_files (
                        relative_path, sha256_hex, size_bytes, source_kind,
                        first_seen_at_utc, last_seen_at_utc
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (relative_path) DO UPDATE
                    SET sha256_hex = EXCLUDED.sha256_hex,
                        size_bytes = EXCLUDED.size_bytes,
                        source_kind = EXCLUDED.source_kind,
                        last_seen_at_utc = EXCLUDED.last_seen_at_utc;
                    """,
                    (relative_path, sha256_hex, size_bytes, source_kind, seen_at_utc, seen_at_utc),
                )
            conn.commit()

    def get_stored_file_by_path(self, relative_path: str) -> StoredFileRecord | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT sha256_hex, size_bytes, source_kind, first_seen_at_utc, last_seen_at_utc
                    FROM api_stored_files
                    WHERE relative_path = %s
                    LIMIT 1;
                    """,
                    (relative_path,),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                return StoredFileRecord(
                    relative_path=relative_path,
                    sha256_hex=str(row[0]),
                    size_bytes=int(row[1]),
                    source_kind=str(row[2]),
                    first_seen_at_utc=str(row[3]),
                    last_seen_at_utc=str(row[4]),
                )

    def list_stored_files(self, *, limit: int, offset: int) -> tuple[int, list[StoredFileRecord]]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM api_stored_files;")
                count_row = cur.fetchone()
                total = int(count_row[0]) if count_row is not None else 0
                cur.execute(
                    """
                    SELECT
                        relative_path,
                        sha256_hex,
                        size_bytes,
                        source_kind,
                        first_seen_at_utc,
                        last_seen_at_utc
                    FROM api_stored_files
                    ORDER BY last_seen_at_utc DESC, relative_path ASC
                    LIMIT %s
                    OFFSET %s;
                    """,
                    (limit, offset),
                )
                rows = cur.fetchall()
                records = [
                    StoredFileRecord(
                        relative_path=str(row[0]),
                        sha256_hex=str(row[1]),
                        size_bytes=int(row[2]),
                        source_kind=str(row[3]),
                        first_seen_at_utc=str(row[4]),
                        last_seen_at_utc=str(row[5]),
                    )
                    for row in rows
                ]
                return total, records

    def upsert_media_asset(
        self,
        *,
        relative_path: str,
        sha256_hex: str,
        size_bytes: int,
        origin_kind: str,
        observed_at_utc: str,
        provenance_job_name: str | None = None,
        provenance_original_filename: str | None = None,
    ) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO api_media_assets (
                        relative_path,
                        sha256_hex,
                        size_bytes,
                        origin_kind,
                        last_observed_origin_kind,
                        provenance_job_name,
                        provenance_original_filename,
                        first_cataloged_at_utc,
                        last_cataloged_at_utc,
                        is_favorite,
                        is_archived
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE, FALSE)
                    ON CONFLICT (relative_path) DO UPDATE
                    SET sha256_hex = EXCLUDED.sha256_hex,
                        size_bytes = EXCLUDED.size_bytes,
                        last_observed_origin_kind = EXCLUDED.last_observed_origin_kind,
                        provenance_job_name = COALESCE(
                            EXCLUDED.provenance_job_name,
                            api_media_assets.provenance_job_name
                        ),
                        provenance_original_filename = COALESCE(
                            EXCLUDED.provenance_original_filename,
                            api_media_assets.provenance_original_filename
                        ),
                        last_cataloged_at_utc = EXCLUDED.last_cataloged_at_utc,
                        is_favorite = api_media_assets.is_favorite,
                        is_archived = api_media_assets.is_archived;
                    """,
                    (
                        relative_path,
                        sha256_hex,
                        size_bytes,
                        origin_kind,
                        origin_kind,
                        provenance_job_name,
                        provenance_original_filename,
                        observed_at_utc,
                        observed_at_utc,
                    ),
                )
            conn.commit()

    def list_media_assets(
        self,
        *,
        limit: int,
        offset: int,
        extraction_status: str | None = None,
        preview_status: str | None = None,
        origin_kind: str | None = None,
        media_type: str | None = None,
        preview_capability: str | None = None,
        is_favorite: bool | None = None,
        is_archived: bool | None = None,
        cataloged_since_utc: str | None = None,
        cataloged_before_utc: str | None = None,
        relative_path_prefix: str | None = None,
    ) -> tuple[int, list[MediaAssetRecord]]:
        where_clauses = []
        params: list[object] = []
        if extraction_status is not None:
            where_clauses.append("COALESCE(me.extraction_status, 'pending') = %s")
            params.append(extraction_status)
        if preview_status is not None:
            where_clauses.append("COALESCE(mp.preview_status, 'pending') = %s")
            params.append(preview_status)
        if origin_kind is not None:
            where_clauses.append("ma.origin_kind = %s")
            params.append(origin_kind)
        if is_favorite is not None:
            where_clauses.append("ma.is_favorite = %s")
            params.append(is_favorite)
        if is_archived is not None:
            where_clauses.append("ma.is_archived = %s")
            params.append(is_archived)
        if relative_path_prefix:
            normalized_prefix = relative_path_prefix.rstrip("/") + "/"
            where_clauses.append("ma.relative_path LIKE %s")
            # Escape SQL LIKE wildcards in the prefix so "%" and "_" in folder
            # names cannot match unrelated paths.
            escaped = (
                normalized_prefix.replace("\\", "\\\\")
                .replace("%", "\\%")
                .replace("_", "\\_")
            )
            params.append(escaped + "%")
        if media_type is not None:
            media_type_suffixes = _MEDIA_TYPE_SUFFIXES.get(media_type)
            if media_type_suffixes is not None:
                like_clauses = ["LOWER(ma.relative_path) LIKE %s" for _ in media_type_suffixes]
                where_clauses.append("(" + " OR ".join(like_clauses) + ")")
                params.extend([f"%{suffix}" for suffix in media_type_suffixes])
            else:
                all_suffixes = [suffix for values in _MEDIA_TYPE_SUFFIXES.values() for suffix in values]
                not_like_clauses = ["LOWER(ma.relative_path) NOT LIKE %s" for _ in all_suffixes]
                where_clauses.append("(" + " AND ".join(not_like_clauses) + ")")
                params.extend([f"%{suffix}" for suffix in all_suffixes])
        if preview_capability is not None:
            previewable_suffixes = tuple(sorted(_PREVIEWABLE_SUFFIXES))
            if preview_capability == "previewable":
                like_clauses = ["LOWER(ma.relative_path) LIKE %s" for _ in previewable_suffixes]
                where_clauses.append("(" + " OR ".join(like_clauses) + ")")
                params.extend([f"%{suffix}" for suffix in previewable_suffixes])
            else:
                not_like_clauses = ["LOWER(ma.relative_path) NOT LIKE %s" for _ in previewable_suffixes]
                where_clauses.append("(" + " AND ".join(not_like_clauses) + ")")
                params.extend([f"%{suffix}" for suffix in previewable_suffixes])
        if cataloged_since_utc is not None:
            where_clauses.append("ma.last_cataloged_at_utc >= %s")
            params.append(cataloged_since_utc)
        if cataloged_before_utc is not None:
            where_clauses.append("ma.last_cataloged_at_utc <= %s")
            params.append(cataloged_before_utc)
        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM api_media_assets ma
                    LEFT JOIN api_media_asset_extractions me
                        ON me.relative_path = ma.relative_path
                    LEFT JOIN api_media_asset_previews mp
                        ON mp.relative_path = ma.relative_path
                    {where_sql};
                    """,
                    tuple(params),
                )
                count_row = cur.fetchone()
                total = int(count_row[0]) if count_row is not None else 0
                cur.execute(
                    f"""
                    SELECT
                        ma.relative_path,
                        ma.sha256_hex,
                        ma.size_bytes,
                        ma.origin_kind,
                        ma.last_observed_origin_kind,
                        ma.provenance_job_name,
                        ma.provenance_original_filename,
                        ma.first_cataloged_at_utc,
                        ma.last_cataloged_at_utc,
                        ma.is_favorite,
                        ma.is_archived,
                        COALESCE(me.extraction_status, 'pending') AS extraction_status,
                        me.last_attempted_at_utc,
                        me.last_succeeded_at_utc,
                        me.last_failed_at_utc,
                        me.failure_detail,
                        COALESCE(mp.preview_status, 'pending') AS preview_status,
                        mp.preview_relative_path,
                        mp.last_attempted_at_utc,
                        mp.last_succeeded_at_utc,
                        mp.last_failed_at_utc,
                        mp.failure_detail,
                        me.capture_timestamp_utc,
                        me.camera_make,
                        me.camera_model,
                        me.image_width,
                        me.image_height,
                        me.orientation,
                        me.lens_model,
                        me.exposure_time_s,
                        me.f_number,
                        me.iso_speed,
                        me.focal_length_mm,
                        me.focal_length_35mm_mm
                    FROM api_media_assets ma
                    LEFT JOIN api_media_asset_extractions me
                        ON me.relative_path = ma.relative_path
                    LEFT JOIN api_media_asset_previews mp
                        ON mp.relative_path = ma.relative_path
                    {where_sql}
                    ORDER BY ma.last_cataloged_at_utc DESC, ma.relative_path ASC
                    LIMIT %s
                    OFFSET %s;
                    """,
                    tuple([*params, limit, offset]),
                )
                rows = cur.fetchall()
                records = [
                    MediaAssetRecord(
                        relative_path=str(row[0]),
                        sha256_hex=str(row[1]),
                        size_bytes=int(row[2]),
                        origin_kind=str(row[3]),
                        last_observed_origin_kind=str(row[4]),
                        provenance_job_name=str(row[5]) if row[5] is not None else None,
                        provenance_original_filename=str(row[6]) if row[6] is not None else None,
                        first_cataloged_at_utc=str(row[7]),
                        last_cataloged_at_utc=str(row[8]),
                        is_favorite=bool(row[9]),
                        is_archived=bool(row[10]),
                        extraction_status=str(row[11]),
                        extraction_last_attempted_at_utc=str(row[12]) if row[12] is not None else None,
                        extraction_last_succeeded_at_utc=str(row[13]) if row[13] is not None else None,
                        extraction_last_failed_at_utc=str(row[14]) if row[14] is not None else None,
                        extraction_failure_detail=str(row[15]) if row[15] is not None else None,
                        preview_status=str(row[16]),
                        preview_relative_path=str(row[17]) if row[17] is not None else None,
                        preview_last_attempted_at_utc=str(row[18]) if row[18] is not None else None,
                        preview_last_succeeded_at_utc=str(row[19]) if row[19] is not None else None,
                        preview_last_failed_at_utc=str(row[20]) if row[20] is not None else None,
                        preview_failure_detail=str(row[21]) if row[21] is not None else None,
                        capture_timestamp_utc=str(row[22]) if row[22] is not None else None,
                        camera_make=str(row[23]) if row[23] is not None else None,
                        camera_model=str(row[24]) if row[24] is not None else None,
                        image_width=int(row[25]) if row[25] is not None else None,
                        image_height=int(row[26]) if row[26] is not None else None,
                        orientation=int(row[27]) if row[27] is not None else None,
                        lens_model=str(row[28]) if row[28] is not None else None,
                        exposure_time_s=float(row[29]) if row[29] is not None else None,
                        f_number=float(row[30]) if row[30] is not None else None,
                        iso_speed=int(row[31]) if row[31] is not None else None,
                        focal_length_mm=float(row[32]) if row[32] is not None else None,
                        focal_length_35mm_mm=int(row[33]) if row[33] is not None else None,
                    )
                    for row in rows
                ]
                return total, records

    def list_media_asset_folders(self) -> list[tuple[str, int, int, int]]:
        # Aggregate folder rows from api_media_assets. We compute direct and
        # total counts per ancestor folder by expanding each asset's path into
        # all of its ancestor folder prefixes (depth 1..N-1), then summing.
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH path_parts AS (
                        SELECT
                            relative_path,
                            string_to_array(relative_path, '/') AS parts
                        FROM api_media_assets
                    ),
                    ancestors AS (
                        SELECT
                            relative_path,
                            array_length(parts, 1) AS num_parts,
                            d AS depth,
                            array_to_string(parts[1:d], '/') AS folder_path
                        FROM path_parts,
                             generate_series(1, COALESCE(array_length(parts, 1), 1) - 1) AS d
                        WHERE array_length(parts, 1) > 1
                    )
                    SELECT
                        folder_path,
                        depth,
                        SUM(CASE WHEN depth = num_parts - 1 THEN 1 ELSE 0 END)::bigint AS direct_count,
                        COUNT(*)::bigint AS total_count
                    FROM ancestors
                    GROUP BY folder_path, depth
                    ORDER BY folder_path ASC;
                    """
                )
                rows = cur.fetchall() or []
                return [
                    (str(row[0]), int(row[1]), int(row[2]), int(row[3]))
                    for row in rows
                ]

    def get_media_asset_by_path(self, relative_path: str) -> MediaAssetRecord | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        ma.relative_path,
                        ma.sha256_hex,
                        ma.size_bytes,
                        ma.origin_kind,
                        ma.last_observed_origin_kind,
                        ma.provenance_job_name,
                        ma.provenance_original_filename,
                        ma.first_cataloged_at_utc,
                        ma.last_cataloged_at_utc,
                        ma.is_favorite,
                        ma.is_archived,
                        COALESCE(me.extraction_status, 'pending') AS extraction_status,
                        me.last_attempted_at_utc,
                        me.last_succeeded_at_utc,
                        me.last_failed_at_utc,
                        me.failure_detail,
                        COALESCE(mp.preview_status, 'pending') AS preview_status,
                        mp.preview_relative_path,
                        mp.last_attempted_at_utc,
                        mp.last_succeeded_at_utc,
                        mp.last_failed_at_utc,
                        mp.failure_detail,
                        me.capture_timestamp_utc,
                        me.camera_make,
                        me.camera_model,
                        me.image_width,
                        me.image_height,
                        me.orientation,
                        me.lens_model,
                        me.exposure_time_s,
                        me.f_number,
                        me.iso_speed,
                        me.focal_length_mm,
                        me.focal_length_35mm_mm
                    FROM api_media_assets ma
                    LEFT JOIN api_media_asset_extractions me
                        ON me.relative_path = ma.relative_path
                    LEFT JOIN api_media_asset_previews mp
                        ON mp.relative_path = ma.relative_path
                    WHERE ma.relative_path = %s
                    LIMIT 1;
                    """,
                    (relative_path,),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                return MediaAssetRecord(
                    relative_path=str(row[0]),
                    sha256_hex=str(row[1]),
                    size_bytes=int(row[2]),
                    origin_kind=str(row[3]),
                    last_observed_origin_kind=str(row[4]),
                    provenance_job_name=str(row[5]) if row[5] is not None else None,
                    provenance_original_filename=str(row[6]) if row[6] is not None else None,
                    first_cataloged_at_utc=str(row[7]),
                    last_cataloged_at_utc=str(row[8]),
                    is_favorite=bool(row[9]),
                    is_archived=bool(row[10]),
                    extraction_status=str(row[11]),
                    extraction_last_attempted_at_utc=str(row[12]) if row[12] is not None else None,
                    extraction_last_succeeded_at_utc=str(row[13]) if row[13] is not None else None,
                    extraction_last_failed_at_utc=str(row[14]) if row[14] is not None else None,
                    extraction_failure_detail=str(row[15]) if row[15] is not None else None,
                    preview_status=str(row[16]),
                    preview_relative_path=str(row[17]) if row[17] is not None else None,
                    preview_last_attempted_at_utc=str(row[18]) if row[18] is not None else None,
                    preview_last_succeeded_at_utc=str(row[19]) if row[19] is not None else None,
                    preview_last_failed_at_utc=str(row[20]) if row[20] is not None else None,
                    preview_failure_detail=str(row[21]) if row[21] is not None else None,
                    capture_timestamp_utc=str(row[22]) if row[22] is not None else None,
                    camera_make=str(row[23]) if row[23] is not None else None,
                    camera_model=str(row[24]) if row[24] is not None else None,
                    image_width=int(row[25]) if row[25] is not None else None,
                    image_height=int(row[26]) if row[26] is not None else None,
                    orientation=int(row[27]) if row[27] is not None else None,
                    lens_model=str(row[28]) if row[28] is not None else None,
                    exposure_time_s=float(row[29]) if row[29] is not None else None,
                    f_number=float(row[30]) if row[30] is not None else None,
                    iso_speed=int(row[31]) if row[31] is not None else None,
                    focal_length_mm=float(row[32]) if row[32] is not None else None,
                    focal_length_35mm_mm=int(row[33]) if row[33] is not None else None,
                )

    def set_media_asset_favorite(
        self, *, relative_path: str, is_favorite: bool, updated_at_utc: str
    ) -> MediaAssetRecord | None:
        del updated_at_utc
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE api_media_assets
                    SET is_favorite = %s
                    WHERE relative_path = %s;
                    """,
                    (is_favorite, relative_path),
                )
                if cur.rowcount <= 0:
                    conn.commit()
                    return None
            conn.commit()
        return self.get_media_asset_by_path(relative_path)

    def set_media_asset_archived(
        self, *, relative_path: str, is_archived: bool, updated_at_utc: str
    ) -> MediaAssetRecord | None:
        del updated_at_utc
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE api_media_assets
                    SET is_archived = %s
                    WHERE relative_path = %s;
                    """,
                    (is_archived, relative_path),
                )
                if cur.rowcount <= 0:
                    conn.commit()
                    return None
            conn.commit()
        return self.get_media_asset_by_path(relative_path)

    def _row_to_media_asset_record(self, row: tuple[object, ...]) -> MediaAssetRecord:
        return MediaAssetRecord(
            relative_path=str(row[0]),
            sha256_hex=str(row[1]),
            size_bytes=int(row[2]),
            origin_kind=str(row[3]),
            last_observed_origin_kind=str(row[4]),
            provenance_job_name=str(row[5]) if row[5] is not None else None,
            provenance_original_filename=str(row[6]) if row[6] is not None else None,
            first_cataloged_at_utc=str(row[7]),
            last_cataloged_at_utc=str(row[8]),
            is_favorite=bool(row[9]),
            is_archived=bool(row[10]),
            extraction_status=str(row[11]),
            extraction_last_attempted_at_utc=str(row[12]) if row[12] is not None else None,
            extraction_last_succeeded_at_utc=str(row[13]) if row[13] is not None else None,
            extraction_last_failed_at_utc=str(row[14]) if row[14] is not None else None,
            extraction_failure_detail=str(row[15]) if row[15] is not None else None,
            preview_status=str(row[16]),
            preview_relative_path=str(row[17]) if row[17] is not None else None,
            preview_last_attempted_at_utc=str(row[18]) if row[18] is not None else None,
            preview_last_succeeded_at_utc=str(row[19]) if row[19] is not None else None,
            preview_last_failed_at_utc=str(row[20]) if row[20] is not None else None,
            preview_failure_detail=str(row[21]) if row[21] is not None else None,
            capture_timestamp_utc=str(row[22]) if row[22] is not None else None,
            camera_make=str(row[23]) if row[23] is not None else None,
            camera_model=str(row[24]) if row[24] is not None else None,
            image_width=int(row[25]) if row[25] is not None else None,
            image_height=int(row[26]) if row[26] is not None else None,
            orientation=int(row[27]) if row[27] is not None else None,
            lens_model=str(row[28]) if row[28] is not None else None,
            exposure_time_s=float(row[29]) if len(row) > 29 and row[29] is not None else None,
            f_number=float(row[30]) if len(row) > 30 and row[30] is not None else None,
            iso_speed=int(row[31]) if len(row) > 31 and row[31] is not None else None,
            focal_length_mm=float(row[32]) if len(row) > 32 and row[32] is not None else None,
            focal_length_35mm_mm=int(row[33]) if len(row) > 33 and row[33] is not None else None,
        )

    def _list_media_assets_for_backfill(
        self,
        *,
        status_column_sql: str,
        statuses: list[str],
        limit: int,
        origin_kind: str | None,
        media_type: str | None,
        preview_capability: str | None,
        cataloged_since_utc: str | None,
        cataloged_before_utc: str | None,
    ) -> list[MediaAssetRecord]:
        if limit <= 0 or not statuses:
            return []

        where_clauses = [f"{status_column_sql} = ANY(%s)"]
        params: list[object] = [statuses]
        if origin_kind is not None:
            where_clauses.append("ma.origin_kind = %s")
            params.append(origin_kind)
        if media_type is not None:
            media_type_suffixes = _MEDIA_TYPE_SUFFIXES.get(media_type)
            if media_type_suffixes is not None:
                like_clauses = ["LOWER(ma.relative_path) LIKE %s" for _ in media_type_suffixes]
                where_clauses.append("(" + " OR ".join(like_clauses) + ")")
                params.extend([f"%{suffix}" for suffix in media_type_suffixes])
            else:
                all_suffixes = [suffix for values in _MEDIA_TYPE_SUFFIXES.values() for suffix in values]
                not_like_clauses = ["LOWER(ma.relative_path) NOT LIKE %s" for _ in all_suffixes]
                where_clauses.append("(" + " AND ".join(not_like_clauses) + ")")
                params.extend([f"%{suffix}" for suffix in all_suffixes])
        if preview_capability is not None:
            previewable_suffixes = tuple(sorted(_PREVIEWABLE_SUFFIXES))
            if preview_capability == "previewable":
                like_clauses = ["LOWER(ma.relative_path) LIKE %s" for _ in previewable_suffixes]
                where_clauses.append("(" + " OR ".join(like_clauses) + ")")
                params.extend([f"%{suffix}" for suffix in previewable_suffixes])
            else:
                not_like_clauses = ["LOWER(ma.relative_path) NOT LIKE %s" for _ in previewable_suffixes]
                where_clauses.append("(" + " AND ".join(not_like_clauses) + ")")
                params.extend([f"%{suffix}" for suffix in previewable_suffixes])
        if cataloged_since_utc is not None:
            where_clauses.append("ma.last_cataloged_at_utc >= %s")
            params.append(cataloged_since_utc)
        if cataloged_before_utc is not None:
            where_clauses.append("ma.last_cataloged_at_utc <= %s")
            params.append(cataloged_before_utc)
        where_sql = "WHERE " + " AND ".join(where_clauses)

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        ma.relative_path,
                        ma.sha256_hex,
                        ma.size_bytes,
                        ma.origin_kind,
                        ma.last_observed_origin_kind,
                        ma.provenance_job_name,
                        ma.provenance_original_filename,
                        ma.first_cataloged_at_utc,
                        ma.last_cataloged_at_utc,
                        ma.is_favorite,
                        ma.is_archived,
                        COALESCE(me.extraction_status, 'pending') AS extraction_status,
                        me.last_attempted_at_utc,
                        me.last_succeeded_at_utc,
                        me.last_failed_at_utc,
                        me.failure_detail,
                        COALESCE(mp.preview_status, 'pending') AS preview_status,
                        mp.preview_relative_path,
                        mp.last_attempted_at_utc,
                        mp.last_succeeded_at_utc,
                        mp.last_failed_at_utc,
                        mp.failure_detail,
                        me.capture_timestamp_utc,
                        me.camera_make,
                        me.camera_model,
                        me.image_width,
                        me.image_height,
                        me.orientation,
                        me.lens_model,
                        me.exposure_time_s,
                        me.f_number,
                        me.iso_speed,
                        me.focal_length_mm,
                        me.focal_length_35mm_mm
                    FROM api_media_assets ma
                    LEFT JOIN api_media_asset_extractions me
                        ON me.relative_path = ma.relative_path
                    LEFT JOIN api_media_asset_previews mp
                        ON mp.relative_path = ma.relative_path
                    {where_sql}
                    ORDER BY ma.last_cataloged_at_utc DESC, ma.relative_path ASC
                    LIMIT %s;
                    """,
                    tuple([*params, limit]),
                )
                rows = cur.fetchall()
                return [self._row_to_media_asset_record(tuple(row)) for row in rows]

    def list_media_assets_for_extraction(
        self,
        *,
        extraction_statuses: list[str],
        limit: int,
        origin_kind: str | None = None,
        media_type: str | None = None,
        preview_capability: str | None = None,
        cataloged_since_utc: str | None = None,
        cataloged_before_utc: str | None = None,
    ) -> list[MediaAssetRecord]:
        return self._list_media_assets_for_backfill(
            status_column_sql="COALESCE(me.extraction_status, 'pending')",
            statuses=extraction_statuses,
            limit=limit,
            origin_kind=origin_kind,
            media_type=media_type,
            preview_capability=preview_capability,
            cataloged_since_utc=cataloged_since_utc,
            cataloged_before_utc=cataloged_before_utc,
        )

    def list_media_assets_for_preview(
        self,
        *,
        preview_statuses: list[str],
        limit: int,
        origin_kind: str | None = None,
        media_type: str | None = None,
        preview_capability: str | None = None,
        cataloged_since_utc: str | None = None,
        cataloged_before_utc: str | None = None,
    ) -> list[MediaAssetRecord]:
        return self._list_media_assets_for_backfill(
            status_column_sql="COALESCE(mp.preview_status, 'pending')",
            statuses=preview_statuses,
            limit=limit,
            origin_kind=origin_kind,
            media_type=media_type,
            preview_capability=preview_capability,
            cataloged_since_utc=cataloged_since_utc,
            cataloged_before_utc=cataloged_before_utc,
        )

    def ensure_media_asset_extraction_row(self, *, relative_path: str, recorded_at_utc: str) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO api_media_asset_extractions (
                        relative_path,
                        extraction_status,
                        updated_at_utc
                    )
                    VALUES (%s, 'pending', %s)
                    ON CONFLICT (relative_path) DO NOTHING;
                    """,
                    (relative_path, recorded_at_utc),
                )
            conn.commit()

    def upsert_media_asset_extraction(
        self,
        *,
        relative_path: str,
        extraction_status: str,
        attempted_at_utc: str | None,
        succeeded_at_utc: str | None,
        failed_at_utc: str | None,
        failure_detail: str | None,
        capture_timestamp_utc: str | None,
        camera_make: str | None,
        camera_model: str | None,
        image_width: int | None,
        image_height: int | None,
        orientation: int | None,
        lens_model: str | None,
        exposure_time_s: float | None = None,
        f_number: float | None = None,
        iso_speed: int | None = None,
        focal_length_mm: float | None = None,
        focal_length_35mm_mm: int | None = None,
        recorded_at_utc: str,
    ) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO api_media_asset_extractions (
                        relative_path,
                        extraction_status,
                        last_attempted_at_utc,
                        last_succeeded_at_utc,
                        last_failed_at_utc,
                        failure_detail,
                        capture_timestamp_utc,
                        camera_make,
                        camera_model,
                        image_width,
                        image_height,
                        orientation,
                        lens_model,
                        exposure_time_s,
                        f_number,
                        iso_speed,
                        focal_length_mm,
                        focal_length_35mm_mm,
                        updated_at_utc
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s
                    )
                    ON CONFLICT (relative_path) DO UPDATE
                    SET extraction_status = EXCLUDED.extraction_status,
                        last_attempted_at_utc = EXCLUDED.last_attempted_at_utc,
                        last_succeeded_at_utc = EXCLUDED.last_succeeded_at_utc,
                        last_failed_at_utc = EXCLUDED.last_failed_at_utc,
                        failure_detail = EXCLUDED.failure_detail,
                        capture_timestamp_utc = EXCLUDED.capture_timestamp_utc,
                        camera_make = EXCLUDED.camera_make,
                        camera_model = EXCLUDED.camera_model,
                        image_width = EXCLUDED.image_width,
                        image_height = EXCLUDED.image_height,
                        orientation = EXCLUDED.orientation,
                        lens_model = EXCLUDED.lens_model,
                        exposure_time_s = EXCLUDED.exposure_time_s,
                        f_number = EXCLUDED.f_number,
                        iso_speed = EXCLUDED.iso_speed,
                        focal_length_mm = EXCLUDED.focal_length_mm,
                        focal_length_35mm_mm = EXCLUDED.focal_length_35mm_mm,
                        updated_at_utc = EXCLUDED.updated_at_utc;
                    """,
                    (
                        relative_path,
                        extraction_status,
                        attempted_at_utc,
                        succeeded_at_utc,
                        failed_at_utc,
                        failure_detail,
                        capture_timestamp_utc,
                        camera_make,
                        camera_model,
                        image_width,
                        image_height,
                        orientation,
                        lens_model,
                        exposure_time_s,
                        f_number,
                        iso_speed,
                        focal_length_mm,
                        focal_length_35mm_mm,
                        recorded_at_utc,
                    ),
                )
            conn.commit()

    def ensure_media_asset_preview_row(self, *, relative_path: str, recorded_at_utc: str) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO api_media_asset_previews (
                        relative_path,
                        preview_status,
                        updated_at_utc
                    )
                    VALUES (%s, 'pending', %s)
                    ON CONFLICT (relative_path) DO NOTHING;
                    """,
                    (relative_path, recorded_at_utc),
                )
            conn.commit()

    def upsert_media_asset_preview(
        self,
        *,
        relative_path: str,
        preview_status: str,
        preview_relative_path: str | None,
        attempted_at_utc: str | None,
        succeeded_at_utc: str | None,
        failed_at_utc: str | None,
        failure_detail: str | None,
        recorded_at_utc: str,
    ) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO api_media_asset_previews (
                        relative_path,
                        preview_status,
                        preview_relative_path,
                        last_attempted_at_utc,
                        last_succeeded_at_utc,
                        last_failed_at_utc,
                        failure_detail,
                        updated_at_utc
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (relative_path) DO UPDATE
                    SET preview_status = EXCLUDED.preview_status,
                        preview_relative_path = EXCLUDED.preview_relative_path,
                        last_attempted_at_utc = EXCLUDED.last_attempted_at_utc,
                        last_succeeded_at_utc = EXCLUDED.last_succeeded_at_utc,
                        last_failed_at_utc = EXCLUDED.last_failed_at_utc,
                        failure_detail = EXCLUDED.failure_detail,
                        updated_at_utc = EXCLUDED.updated_at_utc;
                    """,
                    (
                        relative_path,
                        preview_status,
                        preview_relative_path,
                        attempted_at_utc,
                        succeeded_at_utc,
                        failed_at_utc,
                        failure_detail,
                        recorded_at_utc,
                    ),
                )
            conn.commit()

    def list_duplicate_sha_groups(
        self, *, limit: int, offset: int
    ) -> tuple[int, list[DuplicateShaGroup]]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH duplicate_groups AS (
                        SELECT
                            sha256_hex,
                            COUNT(*) AS file_count,
                            MIN(first_seen_at_utc) AS first_seen_at_utc,
                            MAX(last_seen_at_utc) AS last_seen_at_utc,
                            ARRAY_AGG(relative_path ORDER BY relative_path ASC) AS relative_paths
                        FROM api_stored_files
                        GROUP BY sha256_hex
                        HAVING COUNT(*) > 1
                    )
                    SELECT COUNT(*) FROM duplicate_groups;
                    """
                )
                count_row = cur.fetchone()
                total = int(count_row[0]) if count_row is not None else 0
                cur.execute(
                    """
                    SELECT
                        sha256_hex,
                        file_count,
                        first_seen_at_utc,
                        last_seen_at_utc,
                        relative_paths
                    FROM (
                        SELECT
                            sha256_hex,
                            COUNT(*) AS file_count,
                            MIN(first_seen_at_utc) AS first_seen_at_utc,
                            MAX(last_seen_at_utc) AS last_seen_at_utc,
                            ARRAY_AGG(relative_path ORDER BY relative_path ASC) AS relative_paths
                        FROM api_stored_files
                        GROUP BY sha256_hex
                        HAVING COUNT(*) > 1
                    ) duplicate_groups
                    ORDER BY file_count DESC, last_seen_at_utc DESC, sha256_hex ASC
                    LIMIT %s
                    OFFSET %s;
                    """,
                    (limit, offset),
                )
                rows = cur.fetchall()
                groups = [
                    DuplicateShaGroup(
                        sha256_hex=str(row[0]),
                        file_count=int(row[1]),
                        first_seen_at_utc=str(row[2]),
                        last_seen_at_utc=str(row[3]),
                        relative_paths=tuple(str(path) for path in row[4]),
                    )
                    for row in rows
                ]
                return total, groups

    def record_path_conflict(
        self,
        *,
        relative_path: str,
        previous_sha256_hex: str,
        current_sha256_hex: str,
        detected_at_utc: str,
    ) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO api_storage_path_conflicts (
                        relative_path, previous_sha256_hex, current_sha256_hex, detected_at_utc
                    )
                    VALUES (%s, %s, %s, %s);
                    """,
                    (relative_path, previous_sha256_hex, current_sha256_hex, detected_at_utc),
                )
            conn.commit()

    def list_path_conflicts(self, *, limit: int, offset: int) -> tuple[int, list[PathConflictRecord]]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM api_storage_path_conflicts;")
                count_row = cur.fetchone()
                total = int(count_row[0]) if count_row is not None else 0
                cur.execute(
                    """
                    SELECT relative_path, previous_sha256_hex, current_sha256_hex, detected_at_utc
                    FROM api_storage_path_conflicts
                    ORDER BY detected_at_utc DESC, relative_path ASC
                    LIMIT %s
                    OFFSET %s;
                    """,
                    (limit, offset),
                )
                rows = cur.fetchall()
                records = [
                    PathConflictRecord(
                        relative_path=str(row[0]),
                        previous_sha256_hex=str(row[1]),
                        current_sha256_hex=str(row[2]),
                        detected_at_utc=str(row[3]),
                    )
                    for row in rows
                ]
                return total, records

    def record_storage_index_run(self, record: StorageIndexRunRecord) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO api_storage_index_runs (
                        singleton_key, scanned_files, indexed_files, new_sha_entries,
                        existing_sha_matches, path_conflicts, errors, completed_at_utc
                    )
                    VALUES (TRUE, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (singleton_key) DO UPDATE
                    SET scanned_files = EXCLUDED.scanned_files,
                        indexed_files = EXCLUDED.indexed_files,
                        new_sha_entries = EXCLUDED.new_sha_entries,
                        existing_sha_matches = EXCLUDED.existing_sha_matches,
                        path_conflicts = EXCLUDED.path_conflicts,
                        errors = EXCLUDED.errors,
                        completed_at_utc = EXCLUDED.completed_at_utc;
                    """,
                    (
                        record.scanned_files,
                        record.indexed_files,
                        record.new_sha_entries,
                        record.existing_sha_matches,
                        record.path_conflicts,
                        record.errors,
                        record.completed_at_utc,
                    ),
                )
            conn.commit()

    def get_latest_storage_index_run(self) -> StorageIndexRunRecord | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        scanned_files,
                        indexed_files,
                        new_sha_entries,
                        existing_sha_matches,
                        path_conflicts,
                        errors,
                        completed_at_utc
                    FROM api_storage_index_runs
                    WHERE singleton_key = TRUE
                    LIMIT 1;
                    """
                )
                row = cur.fetchone()
                if row is None:
                    return None
                return StorageIndexRunRecord(
                    scanned_files=int(row[0]),
                    indexed_files=int(row[1]),
                    new_sha_entries=int(row[2]),
                    existing_sha_matches=int(row[3]),
                    path_conflicts=int(row[4]),
                    errors=int(row[5]),
                    completed_at_utc=str(row[6]),
                )

    def record_catalog_backfill_run(self, record: CatalogBackfillRunRecord) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO api_catalog_backfill_runs (
                        backfill_kind,
                        requested_statuses,
                        selected_count,
                        processed_count,
                        succeeded_count,
                        failed_count,
                        remaining_pending_count,
                        remaining_failed_count,
                        filter_origin_kind,
                        filter_media_type,
                        filter_preview_capability,
                        filter_cataloged_since_utc,
                        filter_cataloged_before_utc,
                        limit_count,
                        completed_at_utc
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (backfill_kind) DO UPDATE
                    SET requested_statuses = EXCLUDED.requested_statuses,
                        selected_count = EXCLUDED.selected_count,
                        processed_count = EXCLUDED.processed_count,
                        succeeded_count = EXCLUDED.succeeded_count,
                        failed_count = EXCLUDED.failed_count,
                        remaining_pending_count = EXCLUDED.remaining_pending_count,
                        remaining_failed_count = EXCLUDED.remaining_failed_count,
                        filter_origin_kind = EXCLUDED.filter_origin_kind,
                        filter_media_type = EXCLUDED.filter_media_type,
                        filter_preview_capability = EXCLUDED.filter_preview_capability,
                        filter_cataloged_since_utc = EXCLUDED.filter_cataloged_since_utc,
                        filter_cataloged_before_utc = EXCLUDED.filter_cataloged_before_utc,
                        limit_count = EXCLUDED.limit_count,
                        completed_at_utc = EXCLUDED.completed_at_utc;
                    """,
                    (
                        record.backfill_kind,
                        list(record.requested_statuses),
                        record.selected_count,
                        record.processed_count,
                        record.succeeded_count,
                        record.failed_count,
                        record.remaining_pending_count,
                        record.remaining_failed_count,
                        record.filter_origin_kind,
                        record.filter_media_type,
                        record.filter_preview_capability,
                        record.filter_cataloged_since_utc,
                        record.filter_cataloged_before_utc,
                        record.limit_count,
                        record.completed_at_utc,
                    ),
                )
            conn.commit()

    def get_latest_catalog_backfill_run(self, backfill_kind: str) -> CatalogBackfillRunRecord | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        backfill_kind,
                        requested_statuses,
                        selected_count,
                        processed_count,
                        succeeded_count,
                        failed_count,
                        remaining_pending_count,
                        remaining_failed_count,
                        filter_origin_kind,
                        filter_media_type,
                        filter_preview_capability,
                        filter_cataloged_since_utc,
                        filter_cataloged_before_utc,
                        limit_count,
                        completed_at_utc
                    FROM api_catalog_backfill_runs
                    WHERE backfill_kind = %s
                    LIMIT 1;
                    """,
                    (backfill_kind,),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                raw_statuses = row[1]
                statuses: tuple[str, ...]
                if isinstance(raw_statuses, (list, tuple)):
                    statuses = tuple(str(status) for status in raw_statuses)
                else:
                    statuses = tuple()
                return CatalogBackfillRunRecord(
                    backfill_kind=str(row[0]),
                    requested_statuses=statuses,
                    selected_count=int(row[2]),
                    processed_count=int(row[3]),
                    succeeded_count=int(row[4]),
                    failed_count=int(row[5]),
                    remaining_pending_count=int(row[6]),
                    remaining_failed_count=int(row[7]),
                    filter_origin_kind=str(row[8]) if row[8] is not None else None,
                    filter_media_type=str(row[9]) if row[9] is not None else None,
                    filter_preview_capability=str(row[10]) if row[10] is not None else None,
                    filter_cataloged_since_utc=str(row[11]) if row[11] is not None else None,
                    filter_cataloged_before_utc=str(row[12]) if row[12] is not None else None,
                    limit_count=int(row[13]),
                    completed_at_utc=str(row[14]),
                )

    def summarize_storage(self) -> StorageSummary:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM api_known_sha256;")
                known_row = cur.fetchone()
                total_known_sha256 = int(known_row[0]) if known_row is not None else 0

                cur.execute(
                    """
                    SELECT
                        COUNT(*) AS total_stored_files,
                        COUNT(*) FILTER (WHERE source_kind = 'index_scan') AS indexed_files,
                        COUNT(*) FILTER (WHERE source_kind = 'upload_verify') AS uploaded_files,
                        COUNT(*) - COUNT(DISTINCT sha256_hex) AS duplicate_file_paths
                    FROM api_stored_files;
                    """
                )
                aggregate_row = cur.fetchone()
                total_stored_files = int(aggregate_row[0]) if aggregate_row is not None else 0
                indexed_files = int(aggregate_row[1]) if aggregate_row is not None else 0
                uploaded_files = int(aggregate_row[2]) if aggregate_row is not None else 0
                duplicate_file_paths = int(aggregate_row[3]) if aggregate_row is not None else 0

                cur.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (
                            WHERE source_kind = 'index_scan'
                            AND last_seen_at_utc >= %s
                        ) AS recent_indexed_files_24h,
                        COUNT(*) FILTER (
                            WHERE source_kind = 'upload_verify'
                            AND last_seen_at_utc >= %s
                        ) AS recent_uploaded_files_24h,
                        MAX(last_seen_at_utc) FILTER (
                            WHERE source_kind = 'index_scan'
                        ) AS last_indexed_at_utc,
                        MAX(last_seen_at_utc) FILTER (
                            WHERE source_kind = 'upload_verify'
                        ) AS last_uploaded_at_utc
                    FROM api_stored_files;
                    """,
                    (
                        (datetime.now(UTC) - timedelta(hours=24)).isoformat(),
                        (datetime.now(UTC) - timedelta(hours=24)).isoformat(),
                    ),
                )
                recent_row = cur.fetchone()
                recent_indexed_files_24h = (
                    int(recent_row[0]) if recent_row and recent_row[0] is not None else 0
                )
                recent_uploaded_files_24h = (
                    int(recent_row[1]) if recent_row and recent_row[1] is not None else 0
                )
                last_indexed_at_utc = (
                    str(recent_row[2]) if recent_row and recent_row[2] is not None else None
                )
                last_uploaded_at_utc = (
                    str(recent_row[3]) if recent_row and recent_row[3] is not None else None
                )

                return StorageSummary(
                    total_known_sha256=total_known_sha256,
                    total_stored_files=total_stored_files,
                    indexed_files=indexed_files,
                    uploaded_files=uploaded_files,
                    duplicate_file_paths=duplicate_file_paths,
                    recent_indexed_files_24h=recent_indexed_files_24h,
                    recent_uploaded_files_24h=recent_uploaded_files_24h,
                    last_indexed_at_utc=last_indexed_at_utc,
                    last_uploaded_at_utc=last_uploaded_at_utc,
                )

    def upsert_client_pending(
        self,
        *,
        client_id: str,
        display_name: str,
        enrolled_at_utc: str,
    ) -> ClientRecord:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO api_clients (
                        client_id,
                        display_name,
                        enrollment_status,
                        first_seen_at_utc,
                        last_enrolled_at_utc,
                        approved_at_utc,
                        revoked_at_utc,
                        auth_token
                    )
                    VALUES (%s, %s, 'pending', %s, %s, NULL, NULL, NULL)
                    ON CONFLICT (client_id) DO UPDATE
                    SET display_name = EXCLUDED.display_name,
                        last_enrolled_at_utc = EXCLUDED.last_enrolled_at_utc,
                        enrollment_status = CASE
                            WHEN api_clients.enrollment_status = 'pending' THEN 'pending'
                            ELSE api_clients.enrollment_status
                        END,
                        approved_at_utc = CASE
                            WHEN api_clients.enrollment_status = 'pending' THEN NULL
                            ELSE api_clients.approved_at_utc
                        END,
                        revoked_at_utc = CASE
                            WHEN api_clients.enrollment_status = 'pending' THEN NULL
                            ELSE api_clients.revoked_at_utc
                        END,
                        auth_token = CASE
                            WHEN api_clients.enrollment_status = 'pending' THEN NULL
                            ELSE api_clients.auth_token
                        END
                    RETURNING
                        client_id,
                        display_name,
                        enrollment_status,
                        first_seen_at_utc,
                        last_enrolled_at_utc,
                        approved_at_utc,
                        revoked_at_utc,
                        auth_token;
                    """,
                    (client_id, display_name, enrolled_at_utc, enrolled_at_utc),
                )
                row = cur.fetchone()
            conn.commit()

        if row is None:
            raise RuntimeError("upsert_client_pending must return a row")
        return ClientRecord(
            client_id=str(row[0]),
            display_name=str(row[1]),
            enrollment_status=str(row[2]),
            first_seen_at_utc=str(row[3]),
            last_enrolled_at_utc=str(row[4]),
            approved_at_utc=str(row[5]) if row[5] is not None else None,
            revoked_at_utc=str(row[6]) if row[6] is not None else None,
            auth_token=str(row[7]) if row[7] is not None else None,
        )

    def get_client(self, client_id: str) -> ClientRecord | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        client_id,
                        display_name,
                        enrollment_status,
                        first_seen_at_utc,
                        last_enrolled_at_utc,
                        approved_at_utc,
                        revoked_at_utc,
                        auth_token
                    FROM api_clients
                    WHERE client_id = %s
                    LIMIT 1;
                    """,
                    (client_id,),
                )
                row = cur.fetchone()
        if row is None:
            return None
        return ClientRecord(
            client_id=str(row[0]),
            display_name=str(row[1]),
            enrollment_status=str(row[2]),
            first_seen_at_utc=str(row[3]),
            last_enrolled_at_utc=str(row[4]),
            approved_at_utc=str(row[5]) if row[5] is not None else None,
            revoked_at_utc=str(row[6]) if row[6] is not None else None,
            auth_token=str(row[7]) if row[7] is not None else None,
        )

    def list_clients(self, *, limit: int, offset: int) -> tuple[int, list[ClientRecord]]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM api_clients;")
                count_row = cur.fetchone()
                total = int(count_row[0]) if count_row is not None else 0
                cur.execute(
                    """
                    SELECT
                        client_id,
                        display_name,
                        enrollment_status,
                        first_seen_at_utc,
                        last_enrolled_at_utc,
                        approved_at_utc,
                        revoked_at_utc,
                        auth_token
                    FROM api_clients
                    ORDER BY first_seen_at_utc DESC, client_id ASC
                    LIMIT %s
                    OFFSET %s;
                    """,
                    (limit, offset),
                )
                rows = cur.fetchall()
        return total, [
            ClientRecord(
                client_id=str(row[0]),
                display_name=str(row[1]),
                enrollment_status=str(row[2]),
                first_seen_at_utc=str(row[3]),
                last_enrolled_at_utc=str(row[4]),
                approved_at_utc=str(row[5]) if row[5] is not None else None,
                revoked_at_utc=str(row[6]) if row[6] is not None else None,
                auth_token=str(row[7]) if row[7] is not None else None,
            )
            for row in rows
        ]

    def approve_client(
        self,
        *,
        client_id: str,
        approved_at_utc: str,
        auth_token: str,
    ) -> ClientRecord | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE api_clients
                    SET enrollment_status = 'approved',
                        approved_at_utc = %s,
                        revoked_at_utc = NULL,
                        auth_token = %s
                    WHERE client_id = %s
                    RETURNING
                        client_id,
                        display_name,
                        enrollment_status,
                        first_seen_at_utc,
                        last_enrolled_at_utc,
                        approved_at_utc,
                        revoked_at_utc,
                        auth_token;
                    """,
                    (approved_at_utc, auth_token, client_id),
                )
                row = cur.fetchone()
            conn.commit()
        if row is None:
            return None
        return ClientRecord(
            client_id=str(row[0]),
            display_name=str(row[1]),
            enrollment_status=str(row[2]),
            first_seen_at_utc=str(row[3]),
            last_enrolled_at_utc=str(row[4]),
            approved_at_utc=str(row[5]) if row[5] is not None else None,
            revoked_at_utc=str(row[6]) if row[6] is not None else None,
            auth_token=str(row[7]) if row[7] is not None else None,
        )

    def revoke_client(
        self,
        *,
        client_id: str,
        revoked_at_utc: str,
    ) -> ClientRecord | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE api_clients
                    SET enrollment_status = 'revoked',
                        revoked_at_utc = %s
                    WHERE client_id = %s
                    RETURNING
                        client_id,
                        display_name,
                        enrollment_status,
                        first_seen_at_utc,
                        last_enrolled_at_utc,
                        approved_at_utc,
                        revoked_at_utc,
                        auth_token;
                    """,
                    (revoked_at_utc, client_id),
                )
                row = cur.fetchone()
            conn.commit()
        if row is None:
            return None
        return ClientRecord(
            client_id=str(row[0]),
            display_name=str(row[1]),
            enrollment_status=str(row[2]),
            first_seen_at_utc=str(row[3]),
            last_enrolled_at_utc=str(row[4]),
            approved_at_utc=str(row[5]) if row[5] is not None else None,
            revoked_at_utc=str(row[6]) if row[6] is not None else None,
            auth_token=str(row[7]) if row[7] is not None else None,
        )

    def upsert_client_heartbeat(
        self,
        *,
        client_id: str,
        last_seen_at_utc: str,
        daemon_state: str,
        workload_status: str,
        active_job_id: int | None,
        active_job_label: str | None,
        active_job_status: str | None,
        active_job_ready_to_upload: int | None,
        active_job_uploaded: int | None,
        active_job_retrying: int | None,
        active_job_total_files: int | None,
        active_job_non_terminal_files: int | None,
        active_job_error_files: int | None,
        active_job_blocking_reason: str | None,
        retry_pending_count: int | None,
        retry_next_at_utc: str | None,
        retry_reason: str | None,
        auth_block_reason: str | None,
        recent_error_category: str | None,
        recent_error_message: str | None,
        recent_error_at_utc: str | None,
        updated_at_utc: str,
    ) -> ClientHeartbeatRecord:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO api_client_heartbeats (
                        client_id,
                        last_seen_at_utc,
                        daemon_state,
                        workload_status,
                        active_job_id,
                        active_job_label,
                        active_job_status,
                        active_job_ready_to_upload,
                        active_job_uploaded,
                        active_job_retrying,
                        active_job_total_files,
                        active_job_non_terminal_files,
                        active_job_error_files,
                        active_job_blocking_reason,
                        retry_pending_count,
                        retry_next_at_utc,
                        retry_reason,
                        auth_block_reason,
                        recent_error_category,
                        recent_error_message,
                        recent_error_at_utc,
                        updated_at_utc
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                    ON CONFLICT (client_id) DO UPDATE
                    SET last_seen_at_utc = EXCLUDED.last_seen_at_utc,
                        daemon_state = EXCLUDED.daemon_state,
                        workload_status = EXCLUDED.workload_status,
                        active_job_id = EXCLUDED.active_job_id,
                        active_job_label = EXCLUDED.active_job_label,
                        active_job_status = EXCLUDED.active_job_status,
                        active_job_ready_to_upload = EXCLUDED.active_job_ready_to_upload,
                        active_job_uploaded = EXCLUDED.active_job_uploaded,
                        active_job_retrying = EXCLUDED.active_job_retrying,
                        active_job_total_files = EXCLUDED.active_job_total_files,
                        active_job_non_terminal_files = EXCLUDED.active_job_non_terminal_files,
                        active_job_error_files = EXCLUDED.active_job_error_files,
                        active_job_blocking_reason = EXCLUDED.active_job_blocking_reason,
                        retry_pending_count = EXCLUDED.retry_pending_count,
                        retry_next_at_utc = EXCLUDED.retry_next_at_utc,
                        retry_reason = EXCLUDED.retry_reason,
                        auth_block_reason = EXCLUDED.auth_block_reason,
                        recent_error_category = EXCLUDED.recent_error_category,
                        recent_error_message = EXCLUDED.recent_error_message,
                        recent_error_at_utc = EXCLUDED.recent_error_at_utc,
                        updated_at_utc = EXCLUDED.updated_at_utc
                    RETURNING
                        client_id,
                        last_seen_at_utc,
                        daemon_state,
                        workload_status,
                        active_job_id,
                        active_job_label,
                        active_job_status,
                        active_job_ready_to_upload,
                        active_job_uploaded,
                        active_job_retrying,
                        active_job_total_files,
                        active_job_non_terminal_files,
                        active_job_error_files,
                        active_job_blocking_reason,
                        retry_pending_count,
                        retry_next_at_utc,
                        retry_reason,
                        auth_block_reason,
                        recent_error_category,
                        recent_error_message,
                        recent_error_at_utc,
                        updated_at_utc;
                    """,
                    (
                        client_id,
                        last_seen_at_utc,
                        daemon_state,
                        workload_status,
                        active_job_id,
                        active_job_label,
                        active_job_status,
                        active_job_ready_to_upload,
                        active_job_uploaded,
                        active_job_retrying,
                        active_job_total_files,
                        active_job_non_terminal_files,
                        active_job_error_files,
                        active_job_blocking_reason,
                        retry_pending_count,
                        retry_next_at_utc,
                        retry_reason,
                        auth_block_reason,
                        recent_error_category,
                        recent_error_message,
                        recent_error_at_utc,
                        updated_at_utc,
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        if row is None:
            raise RuntimeError("upsert_client_heartbeat must return a row")
        return ClientHeartbeatRecord(
            client_id=str(row[0]),
            last_seen_at_utc=str(row[1]),
            daemon_state=str(row[2]),
            workload_status=str(row[3]),
            active_job_id=int(row[4]) if row[4] is not None else None,
            active_job_label=str(row[5]) if row[5] is not None else None,
            active_job_status=str(row[6]) if row[6] is not None else None,
            active_job_ready_to_upload=int(row[7]) if row[7] is not None else None,
            active_job_uploaded=int(row[8]) if row[8] is not None else None,
            active_job_retrying=int(row[9]) if row[9] is not None else None,
            active_job_total_files=int(row[10]) if row[10] is not None else None,
            active_job_non_terminal_files=int(row[11]) if row[11] is not None else None,
            active_job_error_files=int(row[12]) if row[12] is not None else None,
            active_job_blocking_reason=str(row[13]) if row[13] is not None else None,
            retry_pending_count=int(row[14]) if row[14] is not None else None,
            retry_next_at_utc=str(row[15]) if row[15] is not None else None,
            retry_reason=str(row[16]) if row[16] is not None else None,
            auth_block_reason=str(row[17]) if row[17] is not None else None,
            recent_error_category=str(row[18]) if row[18] is not None else None,
            recent_error_message=str(row[19]) if row[19] is not None else None,
            recent_error_at_utc=str(row[20]) if row[20] is not None else None,
            updated_at_utc=str(row[21]),
        )

    def get_client_heartbeat(self, client_id: str) -> ClientHeartbeatRecord | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        client_id,
                        last_seen_at_utc,
                        daemon_state,
                        workload_status,
                        active_job_id,
                        active_job_label,
                        active_job_status,
                        active_job_ready_to_upload,
                        active_job_uploaded,
                        active_job_retrying,
                        active_job_total_files,
                        active_job_non_terminal_files,
                        active_job_error_files,
                        active_job_blocking_reason,
                        retry_pending_count,
                        retry_next_at_utc,
                        retry_reason,
                        auth_block_reason,
                        recent_error_category,
                        recent_error_message,
                        recent_error_at_utc,
                        updated_at_utc
                    FROM api_client_heartbeats
                    WHERE client_id = %s
                    LIMIT 1;
                    """,
                    (client_id,),
                )
                row = cur.fetchone()
        if row is None:
            return None
        return ClientHeartbeatRecord(
            client_id=str(row[0]),
            last_seen_at_utc=str(row[1]),
            daemon_state=str(row[2]),
            workload_status=str(row[3]),
            active_job_id=int(row[4]) if row[4] is not None else None,
            active_job_label=str(row[5]) if row[5] is not None else None,
            active_job_status=str(row[6]) if row[6] is not None else None,
            active_job_ready_to_upload=int(row[7]) if row[7] is not None else None,
            active_job_uploaded=int(row[8]) if row[8] is not None else None,
            active_job_retrying=int(row[9]) if row[9] is not None else None,
            active_job_total_files=int(row[10]) if row[10] is not None else None,
            active_job_non_terminal_files=int(row[11]) if row[11] is not None else None,
            active_job_error_files=int(row[12]) if row[12] is not None else None,
            active_job_blocking_reason=str(row[13]) if row[13] is not None else None,
            retry_pending_count=int(row[14]) if row[14] is not None else None,
            retry_next_at_utc=str(row[15]) if row[15] is not None else None,
            retry_reason=str(row[16]) if row[16] is not None else None,
            auth_block_reason=str(row[17]) if row[17] is not None else None,
            recent_error_category=str(row[18]) if row[18] is not None else None,
            recent_error_message=str(row[19]) if row[19] is not None else None,
            recent_error_at_utc=str(row[20]) if row[20] is not None else None,
            updated_at_utc=str(row[21]),
        )

    def remove_temp_upload(self, sha256_hex: str) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM api_temp_uploads WHERE sha256_hex = %s;", (sha256_hex,))
            conn.commit()
