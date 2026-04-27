"""UploadStateStore Protocol definition."""

from __future__ import annotations

from typing import Protocol

from .records import (
    CatalogBackfillRunRecord,
    ClientHeartbeatRecord,
    ClientRecord,
    DuplicateShaGroup,
    MediaAssetRecord,
    PathConflictRecord,
    RejectedAssetRecord,
    StorageIndexRunRecord,
    StorageSummary,
    StoredFileRecord,
    TempUploadRecord,
    TombstoneRecord,
)


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

    def delete_stored_file(self, relative_path: str) -> bool:
        """Remove the stored-file row.

        Backends with FK wiring should cascade this removal to dependent
        catalog/extraction/preview/reject rows.
        """
        ...

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

    # -------- Phase 3.B: reject queue -----------------------------------
    def add_catalog_reject(
        self,
        *,
        relative_path: str,
        marked_at_utc: str,
        marked_reason: str | None = None,
    ) -> RejectedAssetRecord | None:
        """Idempotent upsert; returns ``None`` when ``relative_path`` is not a
        catalog asset. SHA is read from ``api_media_assets`` at insert time.
        """
        ...

    def remove_catalog_reject(self, relative_path: str) -> bool: ...

    def is_catalog_reject(self, relative_path: str) -> bool: ...

    def count_catalog_rejects(self) -> int: ...

    def list_catalog_rejects(
        self, *, limit: int, offset: int
    ) -> tuple[int, list[RejectedAssetRecord]]: ...

    # -------- Phase 3.C: tombstones -----------------------------------
    def add_tombstone(
        self,
        *,
        relative_path: str,
        sha256_hex: str,
        trashed_at_utc: str,
        marked_reason: str | None,
        trash_relative_path: str,
        original_size_bytes: int,
    ) -> TombstoneRecord: ...

    def is_sha_tombstoned(self, sha256_hex: str) -> bool: ...

    def list_sha_tombstones(self, shas: list[str]) -> list[TombstoneRecord]: ...

    def get_tombstone_by_path(self, relative_path: str) -> TombstoneRecord | None: ...

    def remove_tombstone(self, relative_path: str) -> bool: ...

    # -------- Phase 3.D: tombstone list + purge ----------------------------
    def list_tombstones(
        self,
        *,
        limit: int,
        offset: int,
        older_than_days: int | None = None,
    ) -> tuple[int, list[TombstoneRecord]]:
        """Return (total, page) of tombstones sorted by trashed_at_utc ASC.

        When ``older_than_days`` is given, only tombstones whose
        ``trashed_at_utc`` is older than that many days are included.
        This is used by the UI to highlight assets that are about to be
        purged and by the purge worker to identify candidates.
        """
        ...

    def purge_tombstones(
        self,
        *,
        older_than_days: int,
        max_batch: int,
    ) -> list[TombstoneRecord]:
        """Select up to ``max_batch`` tombstones older than ``older_than_days``
        days, delete them from the store, and return the deleted records.

        The Postgres implementation uses ``SELECT … FOR UPDATE SKIP LOCKED``
        so that two concurrent cron invocations cannot double-purge the same
        row.  The caller is responsible for removing the physical files.
        """
        ...

    def delete_media_asset(self, relative_path: str) -> bool:
        """Remove an asset and its dependents (extraction, preview, reject-queue row).

        Returns True if a row was deleted, False if the path was not found.
        Added in Phase 3.C to support the execute-delete path.
        """
        ...

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
