"""In-memory UploadStateStore implementation (for tests and local dev)."""

from __future__ import annotations

from dataclasses import dataclass, field
from threading import Lock

from . import in_memory_catalog, in_memory_clients, in_memory_uploads
from .records import (
    CatalogBackfillRunRecord,
    ClientHeartbeatRecord,
    ClientRecord,
    MediaAssetRecord,
    MediaExtractionRecord,
    MediaPreviewRecord,
    PathConflictRecord,
    RejectedAssetRecord,
    StorageIndexRunRecord,
    StoredFileRecord,
    TempUploadRecord,
    TombstoneRecord,
)


@dataclass
class InMemoryUploadStateStore:
    """In-memory store used for local tests and fallback development."""

    known_sha256: set[str] = field(default_factory=set)
    upload_temp: dict[str, TempUploadRecord] = field(default_factory=dict)
    stored_files: dict[str, StoredFileRecord] = field(default_factory=dict)
    media_assets: dict[str, MediaAssetRecord] = field(default_factory=dict)
    media_asset_extractions: dict[str, MediaExtractionRecord] = field(default_factory=dict)
    media_asset_previews: dict[str, MediaPreviewRecord] = field(default_factory=dict)
    media_asset_rejects: dict[str, RejectedAssetRecord] = field(default_factory=dict)
    tombstones: dict[str, TombstoneRecord] = field(default_factory=dict)
    clients: dict[str, ClientRecord] = field(default_factory=dict)
    client_heartbeats: dict[str, ClientHeartbeatRecord] = field(default_factory=dict)
    path_conflicts: list[PathConflictRecord] = field(default_factory=list)
    latest_index_run: StorageIndexRunRecord | None = None
    latest_catalog_backfill_runs: dict[str, CatalogBackfillRunRecord] = field(default_factory=dict)
    _lock: Lock = field(default_factory=Lock)

    def initialize(self) -> None:
        return

    has_sha = in_memory_uploads.has_sha
    has_shas = in_memory_uploads.has_shas
    upsert_temp_upload = in_memory_uploads.upsert_temp_upload
    get_temp_upload = in_memory_uploads.get_temp_upload
    mark_sha_verified = in_memory_uploads.mark_sha_verified
    upsert_stored_file = in_memory_uploads.upsert_stored_file
    get_stored_file_by_path = in_memory_uploads.get_stored_file_by_path
    list_stored_files = in_memory_uploads.list_stored_files
    delete_stored_file = in_memory_uploads.delete_stored_file
    remove_temp_upload = in_memory_uploads.remove_temp_upload

    upsert_media_asset = in_memory_catalog.upsert_media_asset
    list_media_assets = in_memory_catalog.list_media_assets
    list_media_asset_folders = in_memory_catalog.list_media_asset_folders
    get_media_asset_by_path = in_memory_catalog.get_media_asset_by_path
    set_media_asset_favorite = in_memory_catalog.set_media_asset_favorite
    set_media_asset_archived = in_memory_catalog.set_media_asset_archived
    add_catalog_reject = in_memory_catalog.add_catalog_reject
    remove_catalog_reject = in_memory_catalog.remove_catalog_reject
    is_catalog_reject = in_memory_catalog.is_catalog_reject
    count_catalog_rejects = in_memory_catalog.count_catalog_rejects
    list_catalog_rejects = in_memory_catalog.list_catalog_rejects
    add_tombstone = in_memory_catalog.add_tombstone
    is_sha_tombstoned = in_memory_catalog.is_sha_tombstoned
    list_sha_tombstones = in_memory_catalog.list_sha_tombstones
    get_tombstone_by_path = in_memory_catalog.get_tombstone_by_path
    remove_tombstone = in_memory_catalog.remove_tombstone
    list_tombstones = in_memory_catalog.list_tombstones
    purge_tombstones = in_memory_catalog.purge_tombstones
    delete_media_asset = in_memory_catalog.delete_media_asset
    _filter_assets_for_backfill = in_memory_catalog._filter_assets_for_backfill
    list_media_assets_for_extraction = in_memory_catalog.list_media_assets_for_extraction
    list_media_assets_for_preview = in_memory_catalog.list_media_assets_for_preview
    ensure_media_asset_extraction_row = in_memory_catalog.ensure_media_asset_extraction_row
    upsert_media_asset_extraction = in_memory_catalog.upsert_media_asset_extraction
    ensure_media_asset_preview_row = in_memory_catalog.ensure_media_asset_preview_row
    upsert_media_asset_preview = in_memory_catalog.upsert_media_asset_preview
    list_duplicate_sha_groups = in_memory_catalog.list_duplicate_sha_groups
    record_path_conflict = in_memory_catalog.record_path_conflict
    list_path_conflicts = in_memory_catalog.list_path_conflicts
    record_storage_index_run = in_memory_catalog.record_storage_index_run
    get_latest_storage_index_run = in_memory_catalog.get_latest_storage_index_run
    record_catalog_backfill_run = in_memory_catalog.record_catalog_backfill_run
    get_latest_catalog_backfill_run = in_memory_catalog.get_latest_catalog_backfill_run
    summarize_storage = in_memory_catalog.summarize_storage

    upsert_client_pending = in_memory_clients.upsert_client_pending
    get_client = in_memory_clients.get_client
    list_clients = in_memory_clients.list_clients
    approve_client = in_memory_clients.approve_client
    revoke_client = in_memory_clients.revoke_client
    upsert_client_heartbeat = in_memory_clients.upsert_client_heartbeat
    get_client_heartbeat = in_memory_clients.get_client_heartbeat
