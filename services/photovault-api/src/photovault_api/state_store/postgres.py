"""PostgreSQL-backed UploadStateStore implementation."""

from __future__ import annotations

from dataclasses import dataclass

from . import postgres_catalog, postgres_clients, postgres_uploads


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
                # Phase 3.B: reject queue. SHA is duplicated from
                # api_media_assets so the execute phase still has it after the
                # source asset row is deleted. ON DELETE CASCADE on the FK
                # keeps the queue consistent if an asset is otherwise removed
                # through normal CRUD paths; Phase 3.C's delete path deletes
                # the queue row before it removes the media asset row so the
                # cascade is a belt-and-suspenders guard only.
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS api_catalog_reject_queue (
                        relative_path TEXT PRIMARY KEY REFERENCES api_media_assets(relative_path)
                            ON DELETE CASCADE,
                        sha256_hex TEXT NOT NULL,
                        marked_at_utc TEXT NOT NULL,
                        marked_reason TEXT
                    );
                    """
                )
                # Phase 3.C: tombstones. Records deleted SHAs to prevent re-upload.
                # Stays indefinitely unless explicitly cleared by an admin.
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS api_catalog_tombstones (
                        relative_path TEXT PRIMARY KEY,
                        sha256_hex TEXT NOT NULL,
                        trashed_at_utc TEXT NOT NULL,
                        marked_reason TEXT,
                        trash_relative_path TEXT NOT NULL,
                        original_size_bytes BIGINT
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_catalog_tombstones_sha
                    ON api_catalog_tombstones(sha256_hex);
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

    has_sha = postgres_uploads.has_sha
    has_shas = postgres_uploads.has_shas
    upsert_temp_upload = postgres_uploads.upsert_temp_upload
    get_temp_upload = postgres_uploads.get_temp_upload
    mark_sha_verified = postgres_uploads.mark_sha_verified
    upsert_stored_file = postgres_uploads.upsert_stored_file
    get_stored_file_by_path = postgres_uploads.get_stored_file_by_path
    list_stored_files = postgres_uploads.list_stored_files
    delete_stored_file = postgres_uploads.delete_stored_file
    remove_temp_upload = postgres_uploads.remove_temp_upload

    upsert_media_asset = postgres_catalog.upsert_media_asset
    list_media_assets = postgres_catalog.list_media_assets
    list_media_asset_folders = postgres_catalog.list_media_asset_folders
    get_media_asset_by_path = postgres_catalog.get_media_asset_by_path
    set_media_asset_favorite = postgres_catalog.set_media_asset_favorite
    set_media_asset_archived = postgres_catalog.set_media_asset_archived
    add_catalog_reject = postgres_catalog.add_catalog_reject
    remove_catalog_reject = postgres_catalog.remove_catalog_reject
    is_catalog_reject = postgres_catalog.is_catalog_reject
    count_catalog_rejects = postgres_catalog.count_catalog_rejects
    list_catalog_rejects = postgres_catalog.list_catalog_rejects
    add_tombstone = postgres_catalog.add_tombstone
    is_sha_tombstoned = postgres_catalog.is_sha_tombstoned
    list_sha_tombstones = postgres_catalog.list_sha_tombstones
    remove_tombstone = postgres_catalog.remove_tombstone
    get_tombstone_by_path = postgres_catalog.get_tombstone_by_path
    list_tombstones = postgres_catalog.list_tombstones
    purge_tombstones = postgres_catalog.purge_tombstones
    delete_media_asset = postgres_catalog.delete_media_asset
    _row_to_media_asset_record = postgres_catalog._row_to_media_asset_record
    _list_media_assets_for_backfill = postgres_catalog._list_media_assets_for_backfill
    list_media_assets_for_extraction = postgres_catalog.list_media_assets_for_extraction
    list_media_assets_for_preview = postgres_catalog.list_media_assets_for_preview
    ensure_media_asset_extraction_row = postgres_catalog.ensure_media_asset_extraction_row
    upsert_media_asset_extraction = postgres_catalog.upsert_media_asset_extraction
    ensure_media_asset_preview_row = postgres_catalog.ensure_media_asset_preview_row
    upsert_media_asset_preview = postgres_catalog.upsert_media_asset_preview
    list_duplicate_sha_groups = postgres_catalog.list_duplicate_sha_groups
    record_path_conflict = postgres_catalog.record_path_conflict
    list_path_conflicts = postgres_catalog.list_path_conflicts
    record_storage_index_run = postgres_catalog.record_storage_index_run
    get_latest_storage_index_run = postgres_catalog.get_latest_storage_index_run
    record_catalog_backfill_run = postgres_catalog.record_catalog_backfill_run
    get_latest_catalog_backfill_run = postgres_catalog.get_latest_catalog_backfill_run
    summarize_storage = postgres_catalog.summarize_storage

    upsert_client_pending = postgres_clients.upsert_client_pending
    get_client = postgres_clients.get_client
    list_clients = postgres_clients.list_clients
    approve_client = postgres_clients.approve_client
    revoke_client = postgres_clients.revoke_client
    upsert_client_heartbeat = postgres_clients.upsert_client_heartbeat
    get_client_heartbeat = postgres_clients.get_client_heartbeat
