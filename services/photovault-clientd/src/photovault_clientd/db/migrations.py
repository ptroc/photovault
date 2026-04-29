"""SQLite schema and persistence helpers for photovault-clientd."""

import logging
import sqlite3
from typing import Callable

from photovault_clientd.state_machine import ClientState, FileStatus

DAEMON_EVENT_LOGGER = logging.getLogger("photovault-clientd.daemon_events")

BOOTSTRAP_RESUME_MAP: dict[FileStatus, ClientState] = {
    FileStatus.DISCOVERED: ClientState.STAGING_COPY,
    FileStatus.NEEDS_RETRY_COPY: ClientState.STAGING_COPY,
    FileStatus.STAGED: ClientState.HASHING,
    FileStatus.NEEDS_RETRY_HASH: ClientState.HASHING,
    FileStatus.HASHED: ClientState.DEDUP_SESSION_SHA,
    FileStatus.READY_TO_UPLOAD: ClientState.WAIT_NETWORK,
    FileStatus.UPLOADED: ClientState.SERVER_VERIFY,
    FileStatus.VERIFY_RUNNING: ClientState.VERIFY_HASH,
}

TERMINAL_FILE_STATUSES = {
    FileStatus.VERIFIED_REMOTE.value,
    FileStatus.DUPLICATE_SHA_GLOBAL.value,
    FileStatus.DUPLICATE_SHA_LOCAL.value,
    FileStatus.DUPLICATE_SESSION_SHA.value,
    FileStatus.ERROR_FILE.value,
    FileStatus.QUARANTINED_LOCAL.value,
}

NON_TERMINAL_FILE_STATUSES = {
    status.value for status in FileStatus if status.value not in TERMINAL_FILE_STATUSES
}

COPY_CANDIDATE_STATUSES = {
    FileStatus.DISCOVERED.value,
    FileStatus.NEEDS_RETRY_COPY.value,
}

HASH_PENDING_STATUSES = {
    FileStatus.STAGED.value,
    FileStatus.NEEDS_RETRY_HASH.value,
}

LOCAL_PROCESSING_FILE_STATUSES = (
    FileStatus.DISCOVERED.value,
    FileStatus.NEEDS_RETRY_COPY.value,
    FileStatus.STAGED.value,
    FileStatus.NEEDS_RETRY_HASH.value,
    FileStatus.HASHED.value,
)

BOOTSTRAP_JOB_PHASES = (
    ClientState.DEDUP_LOCAL_SHA,
    ClientState.QUEUE_UPLOAD,
)

RECOVERY_STATE_PRIORITY = (
    ClientState.STAGING_COPY,
    ClientState.HASHING,
    ClientState.DEDUP_SESSION_SHA,
    ClientState.DEDUP_LOCAL_SHA,
    ClientState.QUEUE_UPLOAD,
    ClientState.WAIT_NETWORK,
    ClientState.SERVER_VERIFY,
    ClientState.VERIFY_HASH,
)

DETECTED_MEDIA_STATUS_PRESENT = "PRESENT"
DETECTED_MEDIA_STATUS_REMOVED = "REMOVED"
DETECTED_MEDIA_STATUSES = (
    DETECTED_MEDIA_STATUS_PRESENT,
    DETECTED_MEDIA_STATUS_REMOVED,
)

DETECTED_MEDIA_EVENT_INSERTED = "INSERTED"
DETECTED_MEDIA_EVENT_REMOVED = "REMOVED"
DETECTED_MEDIA_EVENT_TYPES = (
    DETECTED_MEDIA_EVENT_INSERTED,
    DETECTED_MEDIA_EVENT_REMOVED,
)

LATEST_SCHEMA_VERSION = 8
CLIENT_ENROLLMENT_PENDING = "pending"
CLIENT_ENROLLMENT_APPROVED = "approved"
CLIENT_ENROLLMENT_REVOKED = "revoked"
CLIENT_ENROLLMENT_STATUSES = (
    CLIENT_ENROLLMENT_PENDING,
    CLIENT_ENROLLMENT_APPROVED,
    CLIENT_ENROLLMENT_REVOKED,
)
HEARTBEAT_STATUS_NEVER = "never"



def _apply_migration_v1(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daemon_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            current_state TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ingest_jobs (
            id INTEGER PRIMARY KEY,
            media_label TEXT,
            status TEXT NOT NULL,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ingest_files (
            id INTEGER PRIMARY KEY,
            job_id INTEGER NOT NULL,
            source_path TEXT NOT NULL,
            staged_path TEXT,
            sha256_hex TEXT,
            size_bytes INTEGER,
            status TEXT NOT NULL,
            retry_count INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL,
            FOREIGN KEY(job_id) REFERENCES ingest_jobs(id)
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ingest_files_status ON ingest_files(status);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ingest_files_job_id ON ingest_files(job_id);")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bootstrap_queue (
            id INTEGER PRIMARY KEY,
            file_id INTEGER NOT NULL,
            target_state TEXT NOT NULL,
            enqueued_at_utc TEXT NOT NULL,
            processed_at_utc TEXT,
            FOREIGN KEY(file_id) REFERENCES ingest_files(id)
        );
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_bootstrap_queue_pending
        ON bootstrap_queue(processed_at_utc);
        """
    )


def _apply_migration_v2(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daemon_events (
            id INTEGER PRIMARY KEY,
            level TEXT NOT NULL,
            category TEXT NOT NULL,
            message TEXT NOT NULL,
            from_state TEXT,
            to_state TEXT,
            created_at_utc TEXT NOT NULL
        );
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_daemon_events_created_at
        ON daemon_events(created_at_utc);
        """
    )


def _apply_migration_v3(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS local_sha_registry (
            sha256_hex TEXT PRIMARY KEY,
            first_file_id INTEGER NOT NULL,
            first_job_id INTEGER NOT NULL,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL,
            FOREIGN KEY(first_file_id) REFERENCES ingest_files(id),
            FOREIGN KEY(first_job_id) REFERENCES ingest_jobs(id)
        );
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_local_sha_registry_job
        ON local_sha_registry(first_job_id);
        """
    )


def _apply_migration_v4(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS network_ap_config (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            profile_name TEXT NOT NULL,
            ssid TEXT NOT NULL,
            password_plaintext TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL,
            last_applied_at_utc TEXT,
            last_apply_error TEXT
        );
        """
    )


def _apply_migration_v5(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS detected_media (
            id INTEGER PRIMARY KEY,
            media_key TEXT NOT NULL UNIQUE,
            filesystem_uuid TEXT,
            device_path TEXT,
            mount_path TEXT,
            filesystem_label TEXT,
            status TEXT NOT NULL,
            inserted_at_utc TEXT,
            removed_at_utc TEXT,
            last_event_at_utc TEXT NOT NULL,
            insert_event_count INTEGER NOT NULL DEFAULT 0,
            remove_event_count INTEGER NOT NULL DEFAULT 0,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        );
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_detected_media_status
        ON detected_media(status, last_event_at_utc DESC);
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS detected_media_events (
            id INTEGER PRIMARY KEY,
            media_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            event_source TEXT NOT NULL,
            filesystem_uuid TEXT,
            device_path TEXT,
            mount_path TEXT,
            filesystem_label TEXT,
            event_at_utc TEXT NOT NULL,
            FOREIGN KEY(media_id) REFERENCES detected_media(id) ON DELETE CASCADE
        );
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_detected_media_events_media_id
        ON detected_media_events(media_id, event_at_utc DESC);
        """
    )


def _apply_migration_v6(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS server_auth_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            client_id TEXT NOT NULL,
            display_name TEXT NOT NULL,
            enrollment_status TEXT NOT NULL,
            auth_token TEXT,
            server_first_seen_at_utc TEXT,
            server_last_enrolled_at_utc TEXT,
            approved_at_utc TEXT,
            revoked_at_utc TEXT,
            last_enrollment_attempt_at_utc TEXT,
            last_enrollment_result_at_utc TEXT,
            last_error TEXT,
            updated_at_utc TEXT NOT NULL
        );
        """
    )


def _apply_migration_v7(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS server_heartbeat_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            heartbeat_interval_seconds INTEGER NOT NULL,
            last_attempt_at_utc TEXT,
            last_success_at_utc TEXT,
            last_error TEXT,
            last_status TEXT NOT NULL,
            next_due_at_utc TEXT,
            updated_at_utc TEXT NOT NULL
        );
        """
    )


def _apply_migration_v8(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS network_portal_handoff_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            active INTEGER NOT NULL DEFAULT 0,
            started_at_utc TEXT,
            previous_eth_route_prefs_json TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        );
        """
    )


MIGRATIONS: dict[int, Callable[[sqlite3.Connection], None]] = {
    1: _apply_migration_v1,
    2: _apply_migration_v2,
    3: _apply_migration_v3,
    4: _apply_migration_v4,
    5: _apply_migration_v5,
    6: _apply_migration_v6,
    7: _apply_migration_v7,
    8: _apply_migration_v8,
}


def apply_schema_migrations(conn: sqlite3.Connection) -> int:
    current_version = get_schema_version(conn)
    if current_version > LATEST_SCHEMA_VERSION:
        raise RuntimeError(
            f"database schema version {current_version} is newer than supported {LATEST_SCHEMA_VERSION}"
        )

    for version in range(current_version + 1, LATEST_SCHEMA_VERSION + 1):
        migration = MIGRATIONS[version]
        migration(conn)
        _set_schema_version(conn, version)
        conn.commit()

    return get_schema_version(conn)


def get_schema_version(conn: sqlite3.Connection) -> int:
    row = conn.execute("PRAGMA user_version;").fetchone()
    return int(row[0]) if row else 0

def _set_schema_version(conn: sqlite3.Connection, version: int) -> None:
    conn.execute(f"PRAGMA user_version = {version};")