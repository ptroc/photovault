"""SQLite schema and persistence helpers for photovault-clientd."""

import json
import sqlite3
from pathlib import Path
from typing import Callable, Sequence

from photovault_clientd.events import EventCategory, EventLevel
from photovault_clientd.state_machine import ClientState, FileStatus
from photovault_clientd.transitions import is_transition_allowed

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


def validate_recovery_policy() -> None:
    mapped_statuses = {status.value for status in BOOTSTRAP_RESUME_MAP}
    missing = NON_TERMINAL_FILE_STATUSES - mapped_statuses
    invalid = mapped_statuses & TERMINAL_FILE_STATUSES
    if missing:
        raise RuntimeError(f"bootstrap recovery map missing non-terminal statuses: {sorted(missing)}")
    if invalid:
        raise RuntimeError(f"bootstrap recovery map contains terminal statuses: {sorted(invalid)}")


validate_recovery_policy()


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


def get_schema_version(conn: sqlite3.Connection) -> int:
    row = conn.execute("PRAGMA user_version;").fetchone()
    return int(row[0]) if row else 0


def _set_schema_version(conn: sqlite3.Connection, version: int) -> None:
    conn.execute(f"PRAGMA user_version = {version};")


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


def open_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA foreign_keys = ON;")
    apply_schema_migrations(conn)
    return conn


def set_daemon_state(
    conn: sqlite3.Connection,
    state: ClientState,
    now_utc: str,
    *,
    commit: bool = True,
) -> None:
    conn.execute(
        """
        INSERT INTO daemon_state (id, current_state, updated_at_utc)
        VALUES (1, ?, ?)
        ON CONFLICT(id) DO UPDATE
        SET current_state=excluded.current_state,
            updated_at_utc=excluded.updated_at_utc;
        """,
        (state.value, now_utc),
    )
    if commit:
        conn.commit()


def append_daemon_event(
    conn: sqlite3.Connection,
    *,
    level: EventLevel | str,
    category: EventCategory | str,
    message: str,
    created_at_utc: str,
    from_state: ClientState | None = None,
    to_state: ClientState | None = None,
) -> None:
    level_value = level.value if isinstance(level, EventLevel) else level
    category_value = category.value if isinstance(category, EventCategory) else category
    conn.execute(
        """
        INSERT INTO daemon_events (level, category, message, from_state, to_state, created_at_utc)
        VALUES (?, ?, ?, ?, ?, ?);
        """,
        (
            level_value,
            category_value,
            message,
            from_state.value if from_state else None,
            to_state.value if to_state else None,
            created_at_utc,
        ),
    )


def transition_daemon_state(
    conn: sqlite3.Connection,
    target: ClientState,
    now_utc: str,
    *,
    reason: str,
    commit: bool = True,
) -> None:
    current = get_daemon_state(conn)
    if not is_transition_allowed(current, target):
        append_daemon_event(
            conn,
            level=EventLevel.ERROR,
            category=EventCategory.TRANSITION_VIOLATION,
            message=f"disallowed transition: {current} -> {target}; reason={reason}",
            created_at_utc=now_utc,
            from_state=current,
            to_state=target,
        )
        if commit:
            conn.commit()
        raise ValueError(f"disallowed transition {current} -> {target}")

    set_daemon_state(conn, target, now_utc, commit=False)
    append_daemon_event(
        conn,
        level=EventLevel.INFO,
        category=EventCategory.STATE_TRANSITION,
        message=reason,
        created_at_utc=now_utc,
        from_state=current,
        to_state=target,
    )
    if commit:
        conn.commit()


def get_daemon_state(conn: sqlite3.Connection) -> ClientState | None:
    row = conn.execute("SELECT current_state FROM daemon_state WHERE id = 1;").fetchone()
    if row is None:
        return None
    return ClientState(row[0])


def get_daemon_state_safe(conn: sqlite3.Connection) -> ClientState | None:
    try:
        return get_daemon_state(conn)
    except ValueError:
        return None


def run_state_invariant_checks(conn: sqlite3.Connection) -> list[str]:
    issues: list[str] = []

    file_status_values = tuple(status.value for status in FileStatus)
    file_placeholders = ",".join("?" for _ in file_status_values)
    row = conn.execute(
        f"""
        SELECT COUNT(1)
        FROM ingest_files
        WHERE status NOT IN ({file_placeholders});
        """,
        file_status_values,
    ).fetchone()
    if row and row[0] > 0:
        issues.append(f"ingest_files contains unknown status values: {row[0]} row(s)")

    state_values = tuple(state.value for state in ClientState)
    state_placeholders = ",".join("?" for _ in state_values)
    row = conn.execute(
        f"""
        SELECT COUNT(1)
        FROM daemon_state
        WHERE current_state NOT IN ({state_placeholders});
        """,
        state_values,
    ).fetchone()
    if row and row[0] > 0:
        issues.append(f"daemon_state contains unknown current_state values: {row[0]} row(s)")

    row = conn.execute(
        f"""
        SELECT COUNT(1)
        FROM ingest_jobs
        WHERE status NOT IN ({state_placeholders});
        """,
        state_values,
    ).fetchone()
    if row and row[0] > 0:
        issues.append(f"ingest_jobs contains unknown status values: {row[0]} row(s)")

    row = conn.execute(
        """
        SELECT COUNT(1)
        FROM bootstrap_queue q
        LEFT JOIN ingest_files f ON f.id = q.file_id
        WHERE q.processed_at_utc IS NULL AND f.id IS NULL;
        """
    ).fetchone()
    if row and row[0] > 0:
        issues.append(
            f"bootstrap_queue has pending rows with missing ingest_files references: {row[0]} row(s)"
        )

    terminal_values = tuple(TERMINAL_FILE_STATUSES)
    terminal_placeholders = ",".join("?" for _ in terminal_values)
    row = conn.execute(
        f"""
        SELECT COUNT(1)
        FROM bootstrap_queue q
        JOIN ingest_files f ON f.id = q.file_id
        WHERE q.processed_at_utc IS NULL
          AND f.status IN ({terminal_placeholders});
        """,
        terminal_values,
    ).fetchone()
    if row and row[0] > 0:
        issues.append(f"bootstrap_queue has pending rows targeting terminal files: {row[0]} row(s)")

    row = conn.execute(
        """
        SELECT COUNT(1)
        FROM ingest_files
        WHERE status IN (?, ?)
          AND (sha256_hex IS NULL OR LENGTH(sha256_hex) != 64);
        """,
        (FileStatus.HASHED.value, FileStatus.READY_TO_UPLOAD.value),
    ).fetchone()
    if row and row[0] > 0:
        issues.append(f"HASHED/READY_TO_UPLOAD files missing valid sha256_hex values: {row[0]} row(s)")

    row = conn.execute("SELECT COUNT(1) FROM ingest_files WHERE retry_count < 0;").fetchone()
    if row and row[0] > 0:
        issues.append(f"ingest_files contains negative retry_count values: {row[0]} row(s)")

    row = conn.execute(
        """
        SELECT COUNT(1)
        FROM local_sha_registry r
        LEFT JOIN ingest_files f ON f.id = r.first_file_id
        LEFT JOIN ingest_jobs j ON j.id = r.first_job_id
        WHERE f.id IS NULL OR j.id IS NULL OR LENGTH(r.sha256_hex) != 64;
        """
    ).fetchone()
    if row and row[0] > 0:
        issues.append(f"local_sha_registry contains invalid provenance rows: {row[0]} row(s)")

    detected_media_statuses = tuple(DETECTED_MEDIA_STATUSES)
    detected_media_placeholders = ",".join("?" for _ in detected_media_statuses)
    row = conn.execute(
        f"""
        SELECT COUNT(1)
        FROM detected_media
        WHERE status NOT IN ({detected_media_placeholders});
        """,
        detected_media_statuses,
    ).fetchone()
    if row and row[0] > 0:
        issues.append(f"detected_media contains unknown status values: {row[0]} row(s)")

    detected_media_event_types = tuple(DETECTED_MEDIA_EVENT_TYPES)
    detected_media_event_placeholders = ",".join("?" for _ in detected_media_event_types)
    row = conn.execute(
        f"""
        SELECT COUNT(1)
        FROM detected_media_events
        WHERE event_type NOT IN ({detected_media_event_placeholders});
        """,
        detected_media_event_types,
    ).fetchone()
    if row and row[0] > 0:
        issues.append(f"detected_media_events contains unknown event_type values: {row[0]} row(s)")

    row = conn.execute(
        """
        SELECT COUNT(1)
        FROM detected_media_events e
        LEFT JOIN detected_media m ON m.id = e.media_id
        WHERE m.id IS NULL;
        """
    ).fetchone()
    if row and row[0] > 0:
        issues.append(f"detected_media_events has rows with missing media reference: {row[0]} row(s)")

    client_enrollment_statuses = tuple(CLIENT_ENROLLMENT_STATUSES)
    client_enrollment_placeholders = ",".join("?" for _ in client_enrollment_statuses)
    row = conn.execute(
        f"""
        SELECT COUNT(1)
        FROM server_auth_state
        WHERE enrollment_status NOT IN ({client_enrollment_placeholders});
        """,
        client_enrollment_statuses,
    ).fetchone()
    if row and row[0] > 0:
        issues.append(f"server_auth_state contains unknown enrollment_status values: {row[0]} row(s)")

    row = conn.execute(
        """
        SELECT COUNT(1)
        FROM server_heartbeat_state
        WHERE heartbeat_interval_seconds <= 0 OR last_status = '';
        """
    ).fetchone()
    if row and row[0] > 0:
        issues.append(f"server_heartbeat_state contains invalid interval or status values: {row[0]} row(s)")

    return issues


def fetch_server_auth_state(conn: sqlite3.Connection) -> dict[str, object] | None:
    row = conn.execute(
        """
        SELECT
            client_id,
            display_name,
            enrollment_status,
            auth_token,
            server_first_seen_at_utc,
            server_last_enrolled_at_utc,
            approved_at_utc,
            revoked_at_utc,
            last_enrollment_attempt_at_utc,
            last_enrollment_result_at_utc,
            last_error,
            updated_at_utc
        FROM server_auth_state
        WHERE id = 1;
        """
    ).fetchone()
    if row is None:
        return None
    return {
        "client_id": str(row[0]),
        "display_name": str(row[1]),
        "enrollment_status": str(row[2]),
        "auth_token": row[3],
        "server_first_seen_at_utc": row[4],
        "server_last_enrolled_at_utc": row[5],
        "approved_at_utc": row[6],
        "revoked_at_utc": row[7],
        "last_enrollment_attempt_at_utc": row[8],
        "last_enrollment_result_at_utc": row[9],
        "last_error": row[10],
        "updated_at_utc": str(row[11]),
    }


def upsert_server_auth_state(
    conn: sqlite3.Connection,
    *,
    client_id: str,
    display_name: str,
    enrollment_status: str,
    auth_token: str | None,
    server_first_seen_at_utc: str | None,
    server_last_enrolled_at_utc: str | None,
    approved_at_utc: str | None,
    revoked_at_utc: str | None,
    last_enrollment_attempt_at_utc: str | None,
    last_enrollment_result_at_utc: str | None,
    last_error: str | None,
    updated_at_utc: str,
) -> None:
    conn.execute(
        """
        INSERT INTO server_auth_state (
            id,
            client_id,
            display_name,
            enrollment_status,
            auth_token,
            server_first_seen_at_utc,
            server_last_enrolled_at_utc,
            approved_at_utc,
            revoked_at_utc,
            last_enrollment_attempt_at_utc,
            last_enrollment_result_at_utc,
            last_error,
            updated_at_utc
        )
        VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE
        SET client_id = excluded.client_id,
            display_name = excluded.display_name,
            enrollment_status = excluded.enrollment_status,
            auth_token = excluded.auth_token,
            server_first_seen_at_utc = excluded.server_first_seen_at_utc,
            server_last_enrolled_at_utc = excluded.server_last_enrolled_at_utc,
            approved_at_utc = excluded.approved_at_utc,
            revoked_at_utc = excluded.revoked_at_utc,
            last_enrollment_attempt_at_utc = excluded.last_enrollment_attempt_at_utc,
            last_enrollment_result_at_utc = excluded.last_enrollment_result_at_utc,
            last_error = excluded.last_error,
            updated_at_utc = excluded.updated_at_utc;
        """,
        (
            client_id,
            display_name,
            enrollment_status,
            auth_token,
            server_first_seen_at_utc,
            server_last_enrolled_at_utc,
            approved_at_utc,
            revoked_at_utc,
            last_enrollment_attempt_at_utc,
            last_enrollment_result_at_utc,
            last_error,
            updated_at_utc,
        ),
    )


def fetch_server_heartbeat_state(conn: sqlite3.Connection) -> dict[str, object] | None:
    row = conn.execute(
        """
        SELECT
            heartbeat_interval_seconds,
            last_attempt_at_utc,
            last_success_at_utc,
            last_error,
            last_status,
            next_due_at_utc,
            updated_at_utc
        FROM server_heartbeat_state
        WHERE id = 1;
        """
    ).fetchone()
    if row is None:
        return None
    return {
        "heartbeat_interval_seconds": int(row[0]),
        "last_attempt_at_utc": row[1],
        "last_success_at_utc": row[2],
        "last_error": row[3],
        "last_status": str(row[4]),
        "next_due_at_utc": row[5],
        "updated_at_utc": str(row[6]),
    }


def upsert_server_heartbeat_state(
    conn: sqlite3.Connection,
    *,
    heartbeat_interval_seconds: int,
    last_attempt_at_utc: str | None,
    last_success_at_utc: str | None,
    last_error: str | None,
    last_status: str,
    next_due_at_utc: str | None,
    updated_at_utc: str,
) -> None:
    conn.execute(
        """
        INSERT INTO server_heartbeat_state (
            id,
            heartbeat_interval_seconds,
            last_attempt_at_utc,
            last_success_at_utc,
            last_error,
            last_status,
            next_due_at_utc,
            updated_at_utc
        )
        VALUES (1, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE
        SET heartbeat_interval_seconds = excluded.heartbeat_interval_seconds,
            last_attempt_at_utc = excluded.last_attempt_at_utc,
            last_success_at_utc = excluded.last_success_at_utc,
            last_error = excluded.last_error,
            last_status = excluded.last_status,
            next_due_at_utc = excluded.next_due_at_utc,
            updated_at_utc = excluded.updated_at_utc;
        """,
        (
            heartbeat_interval_seconds,
            last_attempt_at_utc,
            last_success_at_utc,
            last_error,
            last_status,
            next_due_at_utc,
            updated_at_utc,
        ),
    )


def fetch_network_ap_config(conn: sqlite3.Connection) -> dict[str, object] | None:
    row = conn.execute(
        """
        SELECT
            profile_name,
            ssid,
            password_plaintext,
            updated_at_utc,
            last_applied_at_utc,
            last_apply_error
        FROM network_ap_config
        WHERE id = 1;
        """
    ).fetchone()
    if row is None:
        return None
    return {
        "profile_name": str(row[0]),
        "ssid": str(row[1]),
        "password_plaintext": str(row[2]),
        "updated_at_utc": str(row[3]),
        "last_applied_at_utc": row[4],
        "last_apply_error": row[5],
    }


def upsert_network_ap_config(
    conn: sqlite3.Connection,
    *,
    profile_name: str,
    ssid: str,
    password_plaintext: str,
    now_utc: str,
) -> None:
    conn.execute(
        """
        INSERT INTO network_ap_config (
            id, profile_name, ssid, password_plaintext, updated_at_utc, last_applied_at_utc, last_apply_error
        )
        VALUES (1, ?, ?, ?, ?, NULL, NULL)
        ON CONFLICT(id) DO UPDATE
        SET profile_name = excluded.profile_name,
            ssid = excluded.ssid,
            password_plaintext = excluded.password_plaintext,
            updated_at_utc = excluded.updated_at_utc;
        """,
        (profile_name, ssid, password_plaintext, now_utc),
    )


def set_network_ap_apply_result(
    conn: sqlite3.Connection,
    *,
    last_applied_at_utc: str | None,
    last_apply_error: str | None,
) -> None:
    conn.execute(
        """
        UPDATE network_ap_config
        SET last_applied_at_utc = ?, last_apply_error = ?
        WHERE id = 1;
        """,
        (last_applied_at_utc, last_apply_error),
    )


def fetch_network_portal_handoff_state(conn: sqlite3.Connection) -> dict[str, object] | None:
    row = conn.execute(
        """
        SELECT active, started_at_utc, previous_eth_route_prefs_json, updated_at_utc
        FROM network_portal_handoff_state
        WHERE id = 1;
        """
    ).fetchone()
    if row is None:
        return None
    return {
        "active": bool(int(row[0])),
        "started_at_utc": row[1],
        "previous_eth_route_prefs_json": str(row[2]),
        "updated_at_utc": str(row[3]),
    }


def upsert_network_portal_handoff_state(
    conn: sqlite3.Connection,
    *,
    active: bool,
    started_at_utc: str | None,
    previous_eth_route_prefs_json: str | None,
    updated_at_utc: str,
) -> None:
    serialized_previous = previous_eth_route_prefs_json
    if serialized_previous is None:
        serialized_previous = json.dumps([])
    conn.execute(
        """
        INSERT INTO network_portal_handoff_state (
            id, active, started_at_utc, previous_eth_route_prefs_json, updated_at_utc
        )
        VALUES (1, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE
        SET active = excluded.active,
            started_at_utc = excluded.started_at_utc,
            previous_eth_route_prefs_json = excluded.previous_eth_route_prefs_json,
            updated_at_utc = excluded.updated_at_utc;
        """,
        (
            1 if active else 0,
            started_at_utc,
            serialized_previous,
            updated_at_utc,
        ),
    )


def create_ingest_job(conn: sqlite3.Connection, media_label: str, now_utc: str) -> int:
    cursor = conn.execute(
        """
        INSERT INTO ingest_jobs (media_label, status, created_at_utc, updated_at_utc)
        VALUES (?, ?, ?, ?);
        """,
        (media_label, ClientState.DISCOVERING.value, now_utc, now_utc),
    )
    return int(cursor.lastrowid)


def insert_discovered_files(
    conn: sqlite3.Connection,
    job_id: int,
    source_paths: Sequence[str],
    now_utc: str,
) -> int:
    inserted = 0
    for source_path in source_paths:
        conn.execute(
            """
            INSERT INTO ingest_files (
                job_id, source_path, staged_path, sha256_hex, size_bytes,
                status, retry_count, last_error, created_at_utc, updated_at_utc
            )
            VALUES (?, ?, NULL, NULL, NULL, ?, 0, NULL, ?, ?);
            """,
            (job_id, source_path, FileStatus.DISCOVERED.value, now_utc, now_utc),
        )
        inserted += 1
    return inserted


def replace_copy_candidate_with_discovered_files(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    file_id: int,
    source_paths: Sequence[str],
    now_utc: str,
) -> tuple[bool, int]:
    conn.execute("DELETE FROM bootstrap_queue WHERE file_id = ?;", (file_id,))
    cursor = conn.execute(
        """
        DELETE FROM ingest_files
        WHERE id = ? AND job_id = ? AND status IN (?, ?);
        """,
        (file_id, job_id, FileStatus.DISCOVERED.value, FileStatus.NEEDS_RETRY_COPY.value),
    )
    if cursor.rowcount != 1:
        return False, 0
    inserted = insert_discovered_files(conn, job_id, source_paths, now_utc)
    return True, inserted


def set_job_status(conn: sqlite3.Connection, job_id: int, status: str, now_utc: str) -> None:
    conn.execute(
        """
        UPDATE ingest_jobs
        SET status = ?, updated_at_utc = ?
        WHERE id = ?;
        """,
        (status, now_utc, job_id),
    )


def ingest_job_exists(conn: sqlite3.Connection, job_id: int) -> bool:
    row = conn.execute("SELECT 1 FROM ingest_jobs WHERE id = ? LIMIT 1;", (job_id,)).fetchone()
    return row is not None


def fetch_next_copy_candidate(conn: sqlite3.Connection, job_id: int) -> tuple[int, str] | None:
    row = conn.execute(
        """
        SELECT id, source_path
        FROM ingest_files
        WHERE job_id = ? AND status IN (?, ?)
        ORDER BY id ASC
        LIMIT 1;
        """,
        (job_id, *sorted(COPY_CANDIDATE_STATUSES)),
    ).fetchone()
    if row is None:
        return None
    return int(row[0]), str(row[1])


def fetch_next_staging_job_with_pending_copy(conn: sqlite3.Connection) -> int | None:
    row = conn.execute(
        """
        SELECT j.id
        FROM ingest_jobs j
        WHERE EXISTS (
            SELECT 1
            FROM ingest_files f
            WHERE f.job_id = j.id AND f.status IN (?, ?)
        )
        ORDER BY j.id ASC
        LIMIT 1;
        """,
        (FileStatus.DISCOVERED.value, FileStatus.NEEDS_RETRY_COPY.value),
    ).fetchone()
    if row is None:
        return None
    return int(row[0])


def fetch_next_hashing_job_with_pending_hash(conn: sqlite3.Connection) -> int | None:
    row = conn.execute(
        """
        SELECT j.id
        FROM ingest_jobs j
        WHERE EXISTS (
            SELECT 1
            FROM ingest_files f
            WHERE f.job_id = j.id AND f.status IN (?, ?)
        )
        ORDER BY j.id ASC
        LIMIT 1;
        """,
        (FileStatus.STAGED.value, FileStatus.NEEDS_RETRY_HASH.value),
    ).fetchone()
    if row is None:
        return None
    return int(row[0])


def fetch_next_job_with_status(conn: sqlite3.Connection, status: ClientState) -> int | None:
    row = conn.execute(
        """
        SELECT id
        FROM ingest_jobs
        WHERE status = ?
        ORDER BY id ASC
        LIMIT 1;
        """,
        (status.value,),
    ).fetchone()
    if row is None:
        return None
    return int(row[0])


def mark_file_staged(
    conn: sqlite3.Connection,
    file_id: int,
    staged_path: str,
    size_bytes: int,
    now_utc: str,
) -> None:
    conn.execute(
        """
        UPDATE ingest_files
        SET status = ?, staged_path = ?, size_bytes = ?, last_error = NULL, updated_at_utc = ?
        WHERE id = ?;
        """,
        (FileStatus.STAGED.value, staged_path, size_bytes, now_utc, file_id),
    )


def fetch_next_hash_candidate(conn: sqlite3.Connection, job_id: int) -> tuple[int, str | None] | None:
    row = conn.execute(
        """
        SELECT id, staged_path
        FROM ingest_files
        WHERE job_id = ? AND status IN (?, ?)
        ORDER BY id ASC
        LIMIT 1;
        """,
        (job_id, FileStatus.STAGED.value, FileStatus.NEEDS_RETRY_HASH.value),
    ).fetchone()
    if row is None:
        return None
    return int(row[0]), row[1]


def mark_file_hashed(conn: sqlite3.Connection, file_id: int, sha256_hex: str, now_utc: str) -> None:
    conn.execute(
        """
        UPDATE ingest_files
        SET status = ?, sha256_hex = ?, last_error = NULL, updated_at_utc = ?
        WHERE id = ?;
        """,
        (FileStatus.HASHED.value, sha256_hex, now_utc, file_id),
    )


def fetch_hashed_files_for_job(conn: sqlite3.Connection, job_id: int) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT id, sha256_hex
        FROM ingest_files
        WHERE job_id = ? AND status = ?
        ORDER BY sha256_hex ASC, id ASC;
        """,
        (job_id, FileStatus.HASHED.value),
    ).fetchall()
    return [{"file_id": int(row[0]), "sha256_hex": str(row[1])} for row in rows]


def mark_files_duplicate_session(
    conn: sqlite3.Connection,
    file_ids: Sequence[int],
    now_utc: str,
) -> int:
    if not file_ids:
        return 0
    placeholders = ",".join("?" for _ in file_ids)
    params: tuple[object, ...] = (FileStatus.DUPLICATE_SESSION_SHA.value, now_utc, *file_ids)
    conn.execute(
        f"""
        UPDATE ingest_files
        SET status = ?, last_error = NULL, updated_at_utc = ?
        WHERE id IN ({placeholders});
        """,
        params,
    )
    return len(file_ids)


def mark_files_duplicate_local(
    conn: sqlite3.Connection,
    file_ids: Sequence[int],
    now_utc: str,
) -> int:
    if not file_ids:
        return 0
    placeholders = ",".join("?" for _ in file_ids)
    params: tuple[object, ...] = (FileStatus.DUPLICATE_SHA_LOCAL.value, now_utc, *file_ids)
    conn.execute(
        f"""
        UPDATE ingest_files
        SET status = ?, last_error = NULL, updated_at_utc = ?
        WHERE id IN ({placeholders});
        """,
        params,
    )
    return len(file_ids)


def mark_file_duplicate_global(
    conn: sqlite3.Connection,
    file_id: int,
    now_utc: str,
) -> None:
    conn.execute(
        """
        UPDATE ingest_files
        SET status = ?, last_error = NULL, updated_at_utc = ?
        WHERE id = ?;
        """,
        (FileStatus.DUPLICATE_SHA_GLOBAL.value, now_utc, file_id),
    )


def mark_files_ready_to_upload(
    conn: sqlite3.Connection,
    file_ids: Sequence[int],
    now_utc: str,
) -> int:
    if not file_ids:
        return 0
    placeholders = ",".join("?" for _ in file_ids)
    params: tuple[object, ...] = (FileStatus.READY_TO_UPLOAD.value, now_utc, *file_ids)
    conn.execute(
        f"""
        UPDATE ingest_files
        SET status = ?, last_error = NULL, updated_at_utc = ?
        WHERE id IN ({placeholders});
        """,
        params,
    )
    return len(file_ids)


def fetch_ready_to_upload_files_global(conn: sqlite3.Connection) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT id, job_id, sha256_hex, size_bytes, retry_count, updated_at_utc
        FROM ingest_files
        WHERE status = ?
        ORDER BY id ASC;
        """,
        (FileStatus.READY_TO_UPLOAD.value,),
    ).fetchall()
    return [
        {
            "file_id": int(row[0]),
            "job_id": int(row[1]),
            "sha256_hex": row[2],
            "size_bytes": row[3],
            "retry_count": int(row[4]),
            "updated_at_utc": row[5],
        }
        for row in rows
    ]


def fetch_next_ready_to_upload_file(conn: sqlite3.Connection) -> dict[str, object] | None:
    row = conn.execute(
        """
        SELECT
            f.id, f.job_id, f.staged_path, f.sha256_hex,
            f.size_bytes, f.retry_count, f.source_path, j.media_label
        FROM ingest_files f
        JOIN ingest_jobs j ON j.id = f.job_id
        WHERE f.status = ?
        ORDER BY f.id ASC
        LIMIT 1;
        """,
        (FileStatus.READY_TO_UPLOAD.value,),
    ).fetchone()
    if row is None:
        return None
    return {
        "file_id": int(row[0]),
        "job_id": int(row[1]),
        "staged_path": row[2],
        "sha256_hex": row[3],
        "size_bytes": row[4],
        "retry_count": int(row[5]),
        "source_path": row[6],
        "job_name": row[7],
    }


def fetch_next_uploaded_file(conn: sqlite3.Connection) -> dict[str, object] | None:
    row = conn.execute(
        """
        SELECT id, job_id, sha256_hex, size_bytes, retry_count, updated_at_utc
        FROM ingest_files
        WHERE status = ?
        ORDER BY id ASC
        LIMIT 1;
        """,
        (FileStatus.UPLOADED.value,),
    ).fetchone()
    if row is None:
        return None
    return {
        "file_id": int(row[0]),
        "job_id": int(row[1]),
        "sha256_hex": row[2],
        "size_bytes": row[3],
        "retry_count": int(row[4]),
        "updated_at_utc": row[5],
    }


def fetch_wait_network_retry_candidates(conn: sqlite3.Connection) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT id, job_id, status, retry_count, last_error, updated_at_utc
        FROM ingest_files
        WHERE status IN (?, ?)
        ORDER BY id ASC;
        """,
        (FileStatus.READY_TO_UPLOAD.value, FileStatus.UPLOADED.value),
    ).fetchall()
    return [
        {
            "file_id": int(row[0]),
            "job_id": int(row[1]),
            "status": row[2],
            "retry_count": int(row[3]),
            "last_error": row[4],
            "updated_at_utc": row[5],
        }
        for row in rows
    ]


def fetch_reupload_target_file(conn: sqlite3.Connection, job_id: int) -> dict[str, object] | None:
    row = conn.execute(
        """
        SELECT id, job_id, staged_path, sha256_hex, size_bytes, retry_count, last_error, updated_at_utc
        FROM ingest_files
        WHERE job_id = ?
          AND status = ?
          AND last_error = ?
        ORDER BY updated_at_utc DESC, id DESC
        LIMIT 1;
        """,
        (job_id, FileStatus.READY_TO_UPLOAD.value, "server verification failed"),
    ).fetchone()
    if row is not None:
        return {
            "file_id": int(row[0]),
            "job_id": int(row[1]),
            "staged_path": row[2],
            "sha256_hex": row[3],
            "size_bytes": row[4],
            "retry_count": int(row[5]),
            "last_error": row[6],
            "updated_at_utc": row[7],
        }

    row = conn.execute(
        """
        SELECT id, job_id, staged_path, sha256_hex, size_bytes, retry_count, last_error, updated_at_utc
        FROM ingest_files
        WHERE job_id = ?
          AND status = ?
        ORDER BY updated_at_utc DESC, id DESC
        LIMIT 1;
        """,
        (job_id, FileStatus.READY_TO_UPLOAD.value),
    ).fetchone()
    if row is None:
        return None
    return {
        "file_id": int(row[0]),
        "job_id": int(row[1]),
        "staged_path": row[2],
        "sha256_hex": row[3],
        "size_bytes": row[4],
        "retry_count": int(row[5]),
        "last_error": row[6],
        "updated_at_utc": row[7],
    }


def fetch_cleanup_remote_terminal_files(conn: sqlite3.Connection, job_id: int) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT id, staged_path, status
        FROM ingest_files
        WHERE job_id = ?
          AND status IN (?, ?)
        ORDER BY id ASC;
        """,
        (job_id, FileStatus.VERIFIED_REMOTE.value, FileStatus.DUPLICATE_SHA_GLOBAL.value),
    ).fetchall()
    return [
        {
            "file_id": int(row[0]),
            "staged_path": row[1],
            "status": row[2],
        }
        for row in rows
    ]


def count_job_files_by_statuses(conn: sqlite3.Connection, job_id: int, statuses: Sequence[str]) -> int:
    if not statuses:
        return 0
    placeholders = ",".join("?" for _ in statuses)
    row = conn.execute(
        f"""
        SELECT COUNT(1)
        FROM ingest_files
        WHERE job_id = ? AND status IN ({placeholders});
        """,
        (job_id, *statuses),
    ).fetchone()
    return int(row[0]) if row else 0


def count_non_terminal_files_for_job(conn: sqlite3.Connection, job_id: int) -> int:
    placeholders = ",".join("?" for _ in TERMINAL_FILE_STATUSES)
    row = conn.execute(
        f"""
        SELECT COUNT(1)
        FROM ingest_files
        WHERE job_id = ? AND status NOT IN ({placeholders});
        """,
        (job_id, *tuple(TERMINAL_FILE_STATUSES)),
    ).fetchone()
    return int(row[0]) if row else 0


def count_uploaded_files_global(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COUNT(1) FROM ingest_files WHERE status = ?;",
        (FileStatus.UPLOADED.value,),
    ).fetchone()
    return int(row[0]) if row else 0


def mark_file_uploaded(conn: sqlite3.Connection, file_id: int, now_utc: str) -> None:
    conn.execute(
        """
        UPDATE ingest_files
        SET status = ?, last_error = NULL, updated_at_utc = ?
        WHERE id = ?;
        """,
        (FileStatus.UPLOADED.value, now_utc, file_id),
    )


def mark_file_verified_remote(conn: sqlite3.Connection, file_id: int, now_utc: str) -> None:
    conn.execute(
        """
        UPDATE ingest_files
        SET status = ?, last_error = NULL, updated_at_utc = ?
        WHERE id = ?;
        """,
        (FileStatus.VERIFIED_REMOTE.value, now_utc, file_id),
    )


def mark_ready_to_upload_retry(
    conn: sqlite3.Connection,
    file_id: int,
    error_message: str,
    now_utc: str,
) -> None:
    conn.execute(
        """
        UPDATE ingest_files
        SET retry_count = retry_count + 1, last_error = ?, updated_at_utc = ?
        WHERE id = ? AND status = ?;
        """,
        (error_message, now_utc, file_id, FileStatus.READY_TO_UPLOAD.value),
    )


def mark_uploaded_retry(
    conn: sqlite3.Connection,
    file_id: int,
    error_message: str,
    now_utc: str,
) -> None:
    conn.execute(
        """
        UPDATE ingest_files
        SET retry_count = retry_count + 1, last_error = ?, updated_at_utc = ?
        WHERE id = ? AND status = ?;
        """,
        (error_message, now_utc, file_id, FileStatus.UPLOADED.value),
    )


def mark_uploaded_for_reupload(
    conn: sqlite3.Connection,
    file_id: int,
    error_message: str,
    now_utc: str,
) -> None:
    conn.execute(
        """
        UPDATE ingest_files
        SET status = ?, retry_count = retry_count + 1, last_error = ?, updated_at_utc = ?
        WHERE id = ? AND status = ?;
        """,
        (FileStatus.READY_TO_UPLOAD.value, error_message, now_utc, file_id, FileStatus.UPLOADED.value),
    )


def mark_ready_to_upload_error(
    conn: sqlite3.Connection,
    file_id: int,
    error_message: str,
    now_utc: str,
) -> None:
    conn.execute(
        """
        UPDATE ingest_files
        SET status = ?, last_error = ?, updated_at_utc = ?
        WHERE id = ? AND status = ?;
        """,
        (FileStatus.ERROR_FILE.value, error_message, now_utc, file_id, FileStatus.READY_TO_UPLOAD.value),
    )


def requeue_error_file_for_upload(conn: sqlite3.Connection, file_id: int, now_utc: str) -> int:
    cursor = conn.execute(
        """
        UPDATE ingest_files
        SET status = ?, last_error = NULL, updated_at_utc = ?
        WHERE id = ? AND status = ?;
        """,
        (FileStatus.READY_TO_UPLOAD.value, now_utc, file_id, FileStatus.ERROR_FILE.value),
    )
    return int(cursor.rowcount)


def mark_files_upload_retry(
    conn: sqlite3.Connection,
    file_ids: Sequence[int],
    error_message: str,
    now_utc: str,
) -> int:
    if not file_ids:
        return 0
    placeholders = ",".join("?" for _ in file_ids)
    params: tuple[object, ...] = (error_message, now_utc, *file_ids)
    cursor = conn.execute(
        f"""
        UPDATE ingest_files
        SET retry_count = retry_count + 1, last_error = ?, updated_at_utc = ?
        WHERE id IN ({placeholders}) AND status = ?;
        """,
        (*params, FileStatus.READY_TO_UPLOAD.value),
    )
    return int(cursor.rowcount)


def clear_ready_to_upload_error(
    conn: sqlite3.Connection,
    file_id: int,
    now_utc: str,
) -> None:
    conn.execute(
        """
        UPDATE ingest_files
        SET last_error = NULL, updated_at_utc = ?
        WHERE id = ? AND status = ?;
        """,
        (now_utc, file_id, FileStatus.READY_TO_UPLOAD.value),
    )


def register_local_sha(
    conn: sqlite3.Connection,
    sha256_hex: str,
    file_id: int,
    job_id: int,
    now_utc: str,
) -> None:
    conn.execute(
        """
        INSERT INTO local_sha_registry (
            sha256_hex, first_file_id, first_job_id, created_at_utc, updated_at_utc
        )
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(sha256_hex) DO UPDATE
        SET updated_at_utc = excluded.updated_at_utc;
        """,
        (sha256_hex, file_id, job_id, now_utc, now_utc),
    )


def local_sha_exists(conn: sqlite3.Connection, sha256_hex: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM local_sha_registry WHERE sha256_hex = ? LIMIT 1;",
        (sha256_hex,),
    ).fetchone()
    return row is not None


def count_pending_copy_files(conn: sqlite3.Connection, job_id: int) -> int:
    row = conn.execute(
        "SELECT COUNT(1) FROM ingest_files WHERE job_id = ? AND status IN (?, ?);",
        (job_id, *sorted(COPY_CANDIDATE_STATUSES)),
    ).fetchone()
    return int(row[0]) if row else 0


def count_pending_copy_files_global(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COUNT(1) FROM ingest_files WHERE status IN (?, ?);",
        tuple(sorted(COPY_CANDIDATE_STATUSES)),
    ).fetchone()
    return int(row[0]) if row else 0


def count_staged_files(conn: sqlite3.Connection, job_id: int) -> int:
    row = conn.execute(
        "SELECT COUNT(1) FROM ingest_files WHERE job_id = ? AND status = ?;",
        (job_id, FileStatus.STAGED.value),
    ).fetchone()
    return int(row[0]) if row else 0


def count_staged_files_global(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COUNT(1) FROM ingest_files WHERE status = ?;",
        (FileStatus.STAGED.value,),
    ).fetchone()
    return int(row[0]) if row else 0


def count_hash_pending_files(conn: sqlite3.Connection, job_id: int) -> int:
    row = conn.execute(
        "SELECT COUNT(1) FROM ingest_files WHERE job_id = ? AND status IN (?, ?);",
        (job_id, *sorted(HASH_PENDING_STATUSES)),
    ).fetchone()
    return int(row[0]) if row else 0


def count_hash_pending_files_global(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COUNT(1) FROM ingest_files WHERE status IN (?, ?);",
        tuple(sorted(HASH_PENDING_STATUSES)),
    ).fetchone()
    return int(row[0]) if row else 0


def count_hashed_files(conn: sqlite3.Connection, job_id: int) -> int:
    row = conn.execute(
        "SELECT COUNT(1) FROM ingest_files WHERE job_id = ? AND status = ?;",
        (job_id, FileStatus.HASHED.value),
    ).fetchone()
    return int(row[0]) if row else 0


def count_hashed_files_global(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COUNT(1) FROM ingest_files WHERE status = ?;",
        (FileStatus.HASHED.value,),
    ).fetchone()
    return int(row[0]) if row else 0


def count_ready_to_upload_files(conn: sqlite3.Connection, job_id: int) -> int:
    row = conn.execute(
        "SELECT COUNT(1) FROM ingest_files WHERE job_id = ? AND status = ?;",
        (job_id, FileStatus.READY_TO_UPLOAD.value),
    ).fetchone()
    return int(row[0]) if row else 0


def count_ready_to_upload_files_global(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COUNT(1) FROM ingest_files WHERE status = ?;",
        (FileStatus.READY_TO_UPLOAD.value,),
    ).fetchone()
    return int(row[0]) if row else 0


def count_local_processing_files(conn: sqlite3.Connection, job_id: int) -> int:
    placeholders = ",".join("?" for _ in LOCAL_PROCESSING_FILE_STATUSES)
    row = conn.execute(
        f"SELECT COUNT(1) FROM ingest_files WHERE job_id = ? AND status IN ({placeholders});",
        (job_id, *LOCAL_PROCESSING_FILE_STATUSES),
    ).fetchone()
    return int(row[0]) if row else 0


def mark_file_copy_retry(conn: sqlite3.Connection, file_id: int, error_message: str, now_utc: str) -> None:
    conn.execute(
        """
        UPDATE ingest_files
        SET status = ?, retry_count = retry_count + 1, last_error = ?, updated_at_utc = ?
        WHERE id = ?;
        """,
        (FileStatus.NEEDS_RETRY_COPY.value, error_message, now_utc, file_id),
    )


def mark_file_hash_retry(conn: sqlite3.Connection, file_id: int, error_message: str, now_utc: str) -> None:
    conn.execute(
        """
        UPDATE ingest_files
        SET status = ?, retry_count = retry_count + 1, last_error = ?, updated_at_utc = ?
        WHERE id = ?;
        """,
        (FileStatus.NEEDS_RETRY_HASH.value, error_message, now_utc, file_id),
    )


def _find_job_phase_override(
    conn: sqlite3.Connection,
    job_id: int,
    target_state: ClientState,
) -> tuple[int, str] | None:
    row = conn.execute(
        """
        SELECT MIN(id)
        FROM ingest_files
        WHERE job_id = ? AND status NOT IN (?, ?, ?, ?, ?, ?);
        """,
        (job_id, *tuple(TERMINAL_FILE_STATUSES)),
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return int(row[0]), target_state.value


def bootstrap_recovery(conn: sqlite3.Connection, now_utc: str) -> int:
    """Rebuild pending bootstrap queue from persisted non-terminal work."""
    conn.execute("DELETE FROM bootstrap_queue WHERE processed_at_utc IS NULL;")

    rows = conn.execute(
        """
        SELECT f.id, f.status, j.status
        FROM ingest_files f
        JOIN ingest_jobs j ON j.id = f.job_id
        WHERE f.status NOT IN (?, ?, ?, ?, ?, ?)
        ORDER BY f.id ASC;
        """,
        tuple(TERMINAL_FILE_STATUSES),
    ).fetchall()

    queued = 0
    for file_id, status, job_status in rows:
        try:
            file_status = FileStatus(status)
            job_phase = ClientState(job_status)
        except ValueError:
            continue

        if file_status == FileStatus.HASHED and job_phase in BOOTSTRAP_JOB_PHASES:
            continue

        target_state = BOOTSTRAP_RESUME_MAP.get(file_status)
        if target_state is None:
            continue
        conn.execute(
            """
            INSERT INTO bootstrap_queue (file_id, target_state, enqueued_at_utc, processed_at_utc)
            VALUES (?, ?, ?, NULL);
            """,
            (file_id, target_state.value, now_utc),
        )
        queued += 1

    for phase in BOOTSTRAP_JOB_PHASES:
        job_rows = conn.execute(
            """
            SELECT id
            FROM ingest_jobs
            WHERE status = ?
            ORDER BY id ASC;
            """,
            (phase.value,),
        ).fetchall()
        for job_row in job_rows:
            override = _find_job_phase_override(conn, int(job_row[0]), phase)
            if override is None:
                continue
            conn.execute(
                """
                INSERT INTO bootstrap_queue (file_id, target_state, enqueued_at_utc, processed_at_utc)
                VALUES (?, ?, ?, NULL);
                """,
                (override[0], override[1], now_utc),
            )
            queued += 1

    conn.commit()
    return queued


def consume_bootstrap_queue(conn: sqlite3.Connection, now_utc: str) -> ClientState:
    """Consume pending bootstrap queue and choose deterministic resume state."""
    rows = conn.execute(
        """
        SELECT DISTINCT target_state
        FROM bootstrap_queue
        WHERE processed_at_utc IS NULL;
        """
    ).fetchall()
    if not rows:
        return ClientState.IDLE

    available_states: set[ClientState] = set()
    for row in rows:
        available_states.add(ClientState(row[0]))

    selected = next(
        (state for state in RECOVERY_STATE_PRIORITY if state in available_states),
        ClientState.IDLE,
    )
    conn.execute(
        "UPDATE bootstrap_queue SET processed_at_utc = ? WHERE processed_at_utc IS NULL;",
        (now_utc,),
    )
    conn.commit()
    return selected


def fetch_recent_daemon_events(conn: sqlite3.Connection, limit: int = 50) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT id, level, category, message, from_state, to_state, created_at_utc
        FROM daemon_events
        ORDER BY id DESC
        LIMIT ?;
        """,
        (limit,),
    ).fetchall()
    return [
        {
            "id": int(row[0]),
            "level": row[1],
            "category": row[2],
            "message": row[3],
            "from_state": row[4],
            "to_state": row[5],
            "created_at_utc": row[6],
        }
        for row in rows
    ]


def fetch_job_status_counts(conn: sqlite3.Connection, job_id: int) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT status, COUNT(1)
        FROM ingest_files
        WHERE job_id = ?
        GROUP BY status
        ORDER BY status ASC;
        """,
        (job_id,),
    ).fetchall()
    return {str(row[0]): int(row[1]) for row in rows}


def fetch_job_files(conn: sqlite3.Connection, job_id: int) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT id, source_path, staged_path, sha256_hex, size_bytes, status, retry_count, last_error
        FROM ingest_files
        WHERE job_id = ?
        ORDER BY id ASC;
        """,
        (job_id,),
    ).fetchall()
    return [
        {
            "file_id": int(row[0]),
            "source_path": row[1],
            "staged_path": row[2],
            "sha256_hex": row[3],
            "size_bytes": row[4],
            "status": row[5],
            "retry_count": int(row[6]),
            "last_error": row[7],
        }
        for row in rows
    ]


def list_non_terminal_source_paths(conn: sqlite3.Connection) -> list[str]:
    placeholders = ",".join("?" for _ in TERMINAL_FILE_STATUSES)
    rows = conn.execute(
        f"""
        SELECT DISTINCT source_path
        FROM ingest_files
        WHERE status NOT IN ({placeholders});
        """,
        tuple(TERMINAL_FILE_STATUSES),
    ).fetchall()
    return [str(row[0]) for row in rows if row and row[0]]


def fetch_ingest_job_detail(conn: sqlite3.Connection, job_id: int) -> dict[str, object] | None:
    row = conn.execute(
        """
        SELECT id, media_label, status, created_at_utc, updated_at_utc
        FROM ingest_jobs
        WHERE id = ?;
        """,
        (job_id,),
    ).fetchone()
    if row is None:
        return None

    counts = fetch_job_status_counts(conn, job_id)
    ready_to_upload_count = counts.get(FileStatus.READY_TO_UPLOAD.value, 0)
    local_processing_count = count_local_processing_files(conn, job_id)
    return {
        "job_id": int(row[0]),
        "media_label": row[1],
        "status": row[2],
        "created_at_utc": row[3],
        "updated_at_utc": row[4],
        "status_counts": counts,
        "local_ingest_complete": local_processing_count == 0,
        "upload_pending": ready_to_upload_count > 0,
        "files": fetch_job_files(conn, job_id),
    }


def list_ingest_job_summaries(conn: sqlite3.Connection) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT id
        FROM ingest_jobs
        ORDER BY id ASC;
        """
    ).fetchall()
    summaries: list[dict[str, object]] = []
    for row in rows:
        detail = fetch_ingest_job_detail(conn, int(row[0]))
        if detail is None:
            continue
        summaries.append(
            {
                "job_id": detail["job_id"],
                "media_label": detail["media_label"],
                "status": detail["status"],
                "created_at_utc": detail["created_at_utc"],
                "updated_at_utc": detail["updated_at_utc"],
                "status_counts": detail["status_counts"],
                "local_ingest_complete": detail["local_ingest_complete"],
                "upload_pending": detail["upload_pending"],
            }
        )
    return summaries


def _normalize_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized if normalized else None


def _detected_media_from_row(row: Sequence[object]) -> dict[str, object]:
    return {
        "media_id": int(row[0]),
        "media_key": str(row[1]),
        "filesystem_uuid": row[2],
        "device_path": row[3],
        "mount_path": row[4],
        "filesystem_label": row[5],
        "status": str(row[6]),
        "inserted_at_utc": row[7],
        "removed_at_utc": row[8],
        "last_event_at_utc": row[9],
        "insert_event_count": int(row[10]),
        "remove_event_count": int(row[11]),
        "created_at_utc": row[12],
        "updated_at_utc": row[13],
    }


def fetch_detected_media_by_id(conn: sqlite3.Connection, media_id: int) -> dict[str, object] | None:
    row = conn.execute(
        """
        SELECT
            id, media_key, filesystem_uuid, device_path, mount_path, filesystem_label,
            status, inserted_at_utc, removed_at_utc, last_event_at_utc,
            insert_event_count, remove_event_count, created_at_utc, updated_at_utc
        FROM detected_media
        WHERE id = ?
        LIMIT 1;
        """,
        (media_id,),
    ).fetchone()
    if row is None:
        return None
    return _detected_media_from_row(row)


def fetch_detected_media_by_key(conn: sqlite3.Connection, media_key: str) -> dict[str, object] | None:
    row = conn.execute(
        """
        SELECT
            id, media_key, filesystem_uuid, device_path, mount_path, filesystem_label,
            status, inserted_at_utc, removed_at_utc, last_event_at_utc,
            insert_event_count, remove_event_count, created_at_utc, updated_at_utc
        FROM detected_media
        WHERE media_key = ?
        LIMIT 1;
        """,
        (media_key,),
    ).fetchone()
    if row is None:
        return None
    return _detected_media_from_row(row)


def find_detected_media_by_device_or_mount(
    conn: sqlite3.Connection,
    *,
    device_path: str | None,
    mount_path: str | None,
) -> dict[str, object] | None:
    normalized_device = _normalize_optional_text(device_path)
    normalized_mount = _normalize_optional_text(mount_path)
    clauses: list[str] = []
    params: list[object] = []
    if normalized_device is not None:
        clauses.append("device_path = ?")
        params.append(normalized_device)
    if normalized_mount is not None:
        clauses.append("mount_path = ?")
        params.append(normalized_mount)

    if not clauses:
        return None

    where_clause = " OR ".join(clauses)
    row = conn.execute(
        f"""
        SELECT
            id, media_key, filesystem_uuid, device_path, mount_path, filesystem_label,
            status, inserted_at_utc, removed_at_utc, last_event_at_utc,
            insert_event_count, remove_event_count, created_at_utc, updated_at_utc
        FROM detected_media
        WHERE ({where_clause})
        ORDER BY CASE WHEN status = ? THEN 0 ELSE 1 END ASC, last_event_at_utc DESC, id DESC
        LIMIT 1;
        """,
        (*params, DETECTED_MEDIA_STATUS_PRESENT),
    ).fetchone()
    if row is None:
        return None
    return _detected_media_from_row(row)


def list_detected_media(conn: sqlite3.Connection, *, limit: int = 100) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT
            id, media_key, filesystem_uuid, device_path, mount_path, filesystem_label,
            status, inserted_at_utc, removed_at_utc, last_event_at_utc,
            insert_event_count, remove_event_count, created_at_utc, updated_at_utc
        FROM detected_media
        ORDER BY CASE WHEN status = ? THEN 0 ELSE 1 END ASC, last_event_at_utc DESC, id DESC
        LIMIT ?;
        """,
        (DETECTED_MEDIA_STATUS_PRESENT, limit),
    ).fetchall()
    return [_detected_media_from_row(row) for row in rows]


def list_detected_media_events(conn: sqlite3.Connection, *, limit: int = 50) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT
            e.id, e.media_id, m.media_key, e.event_type, e.event_source, e.filesystem_uuid,
            e.device_path, e.mount_path, e.filesystem_label, e.event_at_utc
        FROM detected_media_events e
        JOIN detected_media m ON m.id = e.media_id
        ORDER BY e.id DESC
        LIMIT ?;
        """,
        (limit,),
    ).fetchall()
    return [
        {
            "event_id": int(row[0]),
            "media_id": int(row[1]),
            "media_key": str(row[2]),
            "event_type": str(row[3]),
            "event_source": str(row[4]),
            "filesystem_uuid": row[5],
            "device_path": row[6],
            "mount_path": row[7],
            "filesystem_label": row[8],
            "event_at_utc": row[9],
        }
        for row in rows
    ]


def clear_detected_media(conn: sqlite3.Connection) -> dict[str, int]:
    media_count_row = conn.execute("SELECT COUNT(1) FROM detected_media;").fetchone()
    event_count_row = conn.execute("SELECT COUNT(1) FROM detected_media_events;").fetchone()
    media_count = int(media_count_row[0]) if media_count_row else 0
    event_count = int(event_count_row[0]) if event_count_row else 0

    conn.execute("DELETE FROM detected_media;")
    return {
        "deleted_media_rows": media_count,
        "deleted_event_rows": event_count,
    }


def _append_detected_media_event(
    conn: sqlite3.Connection,
    *,
    media_id: int,
    event_type: str,
    event_source: str,
    filesystem_uuid: str | None,
    device_path: str | None,
    mount_path: str | None,
    filesystem_label: str | None,
    event_at_utc: str,
) -> None:
    conn.execute(
        """
        INSERT INTO detected_media_events (
            media_id, event_type, event_source, filesystem_uuid, device_path,
            mount_path, filesystem_label, event_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (
            media_id,
            event_type,
            event_source,
            filesystem_uuid,
            device_path,
            mount_path,
            filesystem_label,
            event_at_utc,
        ),
    )


def register_detected_media_inserted(
    conn: sqlite3.Connection,
    *,
    media_key: str,
    filesystem_uuid: str | None,
    device_path: str | None,
    mount_path: str | None,
    filesystem_label: str | None,
    event_source: str,
    now_utc: str,
) -> dict[str, object]:
    normalized_uuid = _normalize_optional_text(filesystem_uuid)
    normalized_device = _normalize_optional_text(device_path)
    normalized_mount = _normalize_optional_text(mount_path)
    normalized_label = _normalize_optional_text(filesystem_label)
    normalized_source = _normalize_optional_text(event_source) or "unknown"

    existing = fetch_detected_media_by_key(conn, media_key)
    if existing is None:
        cursor = conn.execute(
            """
            INSERT INTO detected_media (
                media_key, filesystem_uuid, device_path, mount_path, filesystem_label, status,
                inserted_at_utc, removed_at_utc, last_event_at_utc,
                insert_event_count, remove_event_count, created_at_utc, updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, 1, 0, ?, ?);
            """,
            (
                media_key,
                normalized_uuid,
                normalized_device,
                normalized_mount,
                normalized_label,
                DETECTED_MEDIA_STATUS_PRESENT,
                now_utc,
                now_utc,
                now_utc,
                now_utc,
            ),
        )
        media_id = int(cursor.lastrowid)
        _append_detected_media_event(
            conn,
            media_id=media_id,
            event_type=DETECTED_MEDIA_EVENT_INSERTED,
            event_source=normalized_source,
            filesystem_uuid=normalized_uuid,
            device_path=normalized_device,
            mount_path=normalized_mount,
            filesystem_label=normalized_label,
            event_at_utc=now_utc,
        )
        created = fetch_detected_media_by_id(conn, media_id)
        if created is None:
            raise RuntimeError(f"failed to create detected_media row for key={media_key}")
        return {
            "media": created,
            "created": True,
            "status_changed": True,
            "deduplicated": False,
            "event_recorded": True,
        }

    next_uuid = normalized_uuid if normalized_uuid is not None else existing["filesystem_uuid"]
    next_device = normalized_device if normalized_device is not None else existing["device_path"]
    next_mount = normalized_mount if normalized_mount is not None else existing["mount_path"]
    next_label = normalized_label if normalized_label is not None else existing["filesystem_label"]
    status_changed = existing["status"] != DETECTED_MEDIA_STATUS_PRESENT
    metadata_changed = (
        next_uuid != existing["filesystem_uuid"]
        or next_device != existing["device_path"]
        or next_mount != existing["mount_path"]
        or next_label != existing["filesystem_label"]
    )
    should_record_event = status_changed or metadata_changed
    if not should_record_event:
        return {
            "media": existing,
            "created": False,
            "status_changed": False,
            "deduplicated": True,
            "event_recorded": False,
        }

    if status_changed:
        conn.execute(
            """
            UPDATE detected_media
            SET filesystem_uuid = ?,
                device_path = ?,
                mount_path = ?,
                filesystem_label = ?,
                status = ?,
                inserted_at_utc = ?,
                removed_at_utc = NULL,
                last_event_at_utc = ?,
                insert_event_count = insert_event_count + 1,
                updated_at_utc = ?
            WHERE id = ?;
            """,
            (
                next_uuid,
                next_device,
                next_mount,
                next_label,
                DETECTED_MEDIA_STATUS_PRESENT,
                now_utc,
                now_utc,
                now_utc,
                existing["media_id"],
            ),
        )
    else:
        conn.execute(
            """
            UPDATE detected_media
            SET filesystem_uuid = ?,
                device_path = ?,
                mount_path = ?,
                filesystem_label = ?,
                last_event_at_utc = ?,
                updated_at_utc = ?
            WHERE id = ?;
            """,
            (
                next_uuid,
                next_device,
                next_mount,
                next_label,
                now_utc,
                now_utc,
                existing["media_id"],
            ),
        )

    _append_detected_media_event(
        conn,
        media_id=int(existing["media_id"]),
        event_type=DETECTED_MEDIA_EVENT_INSERTED,
        event_source=normalized_source,
        filesystem_uuid=next_uuid,
        device_path=next_device,
        mount_path=next_mount,
        filesystem_label=next_label,
        event_at_utc=now_utc,
    )
    updated = fetch_detected_media_by_id(conn, int(existing["media_id"]))
    if updated is None:
        raise RuntimeError(f"detected_media row disappeared for key={media_key}")
    return {
        "media": updated,
        "created": False,
        "status_changed": status_changed,
        "deduplicated": False,
        "event_recorded": True,
    }


def register_detected_media_removed(
    conn: sqlite3.Connection,
    *,
    media_key: str,
    filesystem_uuid: str | None,
    device_path: str | None,
    mount_path: str | None,
    filesystem_label: str | None,
    event_source: str,
    now_utc: str,
) -> dict[str, object]:
    normalized_uuid = _normalize_optional_text(filesystem_uuid)
    normalized_device = _normalize_optional_text(device_path)
    normalized_mount = _normalize_optional_text(mount_path)
    normalized_label = _normalize_optional_text(filesystem_label)
    normalized_source = _normalize_optional_text(event_source) or "unknown"

    existing = fetch_detected_media_by_key(conn, media_key)
    if existing is None:
        cursor = conn.execute(
            """
            INSERT INTO detected_media (
                media_key, filesystem_uuid, device_path, mount_path, filesystem_label, status,
                inserted_at_utc, removed_at_utc, last_event_at_utc,
                insert_event_count, remove_event_count, created_at_utc, updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, 0, 1, ?, ?);
            """,
            (
                media_key,
                normalized_uuid,
                normalized_device,
                normalized_mount,
                normalized_label,
                DETECTED_MEDIA_STATUS_REMOVED,
                now_utc,
                now_utc,
                now_utc,
                now_utc,
            ),
        )
        media_id = int(cursor.lastrowid)
        _append_detected_media_event(
            conn,
            media_id=media_id,
            event_type=DETECTED_MEDIA_EVENT_REMOVED,
            event_source=normalized_source,
            filesystem_uuid=normalized_uuid,
            device_path=normalized_device,
            mount_path=normalized_mount,
            filesystem_label=normalized_label,
            event_at_utc=now_utc,
        )
        created = fetch_detected_media_by_id(conn, media_id)
        if created is None:
            raise RuntimeError(f"failed to create removed detected_media row for key={media_key}")
        return {
            "media": created,
            "created": True,
            "status_changed": True,
            "deduplicated": False,
            "event_recorded": True,
        }

    next_uuid = normalized_uuid if normalized_uuid is not None else existing["filesystem_uuid"]
    next_device = normalized_device if normalized_device is not None else existing["device_path"]
    next_mount = normalized_mount if normalized_mount is not None else existing["mount_path"]
    next_label = normalized_label if normalized_label is not None else existing["filesystem_label"]
    status_changed = existing["status"] != DETECTED_MEDIA_STATUS_REMOVED
    metadata_changed = (
        next_uuid != existing["filesystem_uuid"]
        or next_device != existing["device_path"]
        or next_mount != existing["mount_path"]
        or next_label != existing["filesystem_label"]
    )
    should_record_event = status_changed or metadata_changed
    if not should_record_event:
        return {
            "media": existing,
            "created": False,
            "status_changed": False,
            "deduplicated": True,
            "event_recorded": False,
        }

    if status_changed:
        conn.execute(
            """
            UPDATE detected_media
            SET filesystem_uuid = ?,
                device_path = ?,
                mount_path = ?,
                filesystem_label = ?,
                status = ?,
                removed_at_utc = ?,
                last_event_at_utc = ?,
                remove_event_count = remove_event_count + 1,
                updated_at_utc = ?
            WHERE id = ?;
            """,
            (
                next_uuid,
                next_device,
                next_mount,
                next_label,
                DETECTED_MEDIA_STATUS_REMOVED,
                now_utc,
                now_utc,
                now_utc,
                existing["media_id"],
            ),
        )
    else:
        conn.execute(
            """
            UPDATE detected_media
            SET filesystem_uuid = ?,
                device_path = ?,
                mount_path = ?,
                filesystem_label = ?,
                last_event_at_utc = ?,
                updated_at_utc = ?
            WHERE id = ?;
            """,
            (
                next_uuid,
                next_device,
                next_mount,
                next_label,
                now_utc,
                now_utc,
                existing["media_id"],
            ),
        )

    _append_detected_media_event(
        conn,
        media_id=int(existing["media_id"]),
        event_type=DETECTED_MEDIA_EVENT_REMOVED,
        event_source=normalized_source,
        filesystem_uuid=next_uuid,
        device_path=next_device,
        mount_path=next_mount,
        filesystem_label=next_label,
        event_at_utc=now_utc,
    )
    updated = fetch_detected_media_by_id(conn, int(existing["media_id"]))
    if updated is None:
        raise RuntimeError(f"detected_media row disappeared for key={media_key}")
    return {
        "media": updated,
        "created": False,
        "status_changed": status_changed,
        "deduplicated": False,
        "event_recorded": True,
    }
