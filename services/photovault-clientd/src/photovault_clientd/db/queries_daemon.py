"""SQLite schema and persistence helpers for photovault-clientd."""

import json
import logging
import sqlite3

from photovault_clientd.events import EventCategory, EventLevel
from photovault_clientd.state_machine import ClientState, FileStatus
from photovault_clientd.transitions import is_transition_allowed

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
    log_message = (
        "daemon_event timestamp=%s level=%s category=%s from_state=%s to_state=%s message=%s"
    )
    if str(level_value).upper() == EventLevel.ERROR.value:
        DAEMON_EVENT_LOGGER.error(
            log_message,
            created_at_utc,
            level_value,
            category_value,
            from_state.value if from_state is not None else None,
            to_state.value if to_state is not None else None,
            message,
        )
    elif str(level_value).upper() == EventLevel.WARN.value:
        DAEMON_EVENT_LOGGER.warning(
            log_message,
            created_at_utc,
            level_value,
            category_value,
            from_state.value if from_state is not None else None,
            to_state.value if to_state is not None else None,
            message,
        )
    else:
        DAEMON_EVENT_LOGGER.info(
            log_message,
            created_at_utc,
            level_value,
            category_value,
            from_state.value if from_state is not None else None,
            to_state.value if to_state is not None else None,
            message,
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
