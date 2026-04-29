"""SQLite schema and persistence helpers for photovault-clientd."""

import logging
import sqlite3
from typing import Sequence

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
