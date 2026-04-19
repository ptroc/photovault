"""SQLite schema and persistence helpers for photovault-clientd."""

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

LATEST_SCHEMA_VERSION = 3


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


MIGRATIONS: dict[int, Callable[[sqlite3.Connection], None]] = {
    1: _apply_migration_v1,
    2: _apply_migration_v2,
    3: _apply_migration_v3,
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

    return issues


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
        SELECT id, job_id, sha256_hex, size_bytes
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
        }
        for row in rows
    ]


def fetch_next_ready_to_upload_file(conn: sqlite3.Connection) -> dict[str, object] | None:
    row = conn.execute(
        """
        SELECT id, job_id, staged_path, sha256_hex, size_bytes
        FROM ingest_files
        WHERE status = ?
        ORDER BY id ASC
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
    }


def fetch_next_uploaded_file(conn: sqlite3.Connection) -> dict[str, object] | None:
    row = conn.execute(
        """
        SELECT id, job_id, sha256_hex, size_bytes
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
    }


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
