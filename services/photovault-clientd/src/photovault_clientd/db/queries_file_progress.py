"""File progress, deduplication, and upload query helpers."""

import sqlite3
from typing import Sequence

from photovault_clientd.state_machine import FileStatus

from .queries_common import (
    COPY_CANDIDATE_STATUSES,
    HASH_PENDING_STATUSES,
    LOCAL_PROCESSING_FILE_STATUSES,
    TERMINAL_FILE_STATUSES,
)


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

