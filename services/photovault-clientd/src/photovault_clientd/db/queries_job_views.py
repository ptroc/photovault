"""Job and event read-model query helpers."""

import sqlite3

from photovault_clientd.state_machine import FileStatus

from .queries_common import TERMINAL_FILE_STATUSES
from .queries_file_progress import count_local_processing_files


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

