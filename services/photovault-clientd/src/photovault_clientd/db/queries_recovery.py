"""Bootstrap recovery query helpers."""

import sqlite3

from photovault_clientd.state_machine import ClientState, FileStatus

from .queries_common import (
    BOOTSTRAP_JOB_PHASES,
    BOOTSTRAP_RESUME_MAP,
    RECOVERY_STATE_PRIORITY,
    TERMINAL_FILE_STATUSES,
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

