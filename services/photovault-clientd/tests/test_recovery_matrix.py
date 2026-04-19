from datetime import UTC, datetime
from pathlib import Path

from photovault_clientd.db import (
    BOOTSTRAP_RESUME_MAP,
    NON_TERMINAL_FILE_STATUSES,
    RECOVERY_STATE_PRIORITY,
    TERMINAL_FILE_STATUSES,
    bootstrap_recovery,
    consume_bootstrap_queue,
    open_db,
)
from photovault_clientd.state_machine import ClientState, FileStatus


def _seed_status(conn, file_id: int, status: FileStatus, now: str) -> None:
    conn.execute(
        """
        INSERT INTO ingest_jobs (id, media_label, status, created_at_utc, updated_at_utc)
        VALUES (1, 'matrix-job', 'DISCOVERING', ?, ?)
        ON CONFLICT(id) DO NOTHING;
        """,
        (now, now),
    )
    conn.execute(
        """
        INSERT INTO ingest_files (
            id, job_id, source_path, staged_path, sha256_hex, size_bytes,
            status, retry_count, last_error, created_at_utc, updated_at_utc
        )
        VALUES (?, 1, ?, NULL, NULL, NULL, ?, 0, NULL, ?, ?);
        """,
        (file_id, f"/media/sd/{file_id}.jpg", status.value, now, now),
    )


def test_recovery_policy_covers_all_non_terminal_and_excludes_terminal() -> None:
    mapped = {status.value for status in BOOTSTRAP_RESUME_MAP}
    assert mapped == NON_TERMINAL_FILE_STATUSES
    assert mapped.isdisjoint(TERMINAL_FILE_STATUSES)


def test_bootstrap_recovery_matrix_matches_expected_status_mapping(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    conn = open_db(db_path)
    now = datetime.now(UTC).isoformat()

    for file_id, status in enumerate(FileStatus, start=1):
        _seed_status(conn, file_id, status, now)
    conn.commit()

    queued = bootstrap_recovery(conn, now)
    rows = conn.execute(
        """
        SELECT file_id, target_state
        FROM bootstrap_queue
        WHERE processed_at_utc IS NULL
        ORDER BY file_id ASC
        """
    ).fetchall()

    expected = []
    for file_id, status in enumerate(FileStatus, start=1):
        target = BOOTSTRAP_RESUME_MAP.get(status)
        if target is not None:
            expected.append((file_id, target.value))

    assert queued == len(expected)
    assert rows == expected
    conn.close()


def test_bootstrap_consume_uses_documented_priority_order(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    conn = open_db(db_path)
    now = datetime.now(UTC).isoformat()

    _seed_status(conn, 1, FileStatus.UPLOADED, now)
    _seed_status(conn, 2, FileStatus.DISCOVERED, now)
    _seed_status(conn, 3, FileStatus.HASHED, now)
    conn.commit()

    bootstrap_recovery(conn, now)
    selected = consume_bootstrap_queue(conn, now)

    assert selected == RECOVERY_STATE_PRIORITY[0]
    processed_count = conn.execute(
        "SELECT COUNT(1) FROM bootstrap_queue WHERE processed_at_utc IS NOT NULL"
    ).fetchone()
    assert processed_count is not None
    assert processed_count[0] == 3
    conn.close()


def test_bootstrap_recovery_uses_persisted_job_phase_for_later_m1_states(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    conn = open_db(db_path)
    now = datetime.now(UTC).isoformat()

    _seed_status(conn, 1, FileStatus.HASHED, now)
    conn.execute(
        "UPDATE ingest_jobs SET status = ? WHERE id = 1;",
        ("DEDUP_LOCAL_SHA",),
    )
    conn.commit()

    bootstrap_recovery(conn, now)
    selected = consume_bootstrap_queue(conn, now)
    conn.close()

    assert selected == ClientState.DEDUP_LOCAL_SHA
