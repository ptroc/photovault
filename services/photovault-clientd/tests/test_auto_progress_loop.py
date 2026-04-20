import time
from datetime import UTC, datetime
from pathlib import Path

from fastapi.testclient import TestClient
from photovault_clientd.app import create_app
from photovault_clientd.db import open_db, set_daemon_state
from photovault_clientd.state_machine import ClientState


def _seed_job(
    db_path: Path,
    *,
    job_status: str,
    daemon_state: ClientState,
    file_status: str | None = None,
    staged_path: str | None = None,
    retry_count: int = 0,
) -> None:
    conn = open_db(db_path)
    now = datetime.now(UTC).isoformat()
    conn.execute(
        """
        INSERT INTO ingest_jobs (id, media_label, status, created_at_utc, updated_at_utc)
        VALUES (1, 'seed-job', ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET status=excluded.status, updated_at_utc=excluded.updated_at_utc;
        """,
        (job_status, now, now),
    )
    if file_status is not None:
        conn.execute(
            """
            INSERT INTO ingest_files (
                id, job_id, source_path, staged_path, sha256_hex, size_bytes,
                status, retry_count, last_error, created_at_utc, updated_at_utc
            )
            VALUES (1, 1, '/media/sd/seed.jpg', ?, ?, ?, ?, ?, NULL, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                staged_path=excluded.staged_path,
                sha256_hex=excluded.sha256_hex,
                size_bytes=excluded.size_bytes,
                status=excluded.status,
                retry_count=excluded.retry_count,
                updated_at_utc=excluded.updated_at_utc;
            """,
            (staged_path, "a" * 64, 10, file_status, retry_count, now, now),
        )
    set_daemon_state(conn, daemon_state, now)
    conn.commit()
    conn.close()


def _wait_for_state(client: TestClient, target_state: str, timeout_seconds: float = 2.0) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        response = client.get("/state")
        assert response.status_code == 200
        if response.json()["current_state"] == target_state:
            return
        time.sleep(0.05)
    raise AssertionError(f"daemon did not reach state={target_state} within {timeout_seconds}s")


def test_auto_progress_advances_job_complete_local_to_idle_without_manual_tick(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    app = create_app(db_path=db_path, auto_progress_interval_seconds=0.1)
    with TestClient(app) as client:
        _seed_job(
            db_path,
            job_status=ClientState.JOB_COMPLETE_LOCAL.value,
            daemon_state=ClientState.JOB_COMPLETE_LOCAL,
        )
        _wait_for_state(client, "IDLE")

    conn = open_db(db_path)
    job_row = conn.execute("SELECT status FROM ingest_jobs WHERE id = 1;").fetchone()
    auto_events = conn.execute(
        "SELECT COUNT(1) FROM daemon_events WHERE category = 'AUTO_PROGRESS_APPLIED';"
    ).fetchone()
    conn.close()

    assert job_row is not None
    assert job_row[0] == "JOB_COMPLETE_LOCAL"
    assert auto_events is not None
    assert auto_events[0] >= 1


def test_auto_progress_drains_remote_completion_follow_up_path(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    staged_file = tmp_path / "staging" / "job-1" / "remote.jpg"
    staged_file.parent.mkdir(parents=True, exist_ok=True)
    staged_file.write_bytes(b"remote")

    app = create_app(
        db_path=db_path,
        retain_staged_files=True,
        auto_progress_interval_seconds=0.1,
    )
    with TestClient(app) as client:
        _seed_job(
            db_path,
            job_status=ClientState.POST_UPLOAD_VERIFY.value,
            daemon_state=ClientState.POST_UPLOAD_VERIFY,
            file_status="VERIFIED_REMOTE",
            staged_path=str(staged_file),
        )
        _wait_for_state(client, "IDLE")

    conn = open_db(db_path)
    job_row = conn.execute("SELECT status FROM ingest_jobs WHERE id = 1;").fetchone()
    conn.close()

    assert job_row is not None
    assert job_row[0] == "JOB_COMPLETE_LOCAL"


def test_auto_progress_stops_at_wait_network_boundary(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    _seed_job(
        db_path,
        job_status=ClientState.WAIT_NETWORK.value,
        daemon_state=ClientState.WAIT_NETWORK,
        file_status="READY_TO_UPLOAD",
        retry_count=3,
    )

    app = create_app(db_path=db_path, auto_progress_interval_seconds=0.1)
    with TestClient(app) as client:
        time.sleep(0.35)
        response = client.get("/state")
        assert response.status_code == 200
        assert response.json()["current_state"] == "WAIT_NETWORK"

    conn = open_db(db_path)
    auto_events = conn.execute(
        "SELECT COUNT(1) FROM daemon_events WHERE category = 'AUTO_PROGRESS_APPLIED';"
    ).fetchone()
    conn.close()
    assert auto_events is not None
    assert auto_events[0] == 0


def test_auto_progress_does_not_busy_loop_when_idle(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    conn = open_db(db_path)
    now = datetime.now(UTC).isoformat()
    set_daemon_state(conn, ClientState.IDLE, now)
    conn.close()

    app = create_app(db_path=db_path, auto_progress_interval_seconds=0.1)
    with TestClient(app):
        time.sleep(0.35)

    conn = open_db(db_path)
    auto_events = conn.execute(
        "SELECT COUNT(1) FROM daemon_events WHERE category = 'AUTO_PROGRESS_APPLIED';"
    ).fetchone()
    conn.close()
    assert auto_events is not None
    assert auto_events[0] == 0
