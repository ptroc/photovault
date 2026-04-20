import time
from datetime import UTC, datetime
from pathlib import Path
from threading import Event, Thread

from fastapi.testclient import TestClient
from photovault_clientd import engine
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


def _wait_for_status_count(
    db_path: Path, *, status: str, expected: int, timeout_seconds: float = 2.0
) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        conn = open_db(db_path)
        row = conn.execute(
            "SELECT COUNT(1) FROM ingest_files WHERE status = ?;",
            (status,),
        ).fetchone()
        conn.close()
        if row is not None and int(row[0]) == expected:
            return
        time.sleep(0.05)
    raise AssertionError(
        f"file status count for {status} did not reach expected={expected} within {timeout_seconds}s"
    )


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


def test_manual_tick_is_safe_noop_when_another_tick_cycle_is_in_flight(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "state.sqlite3"
    _seed_job(
        db_path,
        job_status=ClientState.HASHING.value,
        daemon_state=ClientState.HASHING,
        file_status="STAGED",
    )

    tick_started = Event()
    release_tick = Event()

    def _slow_run_daemon_tick(*args, **kwargs) -> dict[str, object]:
        tick_started.set()
        release_tick.wait()
        return {
            "handled": True,
            "progressed": True,
            "errored": False,
            "next_state": "DEDUP_SESSION_SHA",
        }

    monkeypatch.setattr("photovault_clientd.app.run_daemon_tick", _slow_run_daemon_tick)

    app = create_app(db_path=db_path, auto_progress_interval_seconds=10.0)
    with TestClient(app) as client:
        first_response: dict[str, object] = {}

        def _first_tick() -> None:
            response = client.post("/daemon/tick")
            first_response["status_code"] = response.status_code
            first_response["payload"] = response.json()

        first_thread = Thread(target=_first_tick)
        first_thread.start()
        try:
            assert tick_started.wait(timeout=1.5)

            response = client.post("/daemon/tick")
            assert response.status_code == 200
            payload = response.json()
            assert payload["handled"] is True
            assert payload["progressed"] is False
            assert payload["already_progressing"] is True
            assert payload["no_op"] is True
            assert payload["next_state"] == "HASHING"
        finally:
            release_tick.set()
            first_thread.join(timeout=2.0)

    assert first_response["status_code"] == 200
    assert first_response["payload"]["handled"] is True
    assert first_response["payload"]["next_state"] == "DEDUP_SESSION_SHA"

    conn = open_db(db_path)
    transition_violations = conn.execute(
        "SELECT COUNT(1) FROM daemon_events WHERE category = 'TRANSITION_VIOLATION';"
    ).fetchone()
    busy_noops = conn.execute(
        """
        SELECT COUNT(1)
        FROM daemon_events
        WHERE category = 'TICK_NOOP'
          AND message LIKE 'manual daemon tick skipped because another progression cycle is active%';
        """
    ).fetchone()
    conn.close()

    assert transition_violations is not None
    assert transition_violations[0] == 0
    assert busy_noops is not None
    assert busy_noops[0] >= 1


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


def test_auto_progress_drains_online_pipeline_to_idle_without_manual_ticks(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "state.sqlite3"
    staged_file = tmp_path / "staging" / "job-1" / "file-1.jpg"
    staged_file.parent.mkdir(parents=True, exist_ok=True)
    staged_file.write_bytes(b"autodrain")

    monkeypatch.setattr(engine, "_network_is_online", lambda: True)
    monkeypatch.setattr(
        engine,
        "_post_metadata_handshake",
        lambda *, server_base_url, files, timeout_seconds=5.0: {
            int(item["file_id"]): "UPLOAD_REQUIRED" for item in files
        },
    )
    monkeypatch.setattr(
        engine,
        "_upload_file_content",
        lambda *, server_base_url, sha256_hex, size_bytes, content, timeout_seconds=5.0: "STORED_TEMP",
    )
    monkeypatch.setattr(
        engine,
        "_post_server_verify",
        lambda *, server_base_url, sha256_hex, size_bytes, timeout_seconds=5.0: "VERIFIED",
    )

    _seed_job(
        db_path,
        job_status=ClientState.WAIT_NETWORK.value,
        daemon_state=ClientState.WAIT_NETWORK,
        file_status="READY_TO_UPLOAD",
        staged_path=str(staged_file),
        retry_count=0,
    )

    app = create_app(
        db_path=db_path,
        retain_staged_files=True,
        auto_progress_interval_seconds=0.1,
    )
    with TestClient(app) as client:
        _wait_for_state(client, "IDLE", timeout_seconds=3.0)

    conn = open_db(db_path)
    file_status = conn.execute("SELECT status FROM ingest_files WHERE id = 1;").fetchone()
    job_status = conn.execute("SELECT status FROM ingest_jobs WHERE id = 1;").fetchone()
    auto_events = conn.execute(
        "SELECT COUNT(1) FROM daemon_events WHERE category = 'AUTO_PROGRESS_APPLIED';"
    ).fetchone()
    conn.close()

    assert file_status is not None
    assert file_status[0] == "VERIFIED_REMOTE"
    assert job_status is not None
    assert job_status[0] == "JOB_COMPLETE_LOCAL"
    assert auto_events is not None
    assert auto_events[0] >= 1


def test_auto_progress_stops_at_wait_network_when_offline(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "state.sqlite3"
    staged_file = tmp_path / "staging" / "job-1" / "file-1.jpg"
    staged_file.parent.mkdir(parents=True, exist_ok=True)
    staged_file.write_bytes(b"offline")

    monkeypatch.setattr(engine, "_network_is_online", lambda: False)
    _seed_job(
        db_path,
        job_status=ClientState.WAIT_NETWORK.value,
        daemon_state=ClientState.WAIT_NETWORK,
        file_status="READY_TO_UPLOAD",
        staged_path=str(staged_file),
        retry_count=0,
    )

    app = create_app(db_path=db_path, auto_progress_interval_seconds=0.1)
    with TestClient(app) as client:
        time.sleep(0.35)
        response = client.get("/state")
        assert response.status_code == 200
        assert response.json()["current_state"] == "WAIT_NETWORK"

    conn = open_db(db_path)
    file_status = conn.execute("SELECT status FROM ingest_files WHERE id = 1;").fetchone()
    auto_events = conn.execute(
        "SELECT COUNT(1) FROM daemon_events WHERE category = 'AUTO_PROGRESS_APPLIED';"
    ).fetchone()
    conn.close()
    assert file_status is not None
    assert file_status[0] == "READY_TO_UPLOAD"
    assert auto_events is not None
    assert auto_events[0] == 0


def test_auto_progress_continues_to_next_ready_file_after_cleanup(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "state.sqlite3"
    staged_dir = tmp_path / "staging" / "job-1"
    staged_dir.mkdir(parents=True, exist_ok=True)
    first_file = staged_dir / "file-1.jpg"
    second_file = staged_dir / "file-2.jpg"
    first_file.write_bytes(b"first")
    second_file.write_bytes(b"second")

    monkeypatch.setattr(engine, "_network_is_online", lambda: True)
    monkeypatch.setattr(
        engine,
        "_post_metadata_handshake",
        lambda *, server_base_url, files, timeout_seconds=5.0: {
            int(item["file_id"]): "UPLOAD_REQUIRED" for item in files
        },
    )
    monkeypatch.setattr(
        engine,
        "_upload_file_content",
        lambda *, server_base_url, sha256_hex, size_bytes, content, timeout_seconds=5.0: "STORED_TEMP",
    )
    monkeypatch.setattr(
        engine,
        "_post_server_verify",
        lambda *, server_base_url, sha256_hex, size_bytes, timeout_seconds=5.0: "VERIFIED",
    )

    conn = open_db(db_path)
    now = datetime.now(UTC).isoformat()
    conn.execute(
        """
        INSERT INTO ingest_jobs (id, media_label, status, created_at_utc, updated_at_utc)
        VALUES (1, 'seed-job', ?, ?, ?);
        """,
        (ClientState.WAIT_NETWORK.value, now, now),
    )
    conn.execute(
        """
        INSERT INTO ingest_files (
            id, job_id, source_path, staged_path, sha256_hex, size_bytes,
            status, retry_count, last_error, created_at_utc, updated_at_utc
        ) VALUES
            (1, 1, '/media/sd/one.jpg', ?, ?, ?, 'READY_TO_UPLOAD', 0, NULL, ?, ?),
            (2, 1, '/media/sd/two.jpg', ?, ?, ?, 'READY_TO_UPLOAD', 0, NULL, ?, ?);
        """,
        (str(first_file), "1" * 64, 5, now, now, str(second_file), "2" * 64, 6, now, now),
    )
    set_daemon_state(conn, ClientState.WAIT_NETWORK, now)
    conn.commit()
    conn.close()

    app = create_app(
        db_path=db_path,
        retain_staged_files=True,
        auto_progress_interval_seconds=0.1,
    )
    with TestClient(app) as client:
        _wait_for_status_count(db_path, status="VERIFIED_REMOTE", expected=2, timeout_seconds=3.0)
        _wait_for_state(client, "IDLE", timeout_seconds=3.0)

    conn = open_db(db_path)
    summary = conn.execute(
        """
        SELECT
            SUM(CASE WHEN status = 'VERIFIED_REMOTE' THEN 1 ELSE 0 END),
            SUM(CASE WHEN status = 'READY_TO_UPLOAD' THEN 1 ELSE 0 END)
        FROM ingest_files;
        """
    ).fetchone()
    conn.close()
    assert summary is not None
    assert int(summary[0] or 0) == 2
    assert int(summary[1] or 0) == 0


def test_auto_progress_stops_at_error_file_when_retries_exhausted(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "state.sqlite3"
    staged_file = tmp_path / "staging" / "job-1" / "file-1.jpg"
    staged_file.parent.mkdir(parents=True, exist_ok=True)
    staged_file.write_bytes(b"retry-fail")

    monkeypatch.setattr(engine, "_network_is_online", lambda: True)
    monkeypatch.setattr(
        engine,
        "_post_server_verify",
        lambda *, server_base_url, sha256_hex, size_bytes, timeout_seconds=5.0: "VERIFY_FAILED",
    )

    conn = open_db(db_path)
    now = datetime.now(UTC).isoformat()
    conn.execute(
        """
        INSERT INTO ingest_jobs (id, media_label, status, created_at_utc, updated_at_utc)
        VALUES (1, 'seed-job', ?, ?, ?);
        """,
        (ClientState.SERVER_VERIFY.value, now, now),
    )
    conn.execute(
        """
        INSERT INTO ingest_files (
            id, job_id, source_path, staged_path, sha256_hex, size_bytes,
            status, retry_count, last_error, created_at_utc, updated_at_utc
        ) VALUES (
            1, 1, '/media/sd/retry.jpg', ?, ?, ?, 'UPLOADED', ?, NULL, ?, ?
        );
        """,
        (str(staged_file), "f" * 64, 10, engine.DEFAULT_MAX_UPLOAD_RETRIES, now, now),
    )
    set_daemon_state(conn, ClientState.SERVER_VERIFY, now)
    conn.commit()
    conn.close()

    app = create_app(
        db_path=db_path,
        retain_staged_files=True,
        auto_progress_interval_seconds=0.1,
    )
    with TestClient(app) as client:
        _wait_for_state(client, "ERROR_FILE", timeout_seconds=3.0)

    conn = open_db(db_path)
    file_status = conn.execute("SELECT status FROM ingest_files WHERE id = 1;").fetchone()
    conn.close()
    assert file_status is not None
    assert file_status[0] == "ERROR_FILE"
