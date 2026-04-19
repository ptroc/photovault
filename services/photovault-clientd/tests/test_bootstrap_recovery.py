from datetime import UTC, datetime
from pathlib import Path

from fastapi.testclient import TestClient
from photovault_clientd.app import create_app
from photovault_clientd.db import bootstrap_recovery, open_db


def _seed_job_and_file(db_path: Path, status: str, file_id: int, source_path: str | None = None) -> None:
    conn = open_db(db_path)
    now = datetime.now(UTC).isoformat()
    conn.execute(
        """
        INSERT INTO ingest_jobs (id, media_label, status, created_at_utc, updated_at_utc)
        VALUES (1, 'sdcard-1', 'DISCOVERING', ?, ?)
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
        (file_id, source_path or f"/media/sd/{file_id}.jpg", status, now, now),
    )
    conn.commit()
    conn.close()


def test_bootstrap_recovery_enqueues_only_recoverable_non_terminal_states(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    _seed_job_and_file(db_path, "DISCOVERED", 0)
    _seed_job_and_file(db_path, "STAGED", 1)
    _seed_job_and_file(db_path, "NEEDS_RETRY_COPY", 11)
    _seed_job_and_file(db_path, "HASHED", 2)
    _seed_job_and_file(db_path, "READY_TO_UPLOAD", 3)
    _seed_job_and_file(db_path, "UPLOADED", 4)
    _seed_job_and_file(db_path, "VERIFY_RUNNING", 5)
    _seed_job_and_file(db_path, "DUPLICATE_SHA_LOCAL", 6)

    conn = open_db(db_path)
    queued = bootstrap_recovery(conn, datetime.now(UTC).isoformat())
    rows = conn.execute(
        "SELECT file_id, target_state FROM bootstrap_queue WHERE processed_at_utc IS NULL ORDER BY file_id"
    ).fetchall()
    conn.close()

    assert queued == 7
    assert rows == [
        (0, "STAGING_COPY"),
        (1, "HASHING"),
        (2, "DEDUP_SESSION_SHA"),
        (3, "WAIT_NETWORK"),
        (4, "SERVER_VERIFY"),
        (5, "VERIFY_HASH"),
        (11, "STAGING_COPY"),
    ]


def test_startup_sets_daemon_state_to_selected_recovery_state(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    _seed_job_and_file(db_path, "STAGED", 10)

    app = create_app(db_path=db_path)
    with TestClient(app) as client:
        state_response = client.get("/state")
        queue_response = client.get("/bootstrap/recovery")

    assert state_response.status_code == 200
    assert state_response.json()["current_state"] == "HASHING"
    assert queue_response.status_code == 200
    assert queue_response.json()["pending_count"] == 0
    assert queue_response.json()["processed_count"] == 1


def test_startup_recovery_prioritizes_copy_before_hash(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    _seed_job_and_file(db_path, "DISCOVERED", 1)
    _seed_job_and_file(db_path, "STAGED", 2)

    app = create_app(db_path=db_path)
    with TestClient(app) as client:
        state_response = client.get("/state")
        queue_response = client.get("/bootstrap/recovery")

    assert state_response.status_code == 200
    assert state_response.json()["current_state"] == "STAGING_COPY"
    assert queue_response.status_code == 200
    assert queue_response.json()["processed_count"] == 2


def test_startup_recovery_dispatch_executes_copy_phase_until_wait_network_boundary(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    source = tmp_path / "media" / "sd" / "recover.jpg"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(b"recover-bytes")
    staging_root = tmp_path / "staging"
    _seed_job_and_file(db_path, "DISCOVERED", 21, source_path=str(source))

    app = create_app(db_path=db_path, staging_root=staging_root)
    with TestClient(app) as client:
        state_response = client.get("/state")
        events_response = client.get("/events")

    assert state_response.status_code == 200
    assert state_response.json()["current_state"] == "WAIT_NETWORK"
    assert events_response.status_code == 200
    assert any(
        item["category"] == "RECOVERY_BOUNDARY_UNIMPLEMENTED"
        for item in events_response.json()["events"]
    )

    conn = open_db(db_path)
    row = conn.execute(
        "SELECT status, staged_path, size_bytes, sha256_hex FROM ingest_files WHERE id = 21;"
    ).fetchone()
    conn.close()
    assert row is not None
    assert row[0] == "READY_TO_UPLOAD"
    assert row[2] == len(b"recover-bytes")
    assert isinstance(row[3], str)
    assert len(row[3]) == 64
    assert Path(row[1]).exists()
    assert Path(row[1]).read_bytes() == b"recover-bytes"


def test_startup_recovery_copy_failure_persists_retry_and_error_events(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    missing_source = tmp_path / "missing" / "recover-miss.jpg"
    app = create_app(db_path=db_path, staging_root=tmp_path / "staging")

    _seed_job_and_file(db_path, "DISCOVERED", 31, source_path=str(missing_source))
    with TestClient(app) as client:
        state_response = client.get("/state")
        events_response = client.get("/events")

    assert state_response.status_code == 200
    assert state_response.json()["current_state"] == "STAGING_COPY"
    assert events_response.status_code == 200
    assert any(item["category"] == "COPY_RETRY_SCHEDULED" for item in events_response.json()["events"])
    assert any(item["category"] == "RECOVERY_STOPPED_ERROR" for item in events_response.json()["events"])

    conn = open_db(db_path)
    row = conn.execute(
        "SELECT status, retry_count, last_error FROM ingest_files WHERE id = 31;"
    ).fetchone()
    conn.close()
    assert row is not None
    assert row[0] == "NEEDS_RETRY_COPY"
    assert row[1] == 1
    assert row[2]


def test_startup_recovery_hash_failure_persists_retry_and_error_events(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    _seed_job_and_file(db_path, "STAGED", 41, source_path="/media/sd/41.jpg")
    conn = open_db(db_path)
    conn.execute("UPDATE ingest_files SET staged_path = '/missing/staged-41.jpg' WHERE id = 41;")
    conn.commit()
    conn.close()

    app = create_app(db_path=db_path, staging_root=tmp_path / "staging")
    with TestClient(app) as client:
        state_response = client.get("/state")
        events_response = client.get("/events")

    assert state_response.status_code == 200
    assert state_response.json()["current_state"] == "HASHING"
    assert events_response.status_code == 200
    assert any(item["category"] == "HASH_RETRY_SCHEDULED" for item in events_response.json()["events"])
    assert any(item["category"] == "RECOVERY_STOPPED_ERROR" for item in events_response.json()["events"])

    conn = open_db(db_path)
    row = conn.execute(
        "SELECT status, retry_count, last_error FROM ingest_files WHERE id = 41;"
    ).fetchone()
    conn.close()
    assert row is not None
    assert row[0] == "NEEDS_RETRY_HASH"
    assert row[1] == 1
    assert row[2]


def test_reboot_recovery_retries_copy_after_source_becomes_available(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    source = tmp_path / "media" / "sd" / "late.jpg"
    staging_root = tmp_path / "staging"

    _seed_job_and_file(db_path, "DISCOVERED", 51, source_path=str(source))

    app_first = create_app(db_path=db_path, staging_root=staging_root)
    with TestClient(app_first):
        pass

    conn = open_db(db_path)
    first_row = conn.execute(
        "SELECT status, retry_count FROM ingest_files WHERE id = 51;"
    ).fetchone()
    conn.close()
    assert first_row is not None
    assert first_row[0] == "NEEDS_RETRY_COPY"
    assert first_row[1] == 1

    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(b"late-arrival")

    app_second = create_app(db_path=db_path, staging_root=staging_root)
    with TestClient(app_second):
        pass

    conn = open_db(db_path)
    second_row = conn.execute(
        "SELECT status, retry_count, staged_path, sha256_hex FROM ingest_files WHERE id = 51;"
    ).fetchone()
    state_row = conn.execute("SELECT current_state FROM daemon_state WHERE id = 1;").fetchone()
    conn.close()
    assert second_row is not None
    assert second_row[0] == "READY_TO_UPLOAD"
    assert second_row[1] == 1
    assert Path(second_row[2]).exists()
    assert isinstance(second_row[3], str)
    assert len(second_row[3]) == 64
    assert state_row is not None
    assert state_row[0] == "WAIT_NETWORK"


def test_reboot_recovery_retries_hash_after_staged_file_becomes_available(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    staged_path = tmp_path / "staging" / "job-1" / "61-late-hash.jpg"
    _seed_job_and_file(db_path, "STAGED", 61, source_path="/media/sd/61.jpg")
    conn = open_db(db_path)
    conn.execute("UPDATE ingest_files SET staged_path = ? WHERE id = 61;", (str(staged_path),))
    conn.commit()
    conn.close()

    app_first = create_app(db_path=db_path, staging_root=tmp_path / "staging")
    with TestClient(app_first):
        pass

    conn = open_db(db_path)
    first_row = conn.execute(
        "SELECT status, retry_count FROM ingest_files WHERE id = 61;"
    ).fetchone()
    conn.close()
    assert first_row is not None
    assert first_row[0] == "NEEDS_RETRY_HASH"
    assert first_row[1] == 1

    staged_path.parent.mkdir(parents=True, exist_ok=True)
    staged_path.write_bytes(b"late-hash-bytes")

    app_second = create_app(db_path=db_path, staging_root=tmp_path / "staging")
    with TestClient(app_second):
        pass

    conn = open_db(db_path)
    second_row = conn.execute(
        "SELECT status, retry_count, sha256_hex FROM ingest_files WHERE id = 61;"
    ).fetchone()
    state_row = conn.execute("SELECT current_state FROM daemon_state WHERE id = 1;").fetchone()
    conn.close()
    assert second_row is not None
    assert second_row[0] == "READY_TO_UPLOAD"
    assert second_row[1] == 1
    assert isinstance(second_row[2], str)
    assert len(second_row[2]) == 64
    assert state_row is not None
    assert state_row[0] == "WAIT_NETWORK"
