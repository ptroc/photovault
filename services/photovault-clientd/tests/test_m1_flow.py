from datetime import UTC, datetime
from pathlib import Path

from fastapi.testclient import TestClient
from photovault_clientd.app import create_app
from photovault_clientd.db import open_db, register_local_sha


def _write_source_file(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _tick_until_state(client: TestClient, target_state: str, max_steps: int = 20) -> None:
    for _ in range(max_steps):
        state_response = client.get("/state")
        assert state_response.status_code == 200
        if state_response.json()["current_state"] == target_state:
            return
        tick_response = client.post("/daemon/tick")
        assert tick_response.status_code == 200
    raise AssertionError(f"daemon did not reach state {target_state} within {max_steps} ticks")


def _seed_local_registry(db_path: Path, sha256_hex: str) -> None:
    conn = open_db(db_path)
    now = datetime.now(UTC).isoformat()
    conn.execute(
        """
        INSERT INTO ingest_jobs (id, media_label, status, created_at_utc, updated_at_utc)
        VALUES (999, 'historical', 'JOB_COMPLETE_LOCAL', ?, ?)
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
        VALUES (
            999, 999, '/historical/file.jpg', '/historical/file.jpg',
            ?, 10, 'DUPLICATE_SHA_LOCAL', 0, NULL, ?, ?
        )
        ON CONFLICT(id) DO NOTHING;
        """,
        (sha256_hex, now, now),
    )
    register_local_sha(conn, sha256_hex, 999, 999, now)
    conn.commit()
    conn.close()


def test_offline_ingest_applies_session_and_local_dedup_then_waits_for_network(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    staging_root = tmp_path / "staging"
    media_root = tmp_path / "media" / "sd"
    unique_file = media_root / "001.jpg"
    session_duplicate = media_root / "002.jpg"
    historical_duplicate = media_root / "003.jpg"

    _write_source_file(unique_file, b"unique-bytes")
    _write_source_file(session_duplicate, b"unique-bytes")
    _write_source_file(historical_duplicate, b"historical-bytes")

    from photovault_clientd.hashing import compute_sha256

    historical_sha, _ = compute_sha256(historical_duplicate)
    _seed_local_registry(db_path, historical_sha)

    app = create_app(db_path=db_path, staging_root=staging_root)
    with TestClient(app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={
                "media_label": "sd-m1",
                "source_paths": [str(unique_file), str(session_duplicate), str(historical_duplicate)],
            },
        )
        assert create_response.status_code == 200
        job_id = create_response.json()["job_id"]

        _tick_until_state(client, "WAIT_NETWORK")

        detail_response = client.get(f"/ingest/jobs/{job_id}")
        assert detail_response.status_code == 200
        detail = detail_response.json()

        assert detail["status"] == "WAIT_NETWORK"
        assert detail["local_ingest_complete"] is True
        assert detail["upload_pending"] is True
        assert detail["status_counts"]["READY_TO_UPLOAD"] == 1
        assert detail["status_counts"]["DUPLICATE_SESSION_SHA"] == 1
        assert detail["status_counts"]["DUPLICATE_SHA_LOCAL"] == 1

        statuses = {item["source_path"]: item["status"] for item in detail["files"]}
        assert statuses[str(unique_file)] == "READY_TO_UPLOAD"
        assert statuses[str(session_duplicate)] == "DUPLICATE_SESSION_SHA"
        assert statuses[str(historical_duplicate)] == "DUPLICATE_SHA_LOCAL"

        jobs_response = client.get("/ingest/jobs")
        assert jobs_response.status_code == 200
        jobs_body = jobs_response.json()
        assert jobs_body["count"] >= 1
        assert any(job["job_id"] == job_id and job["upload_pending"] is True for job in jobs_body["jobs"])


def test_duplicate_only_job_finishes_locally_and_returns_to_idle(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    staging_root = tmp_path / "staging"
    media_root = tmp_path / "media" / "sd"
    source_one = media_root / "001.jpg"
    source_two = media_root / "002.jpg"

    _write_source_file(source_one, b"duplicate-bytes")
    _write_source_file(source_two, b"duplicate-bytes")

    from photovault_clientd.hashing import compute_sha256

    duplicate_sha, _ = compute_sha256(source_one)
    _seed_local_registry(db_path, duplicate_sha)

    app = create_app(db_path=db_path, staging_root=staging_root)
    with TestClient(app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={
                "media_label": "sd-duplicate-only",
                "source_paths": [str(source_one), str(source_two)],
            },
        )
        assert create_response.status_code == 200
        job_id = create_response.json()["job_id"]

        _tick_until_state(client, "IDLE")

        detail_response = client.get(f"/ingest/jobs/{job_id}")
        assert detail_response.status_code == 200
        detail = detail_response.json()
        assert detail["status"] == "JOB_COMPLETE_LOCAL"
        assert detail["local_ingest_complete"] is True
        assert detail["upload_pending"] is False
        assert detail["status_counts"]["DUPLICATE_SESSION_SHA"] == 1
        assert detail["status_counts"]["DUPLICATE_SHA_LOCAL"] == 1

        events_response = client.get("/events")
        assert events_response.status_code == 200
        assert any(
            event["category"] == "JOB_LOCAL_COMPLETED" for event in events_response.json()["events"]
        )


def test_recovery_resumes_dedup_local_phase_and_finishes_locally(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    conn = open_db(db_path)
    now = datetime.now(UTC).isoformat()
    sha256_hex = "a" * 64
    conn.execute(
        """
        INSERT INTO ingest_jobs (id, media_label, status, created_at_utc, updated_at_utc)
        VALUES (1, 'recover-local', 'DEDUP_LOCAL_SHA', ?, ?);
        """,
        (now, now),
    )
    conn.execute(
        """
        INSERT INTO ingest_files (
            id, job_id, source_path, staged_path, sha256_hex, size_bytes,
            status, retry_count, last_error, created_at_utc, updated_at_utc
        )
        VALUES (1, 1, '/media/sd/local.jpg', '/staging/local.jpg', ?, 10, 'HASHED', 0, NULL, ?, ?);
        """,
        (sha256_hex, now, now),
    )
    register_local_sha(conn, sha256_hex, 1, 1, now)
    conn.commit()
    conn.close()

    app = create_app(db_path=db_path, staging_root=tmp_path / "staging")
    with TestClient(app) as client:
        state_response = client.get("/state")
        detail_response = client.get("/ingest/jobs/1")

    assert state_response.status_code == 200
    assert state_response.json()["current_state"] == "IDLE"
    assert detail_response.status_code == 200
    assert detail_response.json()["status"] == "JOB_COMPLETE_LOCAL"
    assert detail_response.json()["status_counts"]["DUPLICATE_SHA_LOCAL"] == 1


def test_recovery_resumes_queue_upload_phase_and_stops_at_wait_network(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    conn = open_db(db_path)
    now = datetime.now(UTC).isoformat()
    sha256_hex = "b" * 64
    conn.execute(
        """
        INSERT INTO ingest_jobs (id, media_label, status, created_at_utc, updated_at_utc)
        VALUES (1, 'recover-queue', 'QUEUE_UPLOAD', ?, ?);
        """,
        (now, now),
    )
    conn.execute(
        """
        INSERT INTO ingest_files (
            id, job_id, source_path, staged_path, sha256_hex, size_bytes,
            status, retry_count, last_error, created_at_utc, updated_at_utc
        )
        VALUES (1, 1, '/media/sd/queue.jpg', '/staging/queue.jpg', ?, 10, 'HASHED', 0, NULL, ?, ?);
        """,
        (sha256_hex, now, now),
    )
    conn.commit()
    conn.close()

    app = create_app(db_path=db_path, staging_root=tmp_path / "staging")
    with TestClient(app) as client:
        state_response = client.get("/state")
        detail_response = client.get("/ingest/jobs/1")

    assert state_response.status_code == 200
    assert state_response.json()["current_state"] == "WAIT_NETWORK"
    assert detail_response.status_code == 200
    assert detail_response.json()["status"] == "WAIT_NETWORK"
    assert detail_response.json()["status_counts"]["READY_TO_UPLOAD"] == 1
