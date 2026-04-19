from pathlib import Path

from fastapi.testclient import TestClient
from photovault_clientd.app import create_app
from photovault_clientd.db import open_db


def _write_source_file(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def test_ingest_job_creation_moves_daemon_to_staging_copy(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    app = create_app(db_path=db_path)

    with TestClient(app) as client:
        response = client.post(
            "/ingest/jobs",
            json={
                "media_label": "sd-001",
                "source_paths": ["/media/sd/a.jpg", "/media/sd/b.jpg"],
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert body["discovered_count"] == 2
        assert body["state"] == "STAGING_COPY"

        state_response = client.get("/state")
        assert state_response.status_code == 200
        assert state_response.json()["current_state"] == "STAGING_COPY"

    conn = open_db(db_path)
    job_row = conn.execute("SELECT status FROM ingest_jobs WHERE id = ?", (body["job_id"],)).fetchone()
    discovered_rows = conn.execute(
        "SELECT COUNT(1) FROM ingest_files WHERE job_id = ? AND status = 'DISCOVERED'",
        (body["job_id"],),
    ).fetchone()
    conn.close()

    assert job_row is not None
    assert job_row[0] == "STAGING_COPY"
    assert discovered_rows is not None
    assert discovered_rows[0] == 2


def test_stage_next_copies_file_marks_staged_and_returns_hashing_next_state(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    source_root = tmp_path / "media" / "sd"
    source_one = source_root / "001.jpg"
    source_two = source_root / "002.jpg"
    _write_source_file(source_one, b"first")
    _write_source_file(source_two, b"second")

    app = create_app(db_path=db_path)
    staging_root = tmp_path / "staging"

    with TestClient(app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={
                "media_label": "sd-002",
                "source_paths": [str(source_one), str(source_two)],
            },
        )
        assert create_response.status_code == 200
        job_id = create_response.json()["job_id"]

        first_stage = client.post(
            "/ingest/staging/next",
            json={"job_id": job_id, "staging_root": str(staging_root)},
        )
        assert first_stage.status_code == 200
        first_body = first_stage.json()
        assert first_body["pending_copy"] == 1
        assert first_body["staged"] == 1
        assert first_body["hash_pending"] == 1
        assert first_body["next_state"] == "STAGING_COPY"
        copied_path = Path(first_body["copied_file"]["staged_path"])
        assert copied_path.exists()
        assert copied_path.read_bytes() == b"first"

        second_stage = client.post(
            "/ingest/staging/next",
            json={"job_id": job_id, "staging_root": str(staging_root)},
        )
        assert second_stage.status_code == 200
        second_body = second_stage.json()
        assert second_body["pending_copy"] == 0
        assert second_body["staged"] == 2
        assert second_body["hash_pending"] == 2
        assert second_body["next_state"] == "HASHING"
        assert Path(second_body["copied_file"]["staged_path"]).read_bytes() == b"second"

        state_response = client.get("/state")
        assert state_response.status_code == 200
        assert state_response.json()["current_state"] == "HASHING"

        empty_stage = client.post(
            "/ingest/staging/next",
            json={"job_id": job_id, "staging_root": str(staging_root)},
        )
        assert empty_stage.status_code == 409


def test_stage_next_persists_retry_fields_when_copy_fails(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    missing_source = tmp_path / "missing" / "ghost.jpg"
    app = create_app(db_path=db_path)

    with TestClient(app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={
                "media_label": "sd-err",
                "source_paths": [str(missing_source)],
            },
        )
        assert create_response.status_code == 200
        job_id = create_response.json()["job_id"]

        stage_response = client.post(
            "/ingest/staging/next",
            json={"job_id": job_id, "staging_root": str(tmp_path / "staging")},
        )
        assert stage_response.status_code == 200
        body = stage_response.json()
        assert body["copied_file"] is None
        assert body["retry_scheduled"] is True
        assert body["pending_copy"] == 1
        assert body["staged"] == 0
        assert body["hash_pending"] == 0
        assert body["next_state"] == "STAGING_COPY"

        state_response = client.get("/state")
        assert state_response.status_code == 200
        assert state_response.json()["current_state"] == "STAGING_COPY"
        events_response = client.get("/events")
        assert events_response.status_code == 200
        assert any(
            item["category"] == "COPY_RETRY_SCHEDULED" and item["message"].startswith("COPY_SOURCE_MISSING:")
            for item in events_response.json()["events"]
        )

    conn = open_db(db_path)
    row = conn.execute(
        "SELECT status, retry_count, last_error FROM ingest_files WHERE job_id = ?",
        (job_id,),
    ).fetchone()
    conn.close()

    assert row is not None
    assert row[0] == "NEEDS_RETRY_COPY"
    assert row[1] == 1
    assert row[2]


def test_ingest_create_is_rejected_when_daemon_not_idle(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    app = create_app(db_path=db_path)

    with TestClient(app) as client:
        first = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-003", "source_paths": ["/media/sd/x.jpg"]},
        )
        assert first.status_code == 200

        second = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-004", "source_paths": ["/media/sd/y.jpg"]},
        )
        assert second.status_code == 409


def test_stage_next_is_rejected_when_not_in_staging_copy(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    app = create_app(db_path=db_path)

    with TestClient(app) as client:
        response = client.post(
            "/ingest/staging/next",
            json={"job_id": 1, "staging_root": str(tmp_path / "staging")},
        )
        assert response.status_code == 409


def test_stage_next_returns_not_found_for_unknown_job(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    app = create_app(db_path=db_path)

    with TestClient(app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-known", "source_paths": ["/media/sd/known.jpg"]},
        )
        assert create_response.status_code == 200
        response = client.post(
            "/ingest/staging/next",
            json={"job_id": 999, "staging_root": str(tmp_path / "staging")},
        )
        assert response.status_code == 404
