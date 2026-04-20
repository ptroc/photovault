from datetime import UTC, datetime
from pathlib import Path

from fastapi.testclient import TestClient
from photovault_clientd.app import create_app
from photovault_clientd.db import open_db


def _write_source_file(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def test_ingest_job_creation_moves_daemon_to_staging_copy(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    source_one = tmp_path / "media" / "sd" / "a.jpg"
    source_two = tmp_path / "media" / "sd" / "b.jpg"
    _write_source_file(source_one, b"a")
    _write_source_file(source_two, b"b")
    app = create_app(db_path=db_path)

    with TestClient(app) as client:
        response = client.post(
            "/ingest/jobs",
            json={
                "media_label": "sd-001",
                "source_paths": [str(source_one), str(source_two)],
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
    source = tmp_path / "media" / "sd" / "ghost.jpg"
    _write_source_file(source, b"ghost")
    app = create_app(db_path=db_path)

    with TestClient(app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={
                "media_label": "sd-err",
                "source_paths": [str(source)],
            },
        )
        assert create_response.status_code == 200
        job_id = create_response.json()["job_id"]
        source.unlink()

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
    source_one = tmp_path / "media" / "sd" / "x.jpg"
    source_two = tmp_path / "media" / "sd" / "y.jpg"
    _write_source_file(source_one, b"x")
    _write_source_file(source_two, b"y")
    app = create_app(db_path=db_path)

    with TestClient(app) as client:
        first = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-003", "source_paths": [str(source_one)]},
        )
        assert first.status_code == 200

        second = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-004", "source_paths": [str(source_two)]},
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
    source = tmp_path / "media" / "sd" / "known.jpg"
    _write_source_file(source, b"known")
    app = create_app(db_path=db_path)

    with TestClient(app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-known", "source_paths": [str(source)]},
        )
        assert create_response.status_code == 200
        response = client.post(
            "/ingest/staging/next",
            json={"job_id": 999, "staging_root": str(tmp_path / "staging")},
        )
        assert response.status_code == 404


def test_ingest_job_creation_enumerates_directory_source_path(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    media_root = tmp_path / "mnt" / "usb"
    _write_source_file(media_root / "a.jpg", b"a")
    _write_source_file(media_root / "nested" / "b.jpg", b"b")
    app = create_app(db_path=db_path)

    with TestClient(app) as client:
        response = client.post(
            "/ingest/jobs",
            json={"media_label": "usb-root", "source_paths": [str(media_root)]},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["discovered_count"] == 2

    conn = open_db(db_path)
    rows = conn.execute(
        "SELECT source_path FROM ingest_files WHERE job_id = ? ORDER BY source_path ASC",
        (body["job_id"],),
    ).fetchall()
    conn.close()

    assert [row[0] for row in rows] == [str(media_root / "a.jpg"), str(media_root / "nested" / "b.jpg")]


def test_ingest_job_creation_filters_junk_and_disallowed_directory_files(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    media_root = tmp_path / "mnt" / "usb"
    _write_source_file(media_root / "photos" / "a.jpg", b"a")
    _write_source_file(media_root / "photos" / "clip.mp4", b"clip")
    _write_source_file(media_root / "notes.txt", b"notes")
    _write_source_file(media_root / ".DS_Store", b"junk")
    _write_source_file(media_root / ".Spotlight-V100" / "store.db", b"db")
    _write_source_file(media_root / ".fseventsd" / "uuid", b"id")
    app = create_app(db_path=db_path)

    with TestClient(app) as client:
        response = client.post(
            "/ingest/jobs",
            json={"media_label": "usb-root", "source_paths": [str(media_root)]},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["discovered_count"] == 2
        assert body["filtered_count"] == 2
        assert any(item["source_path"].endswith("notes.txt") for item in body["filtered_sources"])

    conn = open_db(db_path)
    rows = conn.execute(
        "SELECT source_path FROM ingest_files WHERE job_id = ? ORDER BY source_path ASC",
        (body["job_id"],),
    ).fetchall()
    conn.close()

    assert [row[0] for row in rows] == [
        str(media_root / "photos" / "a.jpg"),
        str(media_root / "photos" / "clip.mp4"),
    ]


def test_ingest_create_rejects_direct_file_with_disallowed_extension(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    source = tmp_path / "mnt" / "usb" / "notes.txt"
    _write_source_file(source, b"notes")
    app = create_app(db_path=db_path)

    with TestClient(app) as client:
        response = client.post(
            "/ingest/jobs",
            json={"media_label": "usb-root", "source_paths": [str(source)]},
        )
        assert response.status_code == 422
        detail = response.json()["detail"]
        assert detail["code"] == "INGEST_SOURCE_PATH_INVALID"
        assert detail["invalid_sources"] == [
            {
                "source_path": str(source),
                "reason": (
                    "File is not allowed by the v1 ingest policy. Supported extensions include common "
                    "photo, RAW, and video formats; got .txt."
                ),
            }
        ]


def test_ingest_create_rejects_invalid_source_paths_atomically(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    media_root = tmp_path / "mnt" / "usb"
    _write_source_file(media_root / "a.jpg", b"a")
    missing_source = tmp_path / "mnt" / "usb" / "missing.jpg"
    app = create_app(db_path=db_path)

    with TestClient(app) as client:
        response = client.post(
            "/ingest/jobs",
            json={"media_label": "usb-root", "source_paths": [str(media_root), str(missing_source)]},
        )
        assert response.status_code == 422
        detail = response.json()["detail"]
        assert detail["code"] == "INGEST_SOURCE_PATH_INVALID"
        assert detail["message"].startswith("One or more source paths")
        assert detail["invalid_sources"] == [
            {"source_path": str(missing_source), "reason": "Path does not exist."}
        ]

        state_response = client.get("/state")
        assert state_response.status_code == 200
        assert state_response.json()["current_state"] == "IDLE"

    conn = open_db(db_path)
    jobs_count = conn.execute("SELECT COUNT(1) FROM ingest_jobs;").fetchone()
    files_count = conn.execute("SELECT COUNT(1) FROM ingest_files;").fetchone()
    conn.close()

    assert jobs_count is not None
    assert files_count is not None
    assert jobs_count[0] == 0
    assert files_count[0] == 0


def test_daemon_tick_repairs_legacy_directory_copy_candidate(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    media_root = tmp_path / "mnt" / "usb"
    _write_source_file(media_root / "a.jpg", b"a")
    _write_source_file(media_root / "nested" / "b.jpg", b"b")
    app = create_app(db_path=db_path, staging_root=tmp_path / "staging")

    conn = open_db(db_path)
    now = datetime.now(UTC).isoformat()
    conn.execute(
        """
        INSERT INTO ingest_jobs (id, media_label, status, created_at_utc, updated_at_utc)
        VALUES (7, 'usb_test_1', 'STAGING_COPY', ?, ?);
        """,
        (now, now),
    )
    conn.execute(
        """
        INSERT INTO ingest_files (
            id, job_id, source_path, staged_path, sha256_hex, size_bytes,
            status, retry_count, last_error, created_at_utc, updated_at_utc
        )
        VALUES (?, 7, ?, NULL, NULL, NULL, 'NEEDS_RETRY_COPY', 11, ?, ?, ?);
        """,
        (12, str(media_root), "[Errno 21] Is a directory", now, now),
    )
    conn.execute(
        """
        INSERT INTO daemon_state (id, current_state, updated_at_utc)
        VALUES (1, 'STAGING_COPY', ?)
        ON CONFLICT(id) DO UPDATE
        SET current_state=excluded.current_state,
            updated_at_utc=excluded.updated_at_utc;
        """,
        (now,),
    )
    conn.commit()
    conn.close()

    with TestClient(app) as client:
        tick_response = client.post("/daemon/tick")
        assert tick_response.status_code == 200
        tick_body = tick_response.json()
        assert tick_body["handled"] is True
        assert tick_body["errored"] is False

        detail_response = client.get("/ingest/jobs/7")
        assert detail_response.status_code == 200
        detail = detail_response.json()
        source_paths = sorted(file_row["source_path"] for file_row in detail["files"])
        assert str(media_root / "a.jpg") in source_paths
        assert str(media_root / "nested" / "b.jpg") in source_paths
        assert all(file_row["source_path"] != str(media_root) for file_row in detail["files"])
