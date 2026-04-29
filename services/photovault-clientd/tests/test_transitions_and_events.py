import logging
from datetime import UTC, datetime
from pathlib import Path

import photovault_clientd.app as app_module
import pytest
from fastapi.testclient import TestClient
from photovault_clientd.app import create_app
from photovault_clientd.db import (
    append_daemon_event,
    fetch_recent_daemon_events,
    open_db,
    set_daemon_state,
    transition_daemon_state,
)
from photovault_clientd.events import EventCategory, EventLevel
from photovault_clientd.state_machine import ClientState


def test_transition_violation_is_logged_and_state_is_unchanged(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    conn = open_db(db_path)
    now = datetime.now(UTC).isoformat()

    transition_daemon_state(conn, ClientState.BOOTSTRAP, now, reason="test setup")

    with pytest.raises(ValueError):
        transition_daemon_state(
            conn,
            ClientState.UPLOAD_FILE,
            datetime.now(UTC).isoformat(),
            reason="invalid test transition",
        )

    state_row = conn.execute("SELECT current_state FROM daemon_state WHERE id = 1;").fetchone()
    events = fetch_recent_daemon_events(conn, limit=5)
    conn.close()

    assert state_row is not None
    assert state_row[0] == ClientState.BOOTSTRAP.value
    assert any(e["category"] == "TRANSITION_VIOLATION" for e in events)


def test_bootstrap_transition_is_allowed_from_any_persisted_state(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    conn = open_db(db_path)
    now = datetime.now(UTC).isoformat()

    transition_daemon_state(conn, ClientState.BOOTSTRAP, now, reason="initial startup")
    transition_daemon_state(conn, ClientState.IDLE, now, reason="idle")
    transition_daemon_state(conn, ClientState.DISCOVERING, now, reason="discovering")
    transition_daemon_state(conn, ClientState.STAGING_COPY, now, reason="staging")

    # Simulated reboot from non-idle persisted state.
    transition_daemon_state(conn, ClientState.BOOTSTRAP, datetime.now(UTC).isoformat(), reason="reboot")
    state_row = conn.execute("SELECT current_state FROM daemon_state WHERE id = 1;").fetchone()
    conn.close()

    assert state_row is not None
    assert state_row[0] == ClientState.BOOTSTRAP.value


def test_events_endpoint_returns_bootstrap_transition_events(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    app = create_app(db_path=db_path)

    with TestClient(app) as client:
        response = client.get("/events")

    assert response.status_code == 200
    body = response.json()
    assert body["count"] >= 2
    assert any(item["category"] == "STATE_TRANSITION" for item in body["events"])


def test_daemon_tick_processes_staging_copy_work(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    staging_root = tmp_path / "staging"
    source = tmp_path / "media" / "sd" / "tick.jpg"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(b"tick-data")
    app = create_app(db_path=db_path, staging_root=staging_root)

    with TestClient(app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-tick", "source_paths": [str(source)]},
        )
        assert create_response.status_code == 200
        tick_response = client.post("/daemon/tick")
        assert tick_response.status_code == 200
        tick_body = tick_response.json()
        assert tick_body["handled"] is True
        assert tick_body["next_state"] == "HASHING"
        assert tick_body["progressed"] is True


def test_daemon_tick_processes_hashing_work(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    staged_file = tmp_path / "staging" / "job-1" / "1-a.jpg"
    staged_file.parent.mkdir(parents=True, exist_ok=True)
    staged_file.write_bytes(b"hash-me")
    app = create_app(db_path=db_path, staging_root=tmp_path / "staging")

    with TestClient(app) as client:
        conn = open_db(db_path)
        now = datetime.now(UTC).isoformat()
        conn.execute(
            """
            INSERT INTO ingest_jobs (id, media_label, status, created_at_utc, updated_at_utc)
            VALUES (1, 'sd-hash', 'HASHING', ?, ?)
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
            VALUES (1, 1, '/media/sd/a.jpg', ?, NULL, ?, 'STAGED', 0, NULL, ?, ?)
            ON CONFLICT(id) DO NOTHING;
            """,
            (str(staged_file), len(b"hash-me"), now, now),
        )
        set_daemon_state(conn, ClientState.HASHING, now)
        conn.close()

        tick_response = client.post("/daemon/tick")
        assert tick_response.status_code == 200
        tick_body = tick_response.json()
        assert tick_body["handled"] is True
        assert tick_body["next_state"] == "DEDUP_SESSION_SHA"
        assert tick_body["progressed"] is True

        conn = open_db(db_path)
        row = conn.execute("SELECT status, sha256_hex FROM ingest_files WHERE id = 1;").fetchone()
        conn.close()
        assert row is not None
        assert row[0] == "HASHED"
        assert isinstance(row[1], str)
        assert len(row[1]) == 64


def test_daemon_tick_hashing_failure_schedules_retry_and_classified_event(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    app = create_app(db_path=db_path, staging_root=tmp_path / "staging")

    with TestClient(app) as client:
        conn = open_db(db_path)
        now = datetime.now(UTC).isoformat()
        conn.execute(
            """
            INSERT INTO ingest_jobs (id, media_label, status, created_at_utc, updated_at_utc)
            VALUES (1, 'sd-hash-fail', 'HASHING', ?, ?)
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
            VALUES (1, 1, '/media/sd/missing.jpg', '/missing/staged.jpg', NULL, 0, 'STAGED', 0, NULL, ?, ?)
            ON CONFLICT(id) DO NOTHING;
            """,
            (now, now),
        )
        set_daemon_state(conn, ClientState.HASHING, now)
        conn.close()

        tick_response = client.post("/daemon/tick")
        assert tick_response.status_code == 200
        tick_body = tick_response.json()
        assert tick_body["handled"] is True
        assert tick_body["errored"] is True
        assert tick_body["next_state"] == "HASHING"

        conn = open_db(db_path)
        file_row = conn.execute(
            "SELECT status, retry_count, last_error FROM ingest_files WHERE id = 1;"
        ).fetchone()
        events = fetch_recent_daemon_events(conn, limit=20)
        conn.close()

        assert file_row is not None
        assert file_row[0] == "NEEDS_RETRY_HASH"
        assert file_row[1] == 1
        assert file_row[2]
        assert any(
            event["category"] == "HASH_RETRY_SCHEDULED"
            and event["message"].startswith("HASH_SOURCE_MISSING:")
            for event in events
        )


def test_create_ingest_logs_job_id_and_media_label(tmp_path: Path, caplog) -> None:
    db_path = tmp_path / "state.sqlite3"
    staging_root = tmp_path / "staging"
    source = tmp_path / "media" / "sd" / "ingest-log.jpg"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(b"log-data")
    app = create_app(db_path=db_path, staging_root=staging_root)

    caplog.set_level(logging.INFO, logger="photovault-clientd.app")
    with TestClient(app) as client:
        response = client.post(
            "/ingest/jobs",
            json={"media_label": "Card-A", "source_paths": [str(source)]},
        )

    assert response.status_code == 200
    job_id = response.json()["job_id"]
    assert "ingest_job_created" in caplog.text
    assert f"job_id={job_id}" in caplog.text
    assert "media_label=Card-A" in caplog.text


def test_daemon_tick_unhandled_exception_returns_structured_500(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "state.sqlite3"
    app = create_app(db_path=db_path, staging_root=tmp_path / "staging")

    def _raise_unhandled(*_args, **_kwargs):
        raise RuntimeError("forced daemon tick failure")

    monkeypatch.setattr(app_module, "run_daemon_tick", _raise_unhandled)
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.post("/daemon/tick")

    assert response.status_code == 500
    detail = response.json()["detail"]
    assert detail["request_id"]
    assert detail["timestamp_utc"]
    assert detail["message"] == "forced daemon tick failure"
    assert isinstance(detail["traceback"], list)
    assert any("forced daemon tick failure" in line for line in detail["traceback"])


def test_append_daemon_event_is_mirrored_to_process_logs(tmp_path: Path, caplog) -> None:
    db_path = tmp_path / "state.sqlite3"
    conn = open_db(db_path)
    now = datetime.now(UTC).isoformat()
    caplog.set_level(logging.INFO, logger="photovault-clientd.daemon_events")
    append_daemon_event(
        conn,
        level=EventLevel.INFO,
        category=EventCategory.TICK_NOOP,
        message="mirror-check",
        created_at_utc=now,
        from_state=ClientState.IDLE,
        to_state=ClientState.IDLE,
    )
    conn.close()

    daemon_event_messages = [
        record.message for record in caplog.records if record.name == "photovault-clientd.daemon_events"
    ]
    assert any(
        "category=TICK_NOOP" in message and "message=mirror-check" in message
        for message in daemon_event_messages
    )
