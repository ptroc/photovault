import hashlib
import sqlite3
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.error import URLError

import pytest
from fastapi.testclient import TestClient
from photovault_api.app import create_app as create_api_app
from photovault_api.state_store import InMemoryUploadStateStore
from photovault_clientd import engine
from photovault_clientd.app import create_app

ORIGINAL_NETWORK_IS_ONLINE = engine._network_is_online


@pytest.fixture(autouse=True)
def _default_network_online(monkeypatch):
    monkeypatch.setattr(engine, "_network_is_online", lambda: True)


def _write_source_file(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _tick_until_state(client: TestClient, target_state: str, max_steps: int = 30) -> None:
    for _ in range(max_steps):
        state_response = client.get("/state")
        assert state_response.status_code == 200
        if state_response.json()["current_state"] == target_state:
            return
        tick_response = client.post("/daemon/tick")
        assert tick_response.status_code == 200
    raise AssertionError(f"daemon did not reach state {target_state} within {max_steps} ticks")


def _advance_wait_network_handshake(client: TestClient) -> None:
    tick_response = client.post("/daemon/tick")
    assert tick_response.status_code == 200
    assert tick_response.json()["next_state"] == "UPLOAD_PREPARE"

    tick_response = client.post("/daemon/tick")
    assert tick_response.status_code == 200


def _advance_to_upload_file(client: TestClient) -> None:
    _advance_wait_network_handshake(client)
    state_response = client.get("/state")
    assert state_response.status_code == 200
    assert state_response.json()["current_state"] == "UPLOAD_FILE"


def _drain_ticks_until_state(client: TestClient, target_state: str, max_steps: int = 20) -> None:
    for _ in range(max_steps):
        state_response = client.get("/state")
        assert state_response.status_code == 200
        if state_response.json()["current_state"] == target_state:
            return
        tick_response = client.post("/daemon/tick")
        assert tick_response.status_code == 200
    raise AssertionError(f"daemon did not reach state {target_state} within {max_steps} ticks")


def _age_status_rows(db_path: Path, *, status: str, seconds: int = 120) -> None:
    aged_time = (datetime.now(UTC) - timedelta(seconds=seconds)).isoformat()
    conn = sqlite3.connect(db_path)
    conn.execute(
        "UPDATE ingest_files SET updated_at_utc = ? WHERE status = ?;",
        (aged_time, status),
    )
    conn.commit()
    conn.close()


def test_wait_network_handshake_marks_server_existing_file_as_duplicate_global(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "state.sqlite3"
    staging_root = tmp_path / "staging"
    source = tmp_path / "media" / "sd" / "already.jpg"
    _write_source_file(source, b"already-on-server")

    def fake_handshake(*, server_base_url: str, files: list[dict[str, object]], timeout_seconds: float = 5.0):
        return {int(item["file_id"]): "ALREADY_EXISTS" for item in files}

    monkeypatch.setattr(engine, "_post_metadata_handshake", fake_handshake)

    app = create_app(db_path=db_path, staging_root=staging_root, server_base_url="http://fake")
    with TestClient(app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-m2-exists", "source_paths": [str(source)]},
        )
        assert create_response.status_code == 200
        job_id = int(create_response.json()["job_id"])

        _tick_until_state(client, "WAIT_NETWORK")
        _advance_wait_network_handshake(client)

        detail_response = client.get(f"/ingest/jobs/{job_id}")
        assert detail_response.status_code == 200
        detail = detail_response.json()
        assert detail["status_counts"]["DUPLICATE_SHA_GLOBAL"] == 1
        assert detail["upload_pending"] is False

        events_response = client.get("/events")
        assert events_response.status_code == 200
        assert any(
            event["category"] == "HANDSHAKE_CLASSIFIED"
            for event in events_response.json()["events"]
        )


def test_wait_network_handshake_keeps_upload_required_file_ready_and_moves_to_upload_file(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "state.sqlite3"
    staging_root = tmp_path / "staging"
    source = tmp_path / "media" / "sd" / "required.jpg"
    _write_source_file(source, b"needs-upload")

    def fake_handshake(*, server_base_url: str, files: list[dict[str, object]], timeout_seconds: float = 5.0):
        return {int(item["file_id"]): "UPLOAD_REQUIRED" for item in files}

    monkeypatch.setattr(engine, "_post_metadata_handshake", fake_handshake)

    app = create_app(db_path=db_path, staging_root=staging_root, server_base_url="http://fake")
    with TestClient(app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-m2-required", "source_paths": [str(source)]},
        )
        assert create_response.status_code == 200
        job_id = int(create_response.json()["job_id"])

        _tick_until_state(client, "WAIT_NETWORK")
        _advance_wait_network_handshake(client)

        state_response = client.get("/state")
        assert state_response.status_code == 200
        assert state_response.json()["current_state"] == "UPLOAD_FILE"

        detail_response = client.get(f"/ingest/jobs/{job_id}")
        assert detail_response.status_code == 200
        detail = detail_response.json()
        assert detail["status_counts"]["READY_TO_UPLOAD"] == 1
        assert detail["upload_pending"] is True


def test_wait_network_handshake_network_failure_is_retry_safe_and_deterministic(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "state.sqlite3"
    staging_root = tmp_path / "staging"
    source = tmp_path / "media" / "sd" / "retry.jpg"
    _write_source_file(source, b"retry-me")
    call_count = {"value": 0}

    def fake_handshake(*, server_base_url: str, files: list[dict[str, object]], timeout_seconds: float = 5.0):
        call_count["value"] += 1
        if call_count["value"] == 1:
            raise URLError("offline")
        return {int(item["file_id"]): "UPLOAD_REQUIRED" for item in files}

    monkeypatch.setattr(engine, "_post_metadata_handshake", fake_handshake)

    app = create_app(db_path=db_path, staging_root=staging_root, server_base_url="http://fake")
    with TestClient(app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-m2-retry", "source_paths": [str(source)]},
        )
        assert create_response.status_code == 200
        job_id = int(create_response.json()["job_id"])

        _tick_until_state(client, "WAIT_NETWORK")

        first_tick = client.post("/daemon/tick")
        assert first_tick.status_code == 200
        assert first_tick.json()["next_state"] == "UPLOAD_PREPARE"

        failed_tick = client.post("/daemon/tick")
        assert failed_tick.status_code == 200
        failed_body = failed_tick.json()
        assert failed_body["errored"] is True
        assert failed_body["next_state"] == "WAIT_NETWORK"

        detail_after_failure = client.get(f"/ingest/jobs/{job_id}")
        assert detail_after_failure.status_code == 200
        failed_file = detail_after_failure.json()["files"][0]
        assert failed_file["status"] == "READY_TO_UPLOAD"
        assert failed_file["retry_count"] == 1
        assert failed_file["last_error"]

        _age_status_rows(db_path, status="READY_TO_UPLOAD")
        _advance_wait_network_handshake(client)

        state_response = client.get("/state")
        assert state_response.status_code == 200
        assert state_response.json()["current_state"] == "UPLOAD_FILE"

        detail_after_success = client.get(f"/ingest/jobs/{job_id}")
        assert detail_after_success.status_code == 200
        success_file = detail_after_success.json()["files"][0]
        assert success_file["status"] == "READY_TO_UPLOAD"
        assert success_file["retry_count"] == 1
        assert success_file["last_error"] is None


def test_wait_network_stays_put_while_offline_even_when_retry_due(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "state.sqlite3"
    staging_root = tmp_path / "staging"
    source = tmp_path / "media" / "sd" / "offline-due.jpg"
    _write_source_file(source, b"offline-due")

    monkeypatch.setattr(engine, "_network_is_online", lambda: False)

    app = create_app(db_path=db_path, staging_root=staging_root, server_base_url="http://fake")
    with TestClient(app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-m2-offline-due", "source_paths": [str(source)]},
        )
        assert create_response.status_code == 200
        _tick_until_state(client, "WAIT_NETWORK")

        wait_tick = client.post("/daemon/tick")
        assert wait_tick.status_code == 200
        payload = wait_tick.json()
        assert payload["next_state"] == "WAIT_NETWORK"
        assert payload["progressed"] is False
        assert payload["network_online"] is False
        assert payload["ready_to_upload"] == 1


def test_wait_network_stays_put_when_nmcli_unavailable(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "state.sqlite3"
    staging_root = tmp_path / "staging"
    source = tmp_path / "media" / "sd" / "nmcli-missing.jpg"
    _write_source_file(source, b"nmcli-missing")

    def missing_nmcli(*_args, **_kwargs):
        raise FileNotFoundError("nmcli")

    monkeypatch.setattr(engine, "_network_is_online", ORIGINAL_NETWORK_IS_ONLINE)
    monkeypatch.setattr(subprocess, "run", missing_nmcli)

    app = create_app(db_path=db_path, staging_root=staging_root, server_base_url="http://fake")
    with TestClient(app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-m2-nmcli-missing", "source_paths": [str(source)]},
        )
        assert create_response.status_code == 200
        _tick_until_state(client, "WAIT_NETWORK")

        wait_tick = client.post("/daemon/tick")
        assert wait_tick.status_code == 200
        payload = wait_tick.json()
        assert payload["next_state"] == "WAIT_NETWORK"
        assert payload["progressed"] is False
        assert payload["network_online"] is False


def test_wait_network_advances_only_when_online_and_retry_due(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "state.sqlite3"
    staging_root = tmp_path / "staging"
    source = tmp_path / "media" / "sd" / "online-due.jpg"
    _write_source_file(source, b"online-due")

    monkeypatch.setattr(engine, "_network_is_online", lambda: True)

    app = create_app(db_path=db_path, staging_root=staging_root, server_base_url="http://fake")
    with TestClient(app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-m2-online-due", "source_paths": [str(source)]},
        )
        assert create_response.status_code == 200
        _tick_until_state(client, "WAIT_NETWORK")

        now_iso = datetime.now(UTC).isoformat()
        conn = sqlite3.connect(db_path)
        conn.execute(
            "UPDATE ingest_files SET retry_count = 2, updated_at_utc = ? WHERE status = 'READY_TO_UPLOAD';",
            (now_iso,),
        )
        conn.commit()
        conn.close()

        not_due_tick = client.post("/daemon/tick")
        assert not_due_tick.status_code == 200
        not_due_payload = not_due_tick.json()
        assert not_due_payload["next_state"] == "WAIT_NETWORK"
        assert not_due_payload["progressed"] is False
        assert not_due_payload["network_online"] is True
        assert not_due_payload["next_retry_at_utc"] is not None

        _age_status_rows(db_path, status="READY_TO_UPLOAD")

        due_tick = client.post("/daemon/tick")
        assert due_tick.status_code == 200
        due_payload = due_tick.json()
        assert due_payload["next_state"] == "UPLOAD_PREPARE"
        assert due_payload["progressed"] is True
        assert due_payload["network_online"] is True


def test_restart_safety_preserves_handshake_classification(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "state.sqlite3"
    staging_root = tmp_path / "staging"
    source = tmp_path / "media" / "sd" / "restart.jpg"
    _write_source_file(source, b"restart-safe")

    def fake_handshake(*, server_base_url: str, files: list[dict[str, object]], timeout_seconds: float = 5.0):
        return {int(item["file_id"]): "ALREADY_EXISTS" for item in files}

    monkeypatch.setattr(engine, "_post_metadata_handshake", fake_handshake)

    first_app = create_app(db_path=db_path, staging_root=staging_root, server_base_url="http://fake")
    with TestClient(first_app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-m2-restart", "source_paths": [str(source)]},
        )
        assert create_response.status_code == 200
        job_id = int(create_response.json()["job_id"])
        _tick_until_state(client, "WAIT_NETWORK")

    second_app = create_app(db_path=db_path, staging_root=staging_root, server_base_url="http://fake")
    with TestClient(second_app) as client:
        _advance_wait_network_handshake(client)
        detail_response = client.get(f"/ingest/jobs/{job_id}")
        assert detail_response.status_code == 200
        detail = detail_response.json()
        assert detail["status_counts"]["DUPLICATE_SHA_GLOBAL"] == 1

    third_app = create_app(db_path=db_path, staging_root=staging_root, server_base_url="http://fake")
    with TestClient(third_app) as client:
        state_response = client.get("/state")
        assert state_response.status_code == 200
        assert state_response.json()["current_state"] == "IDLE"
        detail_response = client.get(f"/ingest/jobs/{job_id}")
        assert detail_response.status_code == 200
        file_row = detail_response.json()["files"][0]
        assert file_row["status"] == "DUPLICATE_SHA_GLOBAL"
        assert file_row["retry_count"] == 0


def test_upload_file_and_server_verify_mark_file_verified_remote(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "state.sqlite3"
    staging_root = tmp_path / "staging"
    source = tmp_path / "media" / "sd" / "upload-ok.jpg"
    _write_source_file(source, b"upload-ok")

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

    app = create_app(db_path=db_path, staging_root=staging_root, server_base_url="http://fake")
    with TestClient(app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-m2-upload-ok", "source_paths": [str(source)]},
        )
        assert create_response.status_code == 200
        job_id = int(create_response.json()["job_id"])
        _tick_until_state(client, "WAIT_NETWORK")

        _advance_to_upload_file(client)
        upload_tick = client.post("/daemon/tick")
        assert upload_tick.status_code == 200
        assert upload_tick.json()["next_state"] == "SERVER_VERIFY"

        verify_tick = client.post("/daemon/tick")
        assert verify_tick.status_code == 200
        assert verify_tick.json()["verify_status"] == "VERIFIED"
        assert verify_tick.json()["next_state"] == "POST_UPLOAD_VERIFY"

        _drain_ticks_until_state(client, "IDLE")

        detail_response = client.get(f"/ingest/jobs/{job_id}")
        assert detail_response.status_code == 200
        assert detail_response.json()["status"] == "JOB_COMPLETE_LOCAL"
        file_row = detail_response.json()["files"][0]
        assert file_row["status"] == "VERIFIED_REMOTE"
        assert file_row["retry_count"] == 0


def test_upload_file_failure_retries_from_ready_to_upload(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "state.sqlite3"
    staging_root = tmp_path / "staging"
    source = tmp_path / "media" / "sd" / "upload-retry.jpg"
    _write_source_file(source, b"upload-retry")
    call_count = {"upload": 0}

    monkeypatch.setattr(
        engine,
        "_post_metadata_handshake",
        lambda *, server_base_url, files, timeout_seconds=5.0: {
            int(item["file_id"]): "UPLOAD_REQUIRED" for item in files
        },
    )

    def fake_upload(*, server_base_url, sha256_hex, size_bytes, content, timeout_seconds=5.0):
        call_count["upload"] += 1
        if call_count["upload"] == 1:
            raise URLError("upload offline")
        return "STORED_TEMP"

    monkeypatch.setattr(engine, "_upload_file_content", fake_upload)
    monkeypatch.setattr(
        engine,
        "_post_server_verify",
        lambda *, server_base_url, sha256_hex, size_bytes, timeout_seconds=5.0: "VERIFIED",
    )

    app = create_app(db_path=db_path, staging_root=staging_root, server_base_url="http://fake")
    with TestClient(app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-m2-upload-retry", "source_paths": [str(source)]},
        )
        assert create_response.status_code == 200
        job_id = int(create_response.json()["job_id"])
        _tick_until_state(client, "WAIT_NETWORK")

        _advance_to_upload_file(client)
        failed_upload_tick = client.post("/daemon/tick")
        assert failed_upload_tick.status_code == 200
        assert failed_upload_tick.json()["errored"] is True
        assert failed_upload_tick.json()["next_state"] == "WAIT_NETWORK"

        detail_after_failure = client.get(f"/ingest/jobs/{job_id}")
        failed_file = detail_after_failure.json()["files"][0]
        assert failed_file["status"] == "READY_TO_UPLOAD"
        assert failed_file["retry_count"] == 1
        assert failed_file["last_error"]

        _age_status_rows(db_path, status="READY_TO_UPLOAD")
        _advance_to_upload_file(client)
        upload_tick = client.post("/daemon/tick")
        assert upload_tick.status_code == 200
        assert upload_tick.json()["next_state"] == "SERVER_VERIFY"

        verify_tick = client.post("/daemon/tick")
        assert verify_tick.status_code == 200
        assert verify_tick.json()["verify_status"] == "VERIFIED"
        _drain_ticks_until_state(client, "IDLE")


def test_server_verify_failure_moves_file_back_to_ready_to_upload(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "state.sqlite3"
    staging_root = tmp_path / "staging"
    source = tmp_path / "media" / "sd" / "verify-retry.jpg"
    _write_source_file(source, b"verify-retry")

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
        lambda *, server_base_url, sha256_hex, size_bytes, timeout_seconds=5.0: "VERIFY_FAILED",
    )

    app = create_app(db_path=db_path, staging_root=staging_root, server_base_url="http://fake")
    with TestClient(app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-m2-verify-retry", "source_paths": [str(source)]},
        )
        assert create_response.status_code == 200
        job_id = int(create_response.json()["job_id"])
        _tick_until_state(client, "WAIT_NETWORK")

        _advance_to_upload_file(client)
        upload_tick = client.post("/daemon/tick")
        assert upload_tick.status_code == 200
        assert upload_tick.json()["next_state"] == "SERVER_VERIFY"

        verify_tick = client.post("/daemon/tick")
        assert verify_tick.status_code == 200
        assert verify_tick.json()["verify_status"] == "VERIFY_FAILED"
        assert verify_tick.json()["next_state"] == "REUPLOAD_OR_QUARANTINE"

        reupload_tick = client.post("/daemon/tick")
        assert reupload_tick.status_code == 200
        assert reupload_tick.json()["next_state"] == "WAIT_NETWORK"

        detail_response = client.get(f"/ingest/jobs/{job_id}")
        file_row = detail_response.json()["files"][0]
        assert file_row["status"] == "READY_TO_UPLOAD"
        assert file_row["retry_count"] == 1
        assert file_row["last_error"] == "server verification failed"


def test_reupload_or_quarantine_marks_error_file_when_retries_exhausted(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "state.sqlite3"
    staging_root = tmp_path / "staging"
    source = tmp_path / "media" / "sd" / "verify-exhausted.jpg"
    _write_source_file(source, b"verify-exhausted")

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
        lambda *, server_base_url, sha256_hex, size_bytes, timeout_seconds=5.0: "VERIFY_FAILED",
    )

    app = create_app(db_path=db_path, staging_root=staging_root, server_base_url="http://fake")
    with TestClient(app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-m2-verify-exhausted", "source_paths": [str(source)]},
        )
        assert create_response.status_code == 200
        job_id = int(create_response.json()["job_id"])
        _tick_until_state(client, "WAIT_NETWORK")

        _advance_to_upload_file(client)
        upload_tick = client.post("/daemon/tick")
        assert upload_tick.status_code == 200
        assert upload_tick.json()["next_state"] == "SERVER_VERIFY"

        verify_tick = client.post("/daemon/tick")
        assert verify_tick.status_code == 200
        assert verify_tick.json()["next_state"] == "REUPLOAD_OR_QUARANTINE"

        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE ingest_files SET retry_count = 3 WHERE job_id = ?;", (job_id,))
        conn.commit()
        conn.close()

        reupload_tick = client.post("/daemon/tick")
        assert reupload_tick.status_code == 200
        assert reupload_tick.json()["next_state"] == "ERROR_FILE"
        assert reupload_tick.json()["max_retries"] == 3

        state_response = client.get("/state")
        assert state_response.status_code == 200
        assert state_response.json()["current_state"] == "ERROR_FILE"

        detail_response = client.get(f"/ingest/jobs/{job_id}")
        assert detail_response.status_code == 200
        detail = detail_response.json()
        assert detail["status"] == "ERROR_FILE"
        file_row = detail["files"][0]
        assert file_row["status"] == "ERROR_FILE"
        assert "retries exhausted" in (file_row["last_error"] or "")


def test_reupload_or_quarantine_targets_specific_failed_file(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "state.sqlite3"
    staging_root = tmp_path / "staging"
    source_a = tmp_path / "media" / "sd" / "a.jpg"
    source_b = tmp_path / "media" / "sd" / "b.jpg"
    _write_source_file(source_a, b"file-a")
    _write_source_file(source_b, b"file-b")

    app = create_app(db_path=db_path, staging_root=staging_root, server_base_url="http://fake")
    with TestClient(app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={
                "media_label": "sd-m2-reupload-target",
                "source_paths": [str(source_a), str(source_b)],
            },
        )
        assert create_response.status_code == 200
        job_id = int(create_response.json()["job_id"])
        _tick_until_state(client, "WAIT_NETWORK")

        detail = client.get(f"/ingest/jobs/{job_id}").json()
        files = detail["files"]
        assert len(files) == 2
        first_id = int(files[0]["file_id"])
        target_id = int(files[1]["file_id"])

        now_iso = datetime.now(UTC).isoformat()
        conn = sqlite3.connect(db_path)
        conn.execute(
            "UPDATE daemon_state SET current_state = ?, updated_at_utc = ? WHERE id = 1;",
            ("REUPLOAD_OR_QUARANTINE", now_iso),
        )
        conn.execute(
            "UPDATE ingest_jobs SET status = ?, updated_at_utc = ? WHERE id = ?;",
            ("REUPLOAD_OR_QUARANTINE", now_iso, job_id),
        )
        conn.execute(
            "UPDATE ingest_files SET retry_count = 3, last_error = ?, updated_at_utc = ? WHERE id = ?;",
            ("handshake transient", now_iso, first_id),
        )
        conn.execute(
            "UPDATE ingest_files SET retry_count = 1, last_error = ?, updated_at_utc = ? WHERE id = ?;",
            ("server verification failed", now_iso, target_id),
        )
        conn.commit()
        conn.close()

        reupload_tick = client.post("/daemon/tick")
        assert reupload_tick.status_code == 200
        payload = reupload_tick.json()
        assert payload["next_state"] == "WAIT_NETWORK"
        assert int(payload["file_id"]) == target_id

        detail_after = client.get(f"/ingest/jobs/{job_id}").json()
        files_after = {int(item["file_id"]): item for item in detail_after["files"]}
        assert files_after[target_id]["status"] == "READY_TO_UPLOAD"
        assert files_after[target_id]["last_error"] == "server verification failed"
        assert files_after[first_id]["status"] == "READY_TO_UPLOAD"


def test_wait_network_applies_backoff_for_ready_to_upload_retry(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "state.sqlite3"
    staging_root = tmp_path / "staging"
    source = tmp_path / "media" / "sd" / "ready-backoff.jpg"
    _write_source_file(source, b"ready-backoff")

    monkeypatch.setattr(
        engine,
        "_post_metadata_handshake",
        lambda *, server_base_url, files, timeout_seconds=5.0: (_ for _ in ()).throw(
            URLError("network down")
        ),
    )

    app = create_app(db_path=db_path, staging_root=staging_root, server_base_url="http://fake")
    with TestClient(app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-m2-ready-backoff", "source_paths": [str(source)]},
        )
        assert create_response.status_code == 200

        _tick_until_state(client, "WAIT_NETWORK")
        first_wait_tick = client.post("/daemon/tick")
        assert first_wait_tick.status_code == 200
        assert first_wait_tick.json()["next_state"] == "UPLOAD_PREPARE"

        prepare_tick = client.post("/daemon/tick")
        assert prepare_tick.status_code == 200
        assert prepare_tick.json()["next_state"] == "WAIT_NETWORK"

        backoff_tick = client.post("/daemon/tick")
        assert backoff_tick.status_code == 200
        assert backoff_tick.json()["next_state"] == "WAIT_NETWORK"
        assert backoff_tick.json()["progressed"] is False
        assert backoff_tick.json()["next_retry_at_utc"] is not None

        _age_status_rows(db_path, status="READY_TO_UPLOAD")

        due_tick = client.post("/daemon/tick")
        assert due_tick.status_code == 200
        assert due_tick.json()["next_state"] == "UPLOAD_PREPARE"
        assert due_tick.json()["progressed"] is True


def test_wait_network_applies_backoff_for_uploaded_verify_retry(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "state.sqlite3"
    staging_root = tmp_path / "staging"
    source = tmp_path / "media" / "sd" / "uploaded-backoff.jpg"
    _write_source_file(source, b"uploaded-backoff")

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
        lambda *, server_base_url, sha256_hex, size_bytes, timeout_seconds=5.0: (_ for _ in ()).throw(
            URLError("verify network down")
        ),
    )

    app = create_app(db_path=db_path, staging_root=staging_root, server_base_url="http://fake")
    with TestClient(app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-m2-uploaded-backoff", "source_paths": [str(source)]},
        )
        assert create_response.status_code == 200

        _tick_until_state(client, "WAIT_NETWORK")
        _advance_to_upload_file(client)

        upload_tick = client.post("/daemon/tick")
        assert upload_tick.status_code == 200
        assert upload_tick.json()["next_state"] == "SERVER_VERIFY"

        verify_tick = client.post("/daemon/tick")
        assert verify_tick.status_code == 200
        assert verify_tick.json()["next_state"] == "WAIT_NETWORK"

        backoff_tick = client.post("/daemon/tick")
        assert backoff_tick.status_code == 200
        assert backoff_tick.json()["next_state"] == "WAIT_NETWORK"
        assert backoff_tick.json()["progressed"] is False
        assert backoff_tick.json()["next_retry_at_utc"] is not None

        _age_status_rows(db_path, status="UPLOADED")

        due_tick = client.post("/daemon/tick")
        assert due_tick.status_code == 200
        assert due_tick.json()["next_state"] == "UPLOAD_PREPARE"
        assert due_tick.json()["progressed"] is True


def test_operator_can_requeue_error_file_upload(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "state.sqlite3"
    staging_root = tmp_path / "staging"
    source = tmp_path / "media" / "sd" / "operator-requeue.jpg"
    _write_source_file(source, b"operator-requeue")

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
        lambda *, server_base_url, sha256_hex, size_bytes, timeout_seconds=5.0: "VERIFY_FAILED",
    )

    app = create_app(db_path=db_path, staging_root=staging_root, server_base_url="http://fake")
    with TestClient(app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-m2-operator-requeue", "source_paths": [str(source)]},
        )
        assert create_response.status_code == 200
        job_id = int(create_response.json()["job_id"])
        _tick_until_state(client, "WAIT_NETWORK")

        _advance_to_upload_file(client)
        assert client.post("/daemon/tick").status_code == 200  # UPLOAD_FILE -> SERVER_VERIFY
        verify_tick = client.post("/daemon/tick")
        assert verify_tick.status_code == 200
        assert verify_tick.json()["next_state"] == "REUPLOAD_OR_QUARANTINE"

        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE ingest_files SET retry_count = 3 WHERE job_id = ?;", (job_id,))
        conn.commit()
        conn.close()

        exhausted_tick = client.post("/daemon/tick")
        assert exhausted_tick.status_code == 200
        assert exhausted_tick.json()["next_state"] == "ERROR_FILE"

        detail = client.get(f"/ingest/jobs/{job_id}").json()
        file_id = int(detail["files"][0]["file_id"])

        requeue_response = client.post(f"/ingest/files/{file_id}/retry-upload")
        assert requeue_response.status_code == 200
        assert requeue_response.json()["next_state"] == "UPLOAD_PREPARE"

        state_response = client.get("/state")
        assert state_response.status_code == 200
        assert state_response.json()["current_state"] == "UPLOAD_PREPARE"

        detail_after = client.get(f"/ingest/jobs/{job_id}").json()
        assert detail_after["files"][0]["status"] == "READY_TO_UPLOAD"
        assert detail_after["files"][0]["last_error"] is None


def test_restart_safety_with_uploaded_file_resumes_server_verify(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "state.sqlite3"
    staging_root = tmp_path / "staging"
    source = tmp_path / "media" / "sd" / "verify-restart.jpg"
    _write_source_file(source, b"verify-restart")

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

    first_app = create_app(db_path=db_path, staging_root=staging_root, server_base_url="http://fake")
    with TestClient(first_app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-m2-verify-restart", "source_paths": [str(source)]},
        )
        assert create_response.status_code == 200
        job_id = int(create_response.json()["job_id"])
        _tick_until_state(client, "WAIT_NETWORK")
        _advance_to_upload_file(client)
        upload_tick = client.post("/daemon/tick")
        assert upload_tick.status_code == 200
        assert upload_tick.json()["next_state"] == "SERVER_VERIFY"

    second_app = create_app(db_path=db_path, staging_root=staging_root, server_base_url="http://fake")
    with TestClient(second_app) as client:
        state_response = client.get("/state")
        assert state_response.status_code == 200
        assert state_response.json()["current_state"] == "SERVER_VERIFY"
        verify_tick = client.post("/daemon/tick")
        assert verify_tick.status_code == 200
        assert verify_tick.json()["verify_status"] == "VERIFIED"
        assert verify_tick.json()["next_state"] == "POST_UPLOAD_VERIFY"

        _drain_ticks_until_state(client, "IDLE")

        detail_response = client.get(f"/ingest/jobs/{job_id}")
        file_row = detail_response.json()["files"][0]
        assert file_row["status"] == "VERIFIED_REMOTE"


def test_verify_success_transitions_through_post_and_cleanup_states(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "state.sqlite3"
    staging_root = tmp_path / "staging"
    source = tmp_path / "media" / "sd" / "post-cleanup.jpg"
    _write_source_file(source, b"post-cleanup")

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

    app = create_app(db_path=db_path, staging_root=staging_root, server_base_url="http://fake")
    with TestClient(app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-m2-post-cleanup", "source_paths": [str(source)]},
        )
        assert create_response.status_code == 200
        job_id = int(create_response.json()["job_id"])
        _tick_until_state(client, "WAIT_NETWORK")

        _advance_to_upload_file(client)
        assert client.post("/daemon/tick").json()["next_state"] == "SERVER_VERIFY"
        assert client.post("/daemon/tick").json()["next_state"] == "POST_UPLOAD_VERIFY"
        assert client.post("/daemon/tick").json()["next_state"] == "CLEANUP_STAGING"
        assert client.post("/daemon/tick").json()["next_state"] == "JOB_COMPLETE_REMOTE"
        assert client.post("/daemon/tick").json()["next_state"] == "JOB_COMPLETE_LOCAL"
        assert client.post("/daemon/tick").json()["next_state"] == "IDLE"

        detail_response = client.get(f"/ingest/jobs/{job_id}")
        assert detail_response.status_code == 200
        assert detail_response.json()["status"] == "JOB_COMPLETE_LOCAL"


def test_cleanup_staging_retains_files_when_policy_true(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "state.sqlite3"
    staging_root = tmp_path / "staging"
    source = tmp_path / "media" / "sd" / "cleanup-retain.jpg"
    _write_source_file(source, b"cleanup-retain")

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

    app = create_app(
        db_path=db_path,
        staging_root=staging_root,
        server_base_url="http://fake",
        retain_staged_files=True,
    )
    with TestClient(app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-m2-cleanup-retain", "source_paths": [str(source)]},
        )
        assert create_response.status_code == 200
        job_id = int(create_response.json()["job_id"])
        _tick_until_state(client, "WAIT_NETWORK")

        _advance_to_upload_file(client)
        assert client.post("/daemon/tick").json()["next_state"] == "SERVER_VERIFY"
        assert client.post("/daemon/tick").json()["next_state"] == "POST_UPLOAD_VERIFY"
        assert client.post("/daemon/tick").json()["next_state"] == "CLEANUP_STAGING"

        detail = client.get(f"/ingest/jobs/{job_id}").json()
        staged_path = Path(detail["files"][0]["staged_path"])
        assert staged_path.exists()

        cleanup_tick = client.post("/daemon/tick")
        assert cleanup_tick.status_code == 200
        assert cleanup_tick.json()["next_state"] == "JOB_COMPLETE_REMOTE"
        assert cleanup_tick.json()["retained_count"] == 1
        assert cleanup_tick.json()["deleted_count"] == 0
        assert staged_path.exists()


def test_m4_end_to_end_acceptance_path_upload_finalize_index_and_admin_visibility(
    tmp_path: Path, monkeypatch
) -> None:
    client_db_path = tmp_path / "client-state.sqlite3"
    client_staging_root = tmp_path / "client-staging"
    server_storage_root = tmp_path / "server-storage"
    source = tmp_path / "media" / "sd" / "capture-001.jpg"
    _write_source_file(source, b"uploaded-from-client")

    api_store = InMemoryUploadStateStore()
    api_app = create_api_app(
        state_store=api_store,
        storage_root=server_storage_root,
        bootstrap_token="bootstrap-123",
    )

    with TestClient(api_app) as api_client:
        enroll_response = api_client.post(
            "/v1/client/enroll/bootstrap",
            json={
                "client_id": "m4-e2e-client",
                "display_name": "M4 E2E Client",
                "bootstrap_token": "bootstrap-123",
            },
        )
        assert enroll_response.status_code == 200
        approve_response = api_client.post("/v1/admin/clients/m4-e2e-client/approve")
        assert approve_response.status_code == 200
        api_auth_headers = {
            "x-photovault-client-id": "m4-e2e-client",
            "x-photovault-client-token": str(approve_response.json()["item"]["auth_token"]),
        }

        def _api_handshake(
            *, server_base_url: str, files: list[dict[str, object]], timeout_seconds: float = 5.0
        ) -> dict[int, str]:
            response = api_client.post(
                "/v1/upload/metadata-handshake",
                json={
                    "files": [
                        {
                            "client_file_id": int(item["file_id"]),
                            "sha256_hex": str(item["sha256_hex"]),
                            "size_bytes": int(item["size_bytes"]),
                        }
                        for item in files
                    ]
                },
                headers=api_auth_headers,
            )
            assert response.status_code == 200
            return {
                int(item["client_file_id"]): str(item["decision"])
                for item in response.json()["results"]
            }

        def _api_upload(
            *,
            server_base_url: str,
            sha256_hex: str,
            size_bytes: int,
            content: bytes,
            job_name: str | None = None,
            original_filename: str | None = None,
            timeout_seconds: float = 5.0,
        ) -> str:
            response = api_client.put(
                f"/v1/upload/content/{sha256_hex}",
                content=content,
                headers={
                    "x-size-bytes": str(size_bytes),
                    "x-job-name": job_name or "unknown-job",
                    "x-original-filename": original_filename or "unknown.bin",
                    **api_auth_headers,
                },
            )
            assert response.status_code == 200
            return str(response.json()["status"])

        def _api_verify(
            *, server_base_url: str, sha256_hex: str, size_bytes: int, timeout_seconds: float = 5.0
        ) -> str:
            response = api_client.post(
                "/v1/upload/verify",
                json={"sha256_hex": sha256_hex, "size_bytes": size_bytes},
                headers=api_auth_headers,
            )
            assert response.status_code == 200
            return str(response.json()["status"])

        monkeypatch.setattr(engine, "_post_metadata_handshake", _api_handshake)
        monkeypatch.setattr(engine, "_upload_file_content", _api_upload)
        monkeypatch.setattr(engine, "_post_server_verify", _api_verify)

        clientd_app = create_app(
            db_path=client_db_path,
            staging_root=client_staging_root,
            server_base_url="http://photovault-api.test",
        )
        with TestClient(clientd_app) as client:
            create_response = client.post(
                "/ingest/jobs",
                json={"media_label": "m4-e2e-job", "source_paths": [str(source)]},
            )
            assert create_response.status_code == 200
            job_id = int(create_response.json()["job_id"])

            _tick_until_state(client, "WAIT_NETWORK")
            _advance_to_upload_file(client)

            assert client.post("/daemon/tick").json()["next_state"] == "SERVER_VERIFY"
            verify_tick = client.post("/daemon/tick")
            assert verify_tick.status_code == 200
            assert verify_tick.json()["verify_status"] == "VERIFIED"
            assert verify_tick.json()["next_state"] == "POST_UPLOAD_VERIFY"
            _drain_ticks_until_state(client, "IDLE")

            detail_response = client.get(f"/ingest/jobs/{job_id}")
            assert detail_response.status_code == 200
            detail = detail_response.json()
            assert detail["status"] == "JOB_COMPLETE_LOCAL"
            uploaded_file = detail["files"][0]
            assert uploaded_file["status"] == "VERIFIED_REMOTE"

            uploaded_sha = str(uploaded_file["sha256_hex"])
            uploaded_size = int(source.stat().st_size)
            uploaded_paths = list(server_storage_root.rglob("capture-001.jpg"))
            assert len(uploaded_paths) == 1
            assert uploaded_paths[0].read_bytes() == b"uploaded-from-client"

            out_of_band = server_storage_root / "2026" / "04" / "Imported_Archive" / "manual-drop.jpg"
            out_of_band.parent.mkdir(parents=True, exist_ok=True)
            out_of_band.write_bytes(b"manual-server-import")
            out_of_band_sha = hashlib.sha256(b"manual-server-import").hexdigest()

            index_response = api_client.post("/v1/storage/index")
            assert index_response.status_code == 200
            index_payload = index_response.json()
            assert index_payload["scanned_files"] == 2
            assert index_payload["indexed_files"] == 2
            assert index_payload["new_sha_entries"] == 1
            assert index_payload["existing_sha_matches"] == 1
            assert index_payload["path_conflicts"] == 0
            assert index_payload["errors"] == 0

            uploaded_handshake = api_client.post(
                "/v1/upload/metadata-handshake",
                json={
                    "files": [
                        {
                            "client_file_id": 1,
                            "sha256_hex": uploaded_sha,
                            "size_bytes": uploaded_size,
                        }
                    ]
                },
                headers=api_auth_headers,
            )
            assert uploaded_handshake.status_code == 200
            assert uploaded_handshake.json()["results"][0]["decision"] == "ALREADY_EXISTS"

            indexed_handshake = api_client.post(
                "/v1/upload/metadata-handshake",
                json={
                    "files": [
                        {
                            "client_file_id": 2,
                            "sha256_hex": out_of_band_sha,
                            "size_bytes": len(b"manual-server-import"),
                        }
                    ]
                },
                headers=api_auth_headers,
            )
            assert indexed_handshake.status_code == 200
            assert indexed_handshake.json()["results"][0]["decision"] == "ALREADY_EXISTS"

            overview_response = api_client.get("/v1/admin/overview")
            assert overview_response.status_code == 200
            overview_payload = overview_response.json()
            assert overview_payload["total_known_sha256"] == 2
            assert overview_payload["total_stored_files"] == 2
            assert overview_payload["duplicate_file_paths"] == 0

            files_response = api_client.get("/v1/admin/files")
            assert files_response.status_code == 200
            file_rows = files_response.json()["items"]
            assert len(file_rows) == 2
            assert {
                row["relative_path"] for row in file_rows
            } == {
                uploaded_paths[0].relative_to(server_storage_root).as_posix(),
                "2026/04/Imported_Archive/manual-drop.jpg",
            }
            assert {row["source_kind"] for row in file_rows} == {"index_scan"}

            duplicates_response = api_client.get("/v1/admin/duplicates")
            assert duplicates_response.status_code == 200
            assert duplicates_response.json()["total"] == 0

            latest_run_response = api_client.get("/v1/admin/latest-index-run")
            assert latest_run_response.status_code == 200
            latest_run = latest_run_response.json()["latest_run"]
            assert latest_run is not None
            assert latest_run["scanned_files"] == 2
            assert latest_run["new_sha_entries"] == 1

def test_cleanup_staging_deletes_files_when_policy_false(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "state.sqlite3"
    staging_root = tmp_path / "staging"
    source = tmp_path / "media" / "sd" / "cleanup-delete.jpg"
    _write_source_file(source, b"cleanup-delete")

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

    app = create_app(
        db_path=db_path,
        staging_root=staging_root,
        server_base_url="http://fake",
        retain_staged_files=False,
    )
    with TestClient(app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-m2-cleanup-delete", "source_paths": [str(source)]},
        )
        assert create_response.status_code == 200
        job_id = int(create_response.json()["job_id"])
        _tick_until_state(client, "WAIT_NETWORK")

        _advance_to_upload_file(client)
        assert client.post("/daemon/tick").json()["next_state"] == "SERVER_VERIFY"
        assert client.post("/daemon/tick").json()["next_state"] == "POST_UPLOAD_VERIFY"
        assert client.post("/daemon/tick").json()["next_state"] == "CLEANUP_STAGING"

        detail = client.get(f"/ingest/jobs/{job_id}").json()
        staged_path = Path(detail["files"][0]["staged_path"])
        assert staged_path.exists()

        cleanup_tick = client.post("/daemon/tick")
        assert cleanup_tick.status_code == 200
        assert cleanup_tick.json()["next_state"] == "JOB_COMPLETE_REMOTE"
        assert cleanup_tick.json()["retained_count"] == 0
        assert cleanup_tick.json()["deleted_count"] == 1
        assert not staged_path.exists()


def test_cleanup_staging_transitions_paused_storage_on_delete_failure(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "state.sqlite3"
    staging_root = tmp_path / "staging"
    source = tmp_path / "media" / "sd" / "cleanup-fail.jpg"
    _write_source_file(source, b"cleanup-fail")

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

    real_unlink = Path.unlink

    def fail_under_staging(self: Path, *args, **kwargs):
        if "staging" in str(self):
            raise OSError("disk failure")
        return real_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_under_staging)

    app = create_app(
        db_path=db_path,
        staging_root=staging_root,
        server_base_url="http://fake",
        retain_staged_files=False,
    )
    with TestClient(app) as client:
        create_response = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-m2-cleanup-fail", "source_paths": [str(source)]},
        )
        assert create_response.status_code == 200
        job_id = int(create_response.json()["job_id"])
        _tick_until_state(client, "WAIT_NETWORK")

        _advance_to_upload_file(client)
        assert client.post("/daemon/tick").json()["next_state"] == "SERVER_VERIFY"
        assert client.post("/daemon/tick").json()["next_state"] == "POST_UPLOAD_VERIFY"
        assert client.post("/daemon/tick").json()["next_state"] == "CLEANUP_STAGING"

        cleanup_tick = client.post("/daemon/tick")
        assert cleanup_tick.status_code == 200
        payload = cleanup_tick.json()
        assert payload["next_state"] == "PAUSED_STORAGE"
        assert payload["errored"] is True

        state_response = client.get("/state")
        assert state_response.status_code == 200
        assert state_response.json()["current_state"] == "PAUSED_STORAGE"

        detail = client.get(f"/ingest/jobs/{job_id}").json()
        assert detail["status"] == "PAUSED_STORAGE"
