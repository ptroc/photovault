from pathlib import Path
from urllib.error import URLError

from fastapi.testclient import TestClient
from photovault_clientd import engine
from photovault_clientd.app import create_app


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
        assert verify_tick.json()["next_state"] == "WAIT_NETWORK"

        detail_response = client.get(f"/ingest/jobs/{job_id}")
        file_row = detail_response.json()["files"][0]
        assert file_row["status"] == "READY_TO_UPLOAD"
        assert file_row["retry_count"] == 1
        assert file_row["last_error"] == "server verification failed"


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
