import json
import sqlite3
import time
from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError

import pytest
from fastapi.testclient import TestClient
from photovault_clientd import engine
from photovault_clientd.app import create_app
from photovault_clientd.db import fetch_server_auth_state, open_db


class _FakeResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


@pytest.fixture(autouse=True)
def _network_online(monkeypatch):
    monkeypatch.setattr(engine, "_network_is_online", lambda: True)


def _write_source(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _request_header(request, name: str) -> str | None:
    for key, value in request.header_items():
        if key.lower() == name.lower():
            return value
    return None


def _tick_until_state(client: TestClient, target_state: str, max_steps: int = 30) -> None:
    for _ in range(max_steps):
        state = client.get("/state").json()["current_state"]
        if state == target_state:
            return
        tick = client.post("/daemon/tick")
        assert tick.status_code == 200
    raise AssertionError(f"expected state {target_state}")


def test_pending_enrollment_persists_and_blocks_privileged_work(tmp_path: Path, monkeypatch) -> None:
    handshake_called = {"value": False}

    def fake_urlopen(request, timeout=5.0):
        if request.full_url.endswith("/v1/client/enroll/bootstrap"):
            return _FakeResponse(
                {
                    "client_id": "pi-kitchen",
                    "display_name": "Kitchen Pi",
                    "enrollment_status": "pending",
                    "auth_token": None,
                    "first_seen_at_utc": "2026-04-22T10:00:00+00:00",
                    "last_enrolled_at_utc": "2026-04-22T10:00:00+00:00",
                }
            )
        if request.full_url.endswith("/v1/upload/metadata-handshake"):
            handshake_called["value"] = True
            return _FakeResponse({"results": []})
        raise AssertionError(f"unexpected URL: {request.full_url}")

    monkeypatch.setattr(engine, "urlopen", fake_urlopen)

    db_path = tmp_path / "state.sqlite3"
    source = tmp_path / "media" / "pending.jpg"
    _write_source(source, b"needs-approval")

    app = create_app(
        db_path=db_path,
        staging_root=tmp_path / "staging",
        server_base_url="http://fake",
        client_id="pi-kitchen",
        client_display_name="Kitchen Pi",
        bootstrap_token="bootstrap-123",
    )
    with TestClient(app) as client:
        create = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-auth-pending", "source_paths": [str(source)]},
        )
        assert create.status_code == 200
        _tick_until_state(client, "WAIT_NETWORK")

        tick = client.post("/daemon/tick")
        assert tick.status_code == 200
        payload = tick.json()
        assert payload["next_state"] == "WAIT_NETWORK"
        assert payload["auth_blocked"] is True
        assert payload["auth_reason"] == "CLIENT_PENDING_APPROVAL"

        state = client.get("/state").json()
        assert state["server_auth"]["enrollment_status"] == "pending"
        assert state["server_auth"]["auth_token"] is None

    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT client_id, enrollment_status, auth_token FROM server_auth_state WHERE id = 1;"
    ).fetchone()
    conn.close()
    assert row == ("pi-kitchen", "pending", None)
    assert handshake_called["value"] is False


def test_revoked_enrollment_persists_and_blocks_privileged_work(tmp_path: Path, monkeypatch) -> None:
    def fake_urlopen(request, timeout=5.0):
        if request.full_url.endswith("/v1/client/enroll/bootstrap"):
            return _FakeResponse(
                {
                    "client_id": "pi-kitchen",
                    "display_name": "Kitchen Pi",
                    "enrollment_status": "revoked",
                    "auth_token": None,
                    "first_seen_at_utc": "2026-04-22T10:00:00+00:00",
                    "last_enrolled_at_utc": "2026-04-22T10:00:00+00:00",
                }
            )
        raise AssertionError(f"unexpected URL: {request.full_url}")

    monkeypatch.setattr(engine, "urlopen", fake_urlopen)

    source = tmp_path / "media" / "revoked.jpg"
    _write_source(source, b"revoked")

    app = create_app(
        db_path=tmp_path / "state.sqlite3",
        staging_root=tmp_path / "staging",
        server_base_url="http://fake",
        client_id="pi-kitchen",
        client_display_name="Kitchen Pi",
        bootstrap_token="bootstrap-123",
    )
    with TestClient(app) as client:
        create = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-auth-revoked", "source_paths": [str(source)]},
        )
        assert create.status_code == 200
        _tick_until_state(client, "WAIT_NETWORK")

        tick = client.post("/daemon/tick")
        assert tick.status_code == 200
        payload = tick.json()
        assert payload["next_state"] == "WAIT_NETWORK"
        assert payload["auth_blocked"] is True
        assert payload["auth_reason"] == "CLIENT_REVOKED"

        state = client.get("/state").json()
        assert state["server_auth"]["enrollment_status"] == "revoked"


def test_approved_client_uses_auth_headers_for_handshake_and_upload_flow(tmp_path: Path, monkeypatch) -> None:
    observed = {
        "enroll_calls": 0,
        "handshake_headers": None,
        "upload_headers": None,
        "verify_headers": None,
    }

    def fake_urlopen(request, timeout=5.0):
        if request.full_url.endswith("/v1/client/enroll/bootstrap"):
            observed["enroll_calls"] += 1
            return _FakeResponse(
                {
                    "client_id": "pi-kitchen",
                    "display_name": "Kitchen Pi",
                    "enrollment_status": "approved",
                    "auth_token": "issued-token",
                    "first_seen_at_utc": "2026-04-22T10:00:00+00:00",
                    "last_enrolled_at_utc": "2026-04-22T10:00:00+00:00",
                }
            )
        if request.full_url.endswith("/v1/upload/metadata-handshake"):
            observed["handshake_headers"] = {
                "id": _request_header(request, "x-photovault-client-id"),
                "token": _request_header(request, "x-photovault-client-token"),
            }
            payload = json.loads(request.data.decode("utf-8"))
            return _FakeResponse(
                {
                    "results": [
                        {
                            "client_file_id": int(payload["files"][0]["client_file_id"]),
                            "decision": "UPLOAD_REQUIRED",
                        }
                    ]
                }
            )
        if "/v1/upload/content/" in request.full_url:
            observed["upload_headers"] = {
                "id": _request_header(request, "x-photovault-client-id"),
                "token": _request_header(request, "x-photovault-client-token"),
            }
            return _FakeResponse({"status": "STORED_TEMP"})
        if request.full_url.endswith("/v1/upload/verify"):
            observed["verify_headers"] = {
                "id": _request_header(request, "x-photovault-client-id"),
                "token": _request_header(request, "x-photovault-client-token"),
            }
            return _FakeResponse({"status": "VERIFIED"})
        raise AssertionError(f"unexpected URL: {request.full_url}")

    monkeypatch.setattr(engine, "urlopen", fake_urlopen)

    source = tmp_path / "media" / "approved.jpg"
    _write_source(source, b"approved-flow")

    app = create_app(
        db_path=tmp_path / "state.sqlite3",
        staging_root=tmp_path / "staging",
        server_base_url="http://fake",
        client_id="pi-kitchen",
        client_display_name="Kitchen Pi",
        bootstrap_token="bootstrap-123",
    )
    with TestClient(app) as client:
        create = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-auth-approved", "source_paths": [str(source)]},
        )
        assert create.status_code == 200
        _tick_until_state(client, "WAIT_NETWORK")

        _tick_until_state(client, "IDLE", max_steps=40)

        state = client.get("/state").json()
        assert state["server_auth"]["enrollment_status"] == "approved"
        assert state["server_auth"]["auth_token"] == "issued-token"

    assert observed["enroll_calls"] == 1
    assert observed["handshake_headers"] == {"id": "pi-kitchen", "token": "issued-token"}
    assert observed["upload_headers"] == {"id": "pi-kitchen", "token": "issued-token"}
    assert observed["verify_headers"] == {"id": "pi-kitchen", "token": "issued-token"}


def test_heartbeat_sender_is_deterministic_and_does_not_break_upload_flow(
    tmp_path: Path, monkeypatch
) -> None:
    observed = {
        "heartbeat_calls": 0,
        "heartbeat_headers": [],
        "heartbeat_payloads": [],
    }

    def fake_urlopen(request, timeout=5.0):
        if request.full_url.endswith("/v1/client/enroll/bootstrap"):
            return _FakeResponse(
                {
                    "client_id": "pi-kitchen",
                    "display_name": "Kitchen Pi",
                    "enrollment_status": "approved",
                    "auth_token": "issued-token",
                    "first_seen_at_utc": "2026-04-22T10:00:00+00:00",
                    "last_enrolled_at_utc": "2026-04-22T10:00:00+00:00",
                }
            )
        if request.full_url.endswith("/v1/client/heartbeat"):
            observed["heartbeat_calls"] += 1
            observed["heartbeat_headers"].append(
                {
                    "id": _request_header(request, "x-photovault-client-id"),
                    "token": _request_header(request, "x-photovault-client-token"),
                }
            )
            observed["heartbeat_payloads"].append(json.loads(request.data.decode("utf-8")))
            return _FakeResponse(
                {
                    "status": "RECORDED",
                    "client_id": "pi-kitchen",
                    "last_seen_at_utc": "2026-04-22T10:00:00+00:00",
                    "daemon_state": "WAIT_NETWORK",
                    "workload_status": "waiting",
                }
            )
        if request.full_url.endswith("/v1/upload/metadata-handshake"):
            payload = json.loads(request.data.decode("utf-8"))
            return _FakeResponse(
                {
                    "results": [
                        {
                            "client_file_id": int(payload["files"][0]["client_file_id"]),
                            "decision": "UPLOAD_REQUIRED",
                        }
                    ]
                }
            )
        if "/v1/upload/content/" in request.full_url:
            return _FakeResponse({"status": "STORED_TEMP"})
        if request.full_url.endswith("/v1/upload/verify"):
            return _FakeResponse({"status": "VERIFIED"})
        raise AssertionError(f"unexpected URL: {request.full_url}")

    monkeypatch.setattr(engine, "urlopen", fake_urlopen)

    source = tmp_path / "media" / "heartbeat.jpg"
    _write_source(source, b"heartbeat-flow")

    app = create_app(
        db_path=tmp_path / "state.sqlite3",
        staging_root=tmp_path / "staging",
        server_base_url="http://fake",
        client_id="pi-kitchen",
        client_display_name="Kitchen Pi",
        bootstrap_token="bootstrap-123",
        heartbeat_interval_seconds=1,
    )
    with TestClient(app) as client:
        create = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-heartbeat", "source_paths": [str(source)]},
        )
        assert create.status_code == 200
        _tick_until_state(client, "WAIT_NETWORK")

        # First online tick initializes heartbeat cadence.
        first_tick = client.post("/daemon/tick")
        assert first_tick.status_code == 200
        time.sleep(1.1)
        # Next tick sends heartbeat and continues normal upload flow.
        second_tick = client.post("/daemon/tick")
        assert second_tick.status_code == 200

        _tick_until_state(client, "IDLE", max_steps=40)

        state = client.get("/state").json()
        assert state["server_heartbeat"] is not None
        assert state["server_heartbeat"]["last_status"] == "sent"
        assert state["server_heartbeat"]["last_error"] is None

    assert observed["heartbeat_calls"] >= 1
    assert observed["heartbeat_headers"][0] == {"id": "pi-kitchen", "token": "issued-token"}
    assert observed["heartbeat_payloads"][0]["daemon_state"]
    assert observed["heartbeat_payloads"][0]["workload_status"] in {"idle", "working", "waiting", "blocked"}
    active_job = observed["heartbeat_payloads"][0]["active_job"]
    if active_job is not None:
        assert isinstance(active_job, dict)
        assert "total_files" in active_job
        assert "non_terminal_files" in active_job
        assert "error_files" in active_job
        assert "blocking_reason" in active_job


def test_invalid_auth_error_blocks_privileged_work_and_persists_reason(tmp_path: Path, monkeypatch) -> None:
    handshake_calls = {"value": 0}

    def fake_urlopen(request, timeout=5.0):
        if request.full_url.endswith("/v1/client/enroll/bootstrap"):
            return _FakeResponse(
                {
                    "client_id": "pi-kitchen",
                    "display_name": "Kitchen Pi",
                    "enrollment_status": "approved",
                    "auth_token": "issued-token",
                    "first_seen_at_utc": "2026-04-22T10:00:00+00:00",
                    "last_enrolled_at_utc": "2026-04-22T10:00:00+00:00",
                }
            )
        if request.full_url.endswith("/v1/upload/metadata-handshake"):
            handshake_calls["value"] += 1
            raise HTTPError(
                request.full_url,
                401,
                "Unauthorized",
                hdrs=None,
                fp=BytesIO(b'{"detail":"CLIENT_AUTH_INVALID"}'),
            )
        raise AssertionError(f"unexpected URL: {request.full_url}")

    monkeypatch.setattr(engine, "urlopen", fake_urlopen)

    source = tmp_path / "media" / "invalid-auth.jpg"
    _write_source(source, b"invalid-auth")

    app = create_app(
        db_path=tmp_path / "state.sqlite3",
        staging_root=tmp_path / "staging",
        server_base_url="http://fake",
        client_id="pi-kitchen",
        client_display_name="Kitchen Pi",
        bootstrap_token="bootstrap-123",
    )
    with TestClient(app) as client:
        create = client.post(
            "/ingest/jobs",
            json={"media_label": "sd-auth-invalid", "source_paths": [str(source)]},
        )
        assert create.status_code == 200
        _tick_until_state(client, "WAIT_NETWORK")

        tick = client.post("/daemon/tick")
        assert tick.status_code == 200
        assert tick.json()["next_state"] == "UPLOAD_PREPARE"

        blocked_tick = client.post("/daemon/tick")
        assert blocked_tick.status_code == 200
        payload = blocked_tick.json()
        assert payload["next_state"] == "WAIT_NETWORK"
        assert payload["auth_blocked"] is True
        assert payload["auth_reason"] == "CLIENT_AUTH_INVALID"

        state = client.get("/state").json()
        assert state["server_auth"]["enrollment_status"] == "approved"
        assert state["server_auth"]["last_error"] == "CLIENT_AUTH_INVALID"

    assert handshake_calls["value"] >= 1


def test_pending_auth_state_reenrolls_and_picks_up_approval_token(tmp_path: Path, monkeypatch) -> None:
    responses = [
        {
            "client_id": "pi-kitchen",
            "display_name": "Kitchen Pi",
            "enrollment_status": "pending",
            "auth_token": None,
            "first_seen_at_utc": "2026-04-22T10:00:00+00:00",
            "last_enrolled_at_utc": "2026-04-22T10:00:00+00:00",
        },
        {
            "client_id": "pi-kitchen",
            "display_name": "Kitchen Pi",
            "enrollment_status": "approved",
            "auth_token": "issued-token",
            "first_seen_at_utc": "2026-04-22T10:00:00+00:00",
            "last_enrolled_at_utc": "2026-04-22T10:05:00+00:00",
        },
    ]

    def fake_urlopen(request, timeout=5.0):
        assert request.full_url.endswith("/v1/client/enroll/bootstrap")
        payload = responses.pop(0)
        return _FakeResponse(payload)

    monkeypatch.setattr(engine, "urlopen", fake_urlopen)

    conn = open_db(tmp_path / "state.sqlite3")
    try:
        headers, reason = engine._build_client_auth_headers(
            conn,
            server_base_url="http://fake",
            client_id="pi-kitchen",
            display_name="Kitchen Pi",
            bootstrap_token="bootstrap-123",
            now_utc="2026-04-22T10:00:00+00:00",
        )
        assert headers is None
        assert reason == "CLIENT_PENDING_APPROVAL"

        pending_state = fetch_server_auth_state(conn)
        assert pending_state is not None
        assert pending_state["enrollment_status"] == "pending"

        headers, reason = engine._build_client_auth_headers(
            conn,
            server_base_url="http://fake",
            client_id="pi-kitchen",
            display_name="Kitchen Pi",
            bootstrap_token="bootstrap-123",
            now_utc="2026-04-22T10:05:00+00:00",
        )
        assert reason is None
        assert headers == {
            "x-photovault-client-id": "pi-kitchen",
            "x-photovault-client-token": "issued-token",
        }

        approved_state = fetch_server_auth_state(conn)
        assert approved_state is not None
        assert approved_state["enrollment_status"] == "approved"
        assert approved_state["auth_token"] == "issued-token"
    finally:
        conn.close()
