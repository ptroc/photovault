import hashlib
import io
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import photovault_api.app as app_module
import photovault_api.media_preview as media_preview_module
from fastapi.testclient import TestClient
from photovault_api.app import create_app
from photovault_api.state_store import InMemoryUploadStateStore
from PIL import Image

TINY_PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
    b"\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\rIDATx\x9cc\xf8\xcf\xc0"
    b"\xf0\x1f\x00\x05\x00\x01\xff\x89\x99=\x1d\x00\x00\x00\x00IEND\xaeB`\x82"
)


def _jpeg_with_exif_bytes(
    *,
    width: int = 7,
    height: int = 5,
    capture_timestamp: str = "2026:04:21 14:15:16",
    capture_offset: str = "+02:00",
    camera_make: str = "Canon",
    camera_model: str = "EOS R6",
    orientation: int = 6,
    lens_model: str = "RF24-70mm F2.8 L IS USM",
    exposure_time: tuple[int, int] | None = None,
    f_number: tuple[int, int] | None = None,
    iso_speed: int | None = None,
    focal_length_mm: tuple[int, int] | None = None,
    focal_length_35mm: int | None = None,
) -> bytes:
    image = Image.new("RGB", (width, height), color=(120, 80, 40))
    exif = Image.Exif()
    exif[36867] = capture_timestamp
    exif[36881] = capture_offset
    exif[271] = camera_make
    exif[272] = camera_model
    exif[274] = orientation
    exif[42036] = lens_model
    if exposure_time is not None:
        exif[33434] = exposure_time
    if f_number is not None:
        exif[33437] = f_number
    if iso_speed is not None:
        exif[34855] = iso_speed
    if focal_length_mm is not None:
        exif[37386] = focal_length_mm
    if focal_length_35mm is not None:
        exif[41989] = focal_length_35mm
    buffer = io.BytesIO()
    image.save(buffer, format="JPEG", exif=exif)
    return buffer.getvalue()


def _upload_headers(
    *,
    size_bytes: int,
    job_name: str = "job",
    original_filename: str = "file.jpg",
) -> dict[str, str]:
    return {
        "x-size-bytes": str(size_bytes),
        "x-job-name": job_name,
        "x-original-filename": original_filename,
    }


def _client_auth_headers(*, client_id: str, client_token: str) -> dict[str, str]:
    return {
        "x-photovault-client-id": client_id,
        "x-photovault-client-token": client_token,
    }


def _with_auth_headers(base: dict[str, str], auth: dict[str, str]) -> dict[str, str]:
    return {**base, **auth}


def _heartbeat_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "last_seen_at_utc": "2026-04-22T10:10:00+00:00",
        "daemon_state": "WAIT_NETWORK",
        "workload_status": "waiting",
        "active_job": {
            "job_id": 11,
            "media_label": "SD-Card A",
            "job_status": "UPLOAD_PREPARE",
            "ready_to_upload": 4,
            "uploaded": 1,
            "retrying": 1,
            "total_files": 9,
            "non_terminal_files": 5,
            "error_files": 1,
            "blocking_reason": "WAIT_NETWORK",
        },
        "retry_backoff": {
            "pending_count": 2,
            "next_retry_at_utc": "2026-04-22T10:11:00+00:00",
            "reason": "upload offline",
        },
        "auth_block_reason": None,
        "recent_error": {
            "category": "UPLOAD_RETRY_SCHEDULED",
            "message": "temporary upload failure",
            "created_at_utc": "2026-04-22T10:09:00+00:00",
        },
    }
    payload.update(overrides)
    return payload


def _approve_upload_client(
    client: TestClient,
    *,
    client_id: str = "pi-test",
    display_name: str = "Pi Test",
) -> dict[str, str]:
    enroll = client.post(
        "/v1/client/enroll/bootstrap",
        json={
            "client_id": client_id,
            "display_name": display_name,
            "bootstrap_token": "bootstrap-123",
        },
    )
    assert enroll.status_code == 200
    approve = client.post(f"/v1/admin/clients/{client_id}/approve")
    assert approve.status_code == 200
    token = approve.json()["item"]["auth_token"]
    assert isinstance(token, str)
    return _client_auth_headers(client_id=client_id, client_token=token)


def test_healthz(tmp_path: Path) -> None:
    client = TestClient(create_app(storage_root=tmp_path, bootstrap_token="bootstrap-123"))
    response = client.get("/healthz")
    assert response.status_code == 200


def test_bootstrap_enrollment_creates_pending_client_record(tmp_path: Path) -> None:
    client = TestClient(create_app(storage_root=tmp_path, bootstrap_token="bootstrap-123"))
    response = client.post(
        "/v1/client/enroll/bootstrap",
        json={
            "client_id": "pi-kitchen",
            "display_name": "Kitchen Pi",
            "bootstrap_token": "bootstrap-123",
        },
    )
    assert response.status_code == 200
    assert response.json()["enrollment_status"] == "pending"
    assert response.json()["auth_token"] is None

    listing = client.get("/v1/admin/clients")
    assert listing.status_code == 200
    items = listing.json()["items"]
    assert len(items) == 1
    assert items[0]["client_id"] == "pi-kitchen"
    assert items[0]["display_name"] == "Kitchen Pi"
    assert items[0]["enrollment_status"] == "pending"
    assert items[0]["auth_token"] is None


def test_bootstrap_enrollment_is_idempotent_for_existing_client(tmp_path: Path) -> None:
    client = TestClient(create_app(storage_root=tmp_path, bootstrap_token="bootstrap-123"))
    first = client.post(
        "/v1/client/enroll/bootstrap",
        json={
            "client_id": "pi-kitchen",
            "display_name": "Kitchen Pi",
            "bootstrap_token": "bootstrap-123",
        },
    )
    assert first.status_code == 200
    first_seen = first.json()["first_seen_at_utc"]

    second = client.post(
        "/v1/client/enroll/bootstrap",
        json={
            "client_id": "pi-kitchen",
            "display_name": "Kitchen Pi Renamed",
            "bootstrap_token": "bootstrap-123",
        },
    )
    assert second.status_code == 200
    payload = second.json()
    assert payload["enrollment_status"] == "pending"
    assert payload["display_name"] == "Kitchen Pi Renamed"
    assert payload["first_seen_at_utc"] == first_seen


def test_approve_then_revoke_client_transitions_and_token_rules(tmp_path: Path) -> None:
    client = TestClient(create_app(storage_root=tmp_path, bootstrap_token="bootstrap-123"))
    enroll = client.post(
        "/v1/client/enroll/bootstrap",
        json={
            "client_id": "pi-kitchen",
            "display_name": "Kitchen Pi",
            "bootstrap_token": "bootstrap-123",
        },
    )
    assert enroll.status_code == 200

    approved = client.post("/v1/admin/clients/pi-kitchen/approve")
    assert approved.status_code == 200
    approve_item = approved.json()["item"]
    assert approve_item["enrollment_status"] == "approved"
    assert approve_item["auth_token"]

    revoke = client.post("/v1/admin/clients/pi-kitchen/revoke")
    assert revoke.status_code == 200
    revoke_item = revoke.json()["item"]
    assert revoke_item["enrollment_status"] == "revoked"
    assert revoke_item["auth_token"] == approve_item["auth_token"]


def test_bootstrap_enrollment_preserves_approved_client_and_returns_existing_token(tmp_path: Path) -> None:
    client = TestClient(create_app(storage_root=tmp_path, bootstrap_token="bootstrap-123"))
    enroll = client.post(
        "/v1/client/enroll/bootstrap",
        json={
            "client_id": "pi-kitchen",
            "display_name": "Kitchen Pi",
            "bootstrap_token": "bootstrap-123",
        },
    )
    assert enroll.status_code == 200
    approve = client.post("/v1/admin/clients/pi-kitchen/approve")
    assert approve.status_code == 200
    issued_token = approve.json()["item"]["auth_token"]

    reenroll = client.post(
        "/v1/client/enroll/bootstrap",
        json={
            "client_id": "pi-kitchen",
            "display_name": "Kitchen Pi Updated",
            "bootstrap_token": "bootstrap-123",
        },
    )
    assert reenroll.status_code == 200
    payload = reenroll.json()
    assert payload["enrollment_status"] == "approved"
    assert payload["auth_token"] == issued_token
    assert payload["display_name"] == "Kitchen Pi Updated"


def test_pending_and_revoked_client_auth_is_rejected_in_upload_handshake(tmp_path: Path) -> None:
    client = TestClient(create_app(storage_root=tmp_path, bootstrap_token="bootstrap-123"))
    enroll = client.post(
        "/v1/client/enroll/bootstrap",
        json={
            "client_id": "pi-kitchen",
            "display_name": "Kitchen Pi",
            "bootstrap_token": "bootstrap-123",
        },
    )
    assert enroll.status_code == 200

    pending_response = client.post(
        "/v1/upload/metadata-handshake",
        headers=_client_auth_headers(client_id="pi-kitchen", client_token="wrong-token"),
        json={"files": [{"client_file_id": 1, "sha256_hex": "a" * 64, "size_bytes": 10}]},
    )
    assert pending_response.status_code == 403
    assert pending_response.json()["detail"] == "CLIENT_PENDING_APPROVAL"

    approve = client.post("/v1/admin/clients/pi-kitchen/approve")
    assert approve.status_code == 200
    token = approve.json()["item"]["auth_token"]
    assert isinstance(token, str)

    pending_with_valid_token = client.post(
        "/v1/upload/metadata-handshake",
        headers=_client_auth_headers(client_id="pi-kitchen", client_token=token),
        json={"files": [{"client_file_id": 1, "sha256_hex": "a" * 64, "size_bytes": 10}]},
    )
    assert pending_with_valid_token.status_code == 200

    revoked = client.post("/v1/admin/clients/pi-kitchen/revoke")
    assert revoked.status_code == 200
    revoked_response = client.post(
        "/v1/upload/metadata-handshake",
        headers=_client_auth_headers(client_id="pi-kitchen", client_token=token),
        json={"files": [{"client_file_id": 1, "sha256_hex": "a" * 64, "size_bytes": 10}]},
    )
    assert revoked_response.status_code == 403
    assert revoked_response.json()["detail"] == "CLIENT_REVOKED"


def test_privileged_upload_endpoints_reject_missing_or_invalid_auth_headers(tmp_path: Path) -> None:
    content = b"payload"
    sha256_hex = hashlib.sha256(content).hexdigest()
    client = TestClient(create_app(storage_root=tmp_path, bootstrap_token="bootstrap-123"))
    auth_headers = _approve_upload_client(client)

    missing = client.post(
        "/v1/upload/metadata-handshake",
        json={"files": [{"client_file_id": 1, "sha256_hex": sha256_hex, "size_bytes": len(content)}]},
    )
    assert missing.status_code == 401
    assert missing.json()["detail"] == "CLIENT_AUTH_REQUIRED"

    invalid = client.post(
        "/v1/upload/metadata-handshake",
        headers=_client_auth_headers(client_id="pi-test", client_token="wrong"),
        json={"files": [{"client_file_id": 1, "sha256_hex": sha256_hex, "size_bytes": len(content)}]},
    )
    assert invalid.status_code == 401
    assert invalid.json()["detail"] == "CLIENT_AUTH_INVALID"

    upload_missing = client.put(
        f"/v1/upload/content/{sha256_hex}",
        content=content,
        headers=_upload_headers(size_bytes=len(content), job_name="Auth", original_filename="a.jpg"),
    )
    assert upload_missing.status_code == 401
    assert upload_missing.json()["detail"] == "CLIENT_AUTH_REQUIRED"

    upload_ok = client.put(
        f"/v1/upload/content/{sha256_hex}",
        content=content,
        headers=_with_auth_headers(
            _upload_headers(size_bytes=len(content), job_name="Auth", original_filename="a.jpg"),
            auth_headers,
        ),
    )
    assert upload_ok.status_code == 200

    verify_missing = client.post(
        "/v1/upload/verify",
        json={"sha256_hex": sha256_hex, "size_bytes": len(content)},
    )
    assert verify_missing.status_code == 401
    assert verify_missing.json()["detail"] == "CLIENT_AUTH_REQUIRED"


def test_approved_client_heartbeat_persists_latest_snapshot_and_surfaces_in_admin_clients(
    tmp_path: Path,
) -> None:
    client = TestClient(create_app(storage_root=tmp_path, bootstrap_token="bootstrap-123"))
    auth_headers = _approve_upload_client(client, client_id="pi-heart", display_name="Heart Pi")

    first = client.post(
        "/v1/client/heartbeat",
        headers=auth_headers,
        json=_heartbeat_payload(),
    )
    assert first.status_code == 200
    assert first.json()["status"] == "RECORDED"
    assert first.json()["client_id"] == "pi-heart"
    assert first.json()["daemon_state"] == "WAIT_NETWORK"
    assert first.json()["workload_status"] == "waiting"

    second = client.post(
        "/v1/client/heartbeat",
        headers=auth_headers,
        json=_heartbeat_payload(
            last_seen_at_utc="2026-04-22T10:12:00+00:00",
            daemon_state="UPLOAD_FILE",
            workload_status="working",
            active_job={
                "job_id": 11,
                "media_label": "SD-Card A",
                "job_status": "UPLOAD_FILE",
                "ready_to_upload": 3,
                "uploaded": 2,
                "retrying": 0,
                "total_files": 9,
                "non_terminal_files": 3,
                "error_files": 0,
                "blocking_reason": None,
            },
            retry_backoff={"pending_count": 1, "next_retry_at_utc": None, "reason": "n/a"},
        ),
    )
    assert second.status_code == 200
    assert second.json()["daemon_state"] == "UPLOAD_FILE"

    listing = client.get("/v1/admin/clients")
    assert listing.status_code == 200
    items = listing.json()["items"]
    assert len(items) == 1
    item = items[0]
    assert item["client_id"] == "pi-heart"
    assert item["heartbeat_last_seen_at_utc"] == "2026-04-22T10:12:00+00:00"
    assert item["heartbeat_daemon_state"] == "UPLOAD_FILE"
    assert item["heartbeat_workload_status"] == "working"
    assert "status=UPLOAD_FILE" in str(item["heartbeat_active_job_summary"])
    assert "total=9" in str(item["heartbeat_active_job_summary"])
    assert "non_terminal=3" in str(item["heartbeat_active_job_summary"])
    assert item["heartbeat_auth_block_reason"] is None
    assert item["heartbeat_recent_error_summary"] is not None


def test_heartbeat_rejects_missing_pending_and_revoked_auth(tmp_path: Path) -> None:
    client = TestClient(create_app(storage_root=tmp_path, bootstrap_token="bootstrap-123"))
    payload = _heartbeat_payload()

    missing = client.post("/v1/client/heartbeat", json=payload)
    assert missing.status_code == 401
    assert missing.json()["detail"] == "CLIENT_AUTH_REQUIRED"

    enroll = client.post(
        "/v1/client/enroll/bootstrap",
        json={
            "client_id": "pi-heart",
            "display_name": "Heart Pi",
            "bootstrap_token": "bootstrap-123",
        },
    )
    assert enroll.status_code == 200
    pending = client.post(
        "/v1/client/heartbeat",
        headers=_client_auth_headers(client_id="pi-heart", client_token="token"),
        json=payload,
    )
    assert pending.status_code == 403
    assert pending.json()["detail"] == "CLIENT_PENDING_APPROVAL"

    approve = client.post("/v1/admin/clients/pi-heart/approve")
    assert approve.status_code == 200
    token = approve.json()["item"]["auth_token"]
    revoke = client.post("/v1/admin/clients/pi-heart/revoke")
    assert revoke.status_code == 200

    revoked = client.post(
        "/v1/client/heartbeat",
        headers=_client_auth_headers(client_id="pi-heart", client_token=token),
        json=payload,
    )
    assert revoked.status_code == 403
    assert revoked.json()["detail"] == "CLIENT_REVOKED"


def test_heartbeat_presence_status_thresholds_are_deterministic() -> None:
    now_utc = datetime(2026, 4, 22, 10, 15, 0, tzinfo=UTC)
    online = app_module.ClientHeartbeatRecord(
        client_id="online",
        last_seen_at_utc="2026-04-22T10:13:45+00:00",
        daemon_state="WAIT_NETWORK",
        workload_status="waiting",
        active_job_id=None,
        active_job_label=None,
        active_job_status=None,
        active_job_ready_to_upload=None,
        active_job_uploaded=None,
        active_job_retrying=None,
        active_job_total_files=None,
        active_job_non_terminal_files=None,
        active_job_error_files=None,
        active_job_blocking_reason=None,
        retry_pending_count=None,
        retry_next_at_utc=None,
        retry_reason=None,
        auth_block_reason=None,
        recent_error_category=None,
        recent_error_message=None,
        recent_error_at_utc=None,
        updated_at_utc="2026-04-22T10:13:45+00:00",
    )
    stale = app_module.ClientHeartbeatRecord(
        client_id="stale",
        last_seen_at_utc="2026-04-22T10:13:20+00:00",
        daemon_state="WAIT_NETWORK",
        workload_status="waiting",
        active_job_id=None,
        active_job_label=None,
        active_job_status=None,
        active_job_ready_to_upload=None,
        active_job_uploaded=None,
        active_job_retrying=None,
        active_job_total_files=None,
        active_job_non_terminal_files=None,
        active_job_error_files=None,
        active_job_blocking_reason=None,
        retry_pending_count=None,
        retry_next_at_utc=None,
        retry_reason=None,
        auth_block_reason=None,
        recent_error_category=None,
        recent_error_message=None,
        recent_error_at_utc=None,
        updated_at_utc="2026-04-22T10:13:20+00:00",
    )

    assert app_module._heartbeat_presence_status(online, now_utc=now_utc) == "online"
    assert app_module._heartbeat_presence_status(stale, now_utc=now_utc) == "stale"
    assert app_module._heartbeat_presence_status(None, now_utc=now_utc) == "unknown"


def test_admin_clients_filtering_and_sorting_for_presence_and_workload(
    tmp_path: Path, monkeypatch
) -> None:
    fixed_now = datetime(2026, 4, 22, 10, 15, 0, tzinfo=UTC)

    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return fixed_now.replace(tzinfo=None)
            return fixed_now.astimezone(tz)

    monkeypatch.setattr(app_module, "datetime", _FixedDatetime)
    client = TestClient(create_app(storage_root=tmp_path, bootstrap_token="bootstrap-123"))
    online_headers = _approve_upload_client(client, client_id="pi-online", display_name="Online Pi")
    stale_headers = _approve_upload_client(client, client_id="pi-stale", display_name="Stale Pi")
    _approve_upload_client(client, client_id="pi-unknown", display_name="Unknown Pi")

    online_hb = client.post(
        "/v1/client/heartbeat",
        headers=online_headers,
        json=_heartbeat_payload(
            last_seen_at_utc="2026-04-22T10:14:40+00:00",
            workload_status="working",
            active_job={
                "job_id": 2,
                "media_label": "Online Job",
                "job_status": "UPLOAD_FILE",
                "ready_to_upload": 4,
                "uploaded": 3,
                "retrying": 0,
                "total_files": 10,
                "non_terminal_files": 2,
                "error_files": 0,
                "blocking_reason": None,
            },
        ),
    )
    assert online_hb.status_code == 200
    stale_hb = client.post(
        "/v1/client/heartbeat",
        headers=stale_headers,
        json=_heartbeat_payload(
            last_seen_at_utc="2026-04-22T10:12:00+00:00",
            workload_status="blocked",
            active_job={
                "job_id": 3,
                "media_label": "Stale Job",
                "job_status": "WAIT_NETWORK",
                "ready_to_upload": 1,
                "uploaded": 0,
                "retrying": 1,
                "total_files": 4,
                "non_terminal_files": 4,
                "error_files": 1,
                "blocking_reason": "WAIT_NETWORK",
            },
        ),
    )
    assert stale_hb.status_code == 200

    online_listing = client.get("/v1/admin/clients?presence_status=online")
    assert online_listing.status_code == 200
    online_items = online_listing.json()["items"]
    assert len(online_items) == 1
    assert online_items[0]["client_id"] == "pi-online"

    blocked_listing = client.get("/v1/admin/clients?workload_status=blocked")
    assert blocked_listing.status_code == 200
    blocked_items = blocked_listing.json()["items"]
    assert len(blocked_items) == 1
    assert blocked_items[0]["client_id"] == "pi-stale"

    sorted_listing = client.get("/v1/admin/clients?sort_by=presence_status&sort_order=asc")
    assert sorted_listing.status_code == 200
    sorted_ids = [item["client_id"] for item in sorted_listing.json()["items"]]
    assert sorted_ids == ["pi-online", "pi-stale", "pi-unknown"]

    invalid_filter = client.get("/v1/admin/clients?presence_status=bad")
    assert invalid_filter.status_code == 400
    assert invalid_filter.json()["detail"] == "invalid presence_status filter"


def test_metadata_handshake_reports_already_exists_for_known_sha(tmp_path: Path) -> None:
    known_sha = "a" * 64
    client = TestClient(
        create_app(
            initial_known_sha256={known_sha},
            storage_root=tmp_path,
            bootstrap_token="bootstrap-123",
        )
    )
    auth_headers = _approve_upload_client(client)
    response = client.post(
        "/v1/upload/metadata-handshake",
        headers=auth_headers,
        json={
            "files": [
                {"client_file_id": 1, "sha256_hex": known_sha, "size_bytes": 42},
            ]
        },
    )
    assert response.status_code == 200
    assert response.json() == {
        "results": [
            {"client_file_id": 1, "decision": "ALREADY_EXISTS"},
        ]
    }


def test_metadata_handshake_classifies_mixed_batch_with_single_lookup(tmp_path: Path) -> None:
    known_sha = "a" * 64
    unknown_sha = "b" * 64
    observed: dict[str, object] = {}

    class _BatchStore:
        def initialize(self) -> None:
            return None

        def has_sha(self, sha256_hex: str) -> bool:
            observed["has_sha_called"] = True
            return False

        def has_shas(self, sha256_hex_values: list[str]) -> set[str]:
            observed["lookup"] = list(sha256_hex_values)
            return {known_sha}

        def get_temp_upload(self, sha256_hex: str):
            return None

        def upsert_temp_upload(self, **kwargs) -> None:
            return None

        def mark_sha_verified(self, sha256_hex: str) -> bool:
            return False

        def upsert_stored_file(self, **kwargs) -> None:
            return None

        def get_stored_file_by_path(self, relative_path: str):
            return None

        def list_stored_files(self, *, limit: int, offset: int):
            return 0, []

        def list_duplicate_sha_groups(self, *, limit: int, offset: int):
            return 0, []

        def record_path_conflict(self, **kwargs) -> None:
            return None

        def list_path_conflicts(self, *, limit: int, offset: int):
            return 0, []

        def record_storage_index_run(self, record) -> None:
            return None

        def get_latest_storage_index_run(self):
            return None

        def summarize_storage(self):
            return {
                "total_known_sha256": 0,
                "total_stored_files": 0,
                "indexed_files": 0,
                "uploaded_files": 0,
                "duplicate_file_paths": 0,
                "recent_indexed_files_24h": 0,
                "recent_uploaded_files_24h": 0,
                "last_indexed_at_utc": None,
                "last_uploaded_at_utc": None,
            }

        def remove_temp_upload(self, sha256_hex: str) -> None:
            return None

        def get_client(self, client_id: str):
            if client_id != "pi-test":
                return None
            return SimpleNamespace(
                client_id=client_id,
                display_name="Pi Test",
                enrollment_status="approved",
                auth_token="token-123",
            )

    client = TestClient(create_app(state_store=_BatchStore(), storage_root=tmp_path))
    response = client.post(
        "/v1/upload/metadata-handshake",
        headers=_client_auth_headers(client_id="pi-test", client_token="token-123"),
        json={
            "files": [
                {"client_file_id": 1, "sha256_hex": known_sha, "size_bytes": 10},
                {"client_file_id": 2, "sha256_hex": unknown_sha, "size_bytes": 20},
            ]
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "results": [
            {"client_file_id": 1, "decision": "ALREADY_EXISTS"},
            {"client_file_id": 2, "decision": "UPLOAD_REQUIRED"},
        ]
    }
    assert observed["lookup"] == [known_sha, unknown_sha]
    assert observed.get("has_sha_called") is None


def test_upload_content_and_verify_writes_filesystem_and_promotes_sha(tmp_path: Path) -> None:
    store = InMemoryUploadStateStore()
    content = TINY_PNG
    sha256_hex = hashlib.sha256(content).hexdigest()
    client = TestClient(create_app(state_store=store, storage_root=tmp_path, bootstrap_token="bootstrap-123"))
    auth_headers = _approve_upload_client(client)

    upload_response = client.put(
        f"/v1/upload/content/{sha256_hex}",
        content=content,
        headers=_with_auth_headers(
            _upload_headers(
                size_bytes=len(content),
                job_name="Wedding Shoot",
                original_filename="IMG_0001.PNG",
            ),
            auth_headers,
        ),
    )
    assert upload_response.status_code == 200
    assert upload_response.json()["status"] == "STORED_TEMP"
    temp_path = tmp_path / ".temp_uploads" / f"{sha256_hex}.upload"
    assert temp_path.read_bytes() == content

    temp_record = store.get_temp_upload(sha256_hex)
    assert temp_record is not None
    verify_response = client.post(
        "/v1/upload/verify",
        headers=auth_headers,
        json={"sha256_hex": sha256_hex, "size_bytes": len(content)},
    )
    assert verify_response.status_code == 200
    assert verify_response.json()["status"] == "VERIFIED"
    repeat_verify_response = client.post(
        "/v1/upload/verify",
        headers=auth_headers,
        json={"sha256_hex": sha256_hex, "size_bytes": len(content)},
    )
    assert repeat_verify_response.status_code == 200
    assert repeat_verify_response.json()["status"] == "ALREADY_EXISTS"

    received = datetime.fromisoformat(temp_record.received_at_utc)
    final_path = (
        tmp_path / f"{received.year:04d}" / f"{received.month:02d}" / "Wedding_Shoot" / "IMG_0001.PNG"
    )
    assert final_path.read_bytes() == content
    assert not temp_path.exists()

    handshake_response = client.post(
        "/v1/upload/metadata-handshake",
        headers=auth_headers,
        json={"files": [{"client_file_id": 1, "sha256_hex": sha256_hex, "size_bytes": len(content)}]},
    )
    assert handshake_response.status_code == 200
    assert handshake_response.json()["results"][0]["decision"] == "ALREADY_EXISTS"

    catalog_response = client.get("/v1/admin/catalog")
    assert catalog_response.status_code == 200
    catalog_payload = catalog_response.json()
    assert catalog_payload["total"] == 1
    assert catalog_payload["items"] == [
        {
            "relative_path": str(final_path.relative_to(tmp_path).as_posix()),
            "sha256_hex": sha256_hex,
            "size_bytes": len(content),
            "media_type": "png",
            "preview_capability": "previewable",
            "origin_kind": "uploaded",
            "last_observed_origin_kind": "uploaded",
            "provenance_job_name": "Wedding Shoot",
            "provenance_original_filename": "IMG_0001.PNG",
            "first_cataloged_at_utc": catalog_payload["items"][0]["first_cataloged_at_utc"],
            "last_cataloged_at_utc": catalog_payload["items"][0]["last_cataloged_at_utc"],
            "extraction_status": "succeeded",
            "extraction_last_attempted_at_utc": catalog_payload["items"][0][
                "extraction_last_attempted_at_utc"
            ],
            "extraction_last_succeeded_at_utc": catalog_payload["items"][0][
                "extraction_last_succeeded_at_utc"
            ],
            "extraction_last_failed_at_utc": None,
            "extraction_failure_detail": None,
            "preview_status": "pending",
            "preview_relative_path": None,
            "preview_last_attempted_at_utc": None,
            "preview_last_succeeded_at_utc": None,
            "preview_last_failed_at_utc": None,
            "preview_failure_detail": None,
            "is_favorite": False,
            "is_archived": False,
            "capture_timestamp_utc": None,
            "camera_make": None,
            "camera_model": None,
            "image_width": 1,
            "image_height": 1,
            "orientation": None,
            "lens_model": None,
            "exposure_time_s": None,
            "f_number": None,
            "iso_speed": None,
            "focal_length_mm": None,
            "focal_length_35mm_mm": None,
            "is_rejected": False,
        }
    ]


def test_upload_verify_populates_exif_metadata_when_available(tmp_path: Path) -> None:
    store = InMemoryUploadStateStore()
    content = _jpeg_with_exif_bytes()
    sha256_hex = hashlib.sha256(content).hexdigest()
    client = TestClient(create_app(state_store=store, storage_root=tmp_path, bootstrap_token="bootstrap-123"))
    auth_headers = _approve_upload_client(client)

    upload_response = client.put(
        f"/v1/upload/content/{sha256_hex}",
        content=content,
        headers=_with_auth_headers(
            _upload_headers(
                size_bytes=len(content),
                job_name="EXIF Upload",
                original_filename="IMG_4321.JPG",
            ),
            auth_headers,
        ),
    )
    assert upload_response.status_code == 200
    verify_response = client.post(
        "/v1/upload/verify",
        headers=auth_headers,
        json={"sha256_hex": sha256_hex, "size_bytes": len(content)},
    )
    assert verify_response.status_code == 200
    assert verify_response.json()["status"] == "VERIFIED"

    catalog_response = client.get("/v1/admin/catalog")
    assert catalog_response.status_code == 200
    payload = catalog_response.json()
    assert payload["total"] == 1
    item = payload["items"][0]
    assert item["extraction_status"] == "succeeded"
    assert item["capture_timestamp_utc"] == "2026-04-21T12:15:16+00:00"
    assert item["camera_make"] == "Canon"
    assert item["camera_model"] == "EOS R6"
    assert item["orientation"] == 6
    assert item["lens_model"] == "RF24-70mm F2.8 L IS USM"
    assert item["image_width"] == 7
    assert item["image_height"] == 5
    assert item["extraction_last_attempted_at_utc"] is not None
    assert item["extraction_last_succeeded_at_utc"] is not None
    assert item["extraction_last_failed_at_utc"] is None
    assert item["extraction_failure_detail"] is None

    # A repeated verify should be a no-op for extraction metadata.
    repeat_verify_response = client.post(
        "/v1/upload/verify",
        headers=auth_headers,
        json={"sha256_hex": sha256_hex, "size_bytes": len(content)},
    )
    assert repeat_verify_response.status_code == 200
    assert repeat_verify_response.json()["status"] == "ALREADY_EXISTS"
    repeat_catalog_item = client.get("/v1/admin/catalog").json()["items"][0]
    assert repeat_catalog_item["capture_timestamp_utc"] == "2026-04-21T12:15:16+00:00"
    assert repeat_catalog_item["camera_make"] == "Canon"
    assert repeat_catalog_item["camera_model"] == "EOS R6"
    assert repeat_catalog_item["orientation"] == 6
    assert repeat_catalog_item["lens_model"] == "RF24-70mm F2.8 L IS USM"
    assert repeat_catalog_item["image_width"] == 7
    assert repeat_catalog_item["image_height"] == 5


def test_verify_handles_filename_collision_with_deterministic_sha_suffix(tmp_path: Path) -> None:
    store = InMemoryUploadStateStore()
    content = b"new-content"
    sha256_hex = hashlib.sha256(content).hexdigest()
    client = TestClient(create_app(state_store=store, storage_root=tmp_path, bootstrap_token="bootstrap-123"))
    auth_headers = _approve_upload_client(client)

    upload_response = client.put(
        f"/v1/upload/content/{sha256_hex}",
        content=content,
        headers=_with_auth_headers(
            _upload_headers(size_bytes=len(content), job_name="Trip 1", original_filename="photo.jpg"),
            auth_headers,
        ),
    )
    assert upload_response.status_code == 200
    temp_record = store.get_temp_upload(sha256_hex)
    assert temp_record is not None
    received = datetime.fromisoformat(temp_record.received_at_utc)
    base_dir = tmp_path / f"{received.year:04d}" / f"{received.month:02d}" / "Trip_1"
    base_dir.mkdir(parents=True, exist_ok=True)
    (base_dir / "photo.jpg").write_bytes(b"other-content")

    verify_response = client.post(
        "/v1/upload/verify",
        headers=auth_headers,
        json={"sha256_hex": sha256_hex, "size_bytes": len(content)},
    )
    assert verify_response.status_code == 200
    assert verify_response.json()["status"] == "VERIFIED"
    assert (base_dir / f"photo__{sha256_hex[:12]}.jpg").read_bytes() == content


def test_storage_index_registers_existing_files_and_reindex_is_idempotent(tmp_path: Path) -> None:
    content = TINY_PNG
    sha256_hex = hashlib.sha256(content).hexdigest()
    first = tmp_path / "2026" / "04" / "Job_A" / "a.png"
    second = tmp_path / "2026" / "04" / "Job_B" / "b.png"
    first.parent.mkdir(parents=True, exist_ok=True)
    second.parent.mkdir(parents=True, exist_ok=True)
    first.write_bytes(content)
    second.write_bytes(content)

    client = TestClient(create_app(storage_root=tmp_path, bootstrap_token="bootstrap-123"))
    auth_headers = _approve_upload_client(client)
    first_index = client.post("/v1/storage/index")
    assert first_index.status_code == 200
    assert first_index.json() == {
        "scanned_files": 2,
        "indexed_files": 2,
        "new_sha_entries": 1,
        "existing_sha_matches": 1,
        "path_conflicts": 0,
        "errors": 0,
    }

    handshake_response = client.post(
        "/v1/upload/metadata-handshake",
        headers=auth_headers,
        json={"files": [{"client_file_id": 5, "sha256_hex": sha256_hex, "size_bytes": len(content)}]},
    )
    assert handshake_response.status_code == 200
    assert handshake_response.json()["results"][0]["decision"] == "ALREADY_EXISTS"

    second_index = client.post("/v1/storage/index")
    assert second_index.status_code == 200
    assert second_index.json() == {
        "scanned_files": 2,
        "indexed_files": 2,
        "new_sha_entries": 0,
        "existing_sha_matches": 2,
        "path_conflicts": 0,
        "errors": 0,
    }

    catalog_response = client.get("/v1/admin/catalog")
    assert catalog_response.status_code == 200
    catalog_payload = catalog_response.json()
    assert catalog_payload["total"] == 2
    assert [item["relative_path"] for item in catalog_payload["items"]] == [
        "2026/04/Job_A/a.png",
        "2026/04/Job_B/b.png",
    ]
    assert all(item["origin_kind"] == "indexed" for item in catalog_payload["items"])
    assert all(item["last_observed_origin_kind"] == "indexed" for item in catalog_payload["items"])
    assert all(item["extraction_status"] == "succeeded" for item in catalog_payload["items"])
    assert all(item["image_width"] == 1 for item in catalog_payload["items"])
    assert all(item["image_height"] == 1 for item in catalog_payload["items"])


def test_storage_index_records_extraction_failure_without_invalidating_catalog(tmp_path: Path) -> None:
    bad_path = tmp_path / "2026" / "04" / "Job_A" / "note.txt"
    bad_path.parent.mkdir(parents=True, exist_ok=True)
    bad_path.write_text("not-an-image", encoding="utf-8")

    client = TestClient(create_app(storage_root=tmp_path, bootstrap_token="bootstrap-123"))
    response = client.post("/v1/storage/index")
    assert response.status_code == 200
    assert response.json()["indexed_files"] == 1
    assert response.json()["errors"] == 0

    catalog_response = client.get("/v1/admin/catalog")
    assert catalog_response.status_code == 200
    payload = catalog_response.json()
    assert payload["total"] == 1
    item = payload["items"][0]
    assert item["relative_path"] == "2026/04/Job_A/note.txt"
    assert item["extraction_status"] == "failed"
    assert item["extraction_last_failed_at_utc"] is not None
    assert "unsupported media format for extraction" in str(item["extraction_failure_detail"])


def test_storage_index_records_invalid_jpeg_extraction_failure_without_invalidating_catalog(
    tmp_path: Path,
) -> None:
    bad_path = tmp_path / "2026" / "04" / "Job_A" / "broken.jpg"
    bad_path.parent.mkdir(parents=True, exist_ok=True)
    bad_path.write_bytes(b"not-a-real-jpeg")

    client = TestClient(create_app(storage_root=tmp_path, bootstrap_token="bootstrap-123"))
    response = client.post("/v1/storage/index")
    assert response.status_code == 200
    assert response.json()["indexed_files"] == 1
    assert response.json()["errors"] == 0

    catalog_response = client.get("/v1/admin/catalog")
    assert catalog_response.status_code == 200
    payload = catalog_response.json()
    assert payload["total"] == 1
    item = payload["items"][0]
    assert item["relative_path"] == "2026/04/Job_A/broken.jpg"
    assert item["extraction_status"] == "failed"
    assert item["extraction_last_failed_at_utc"] is not None
    assert "invalid media content for extraction" in str(item["extraction_failure_detail"])
    assert item["capture_timestamp_utc"] is None
    assert item["camera_make"] is None
    assert item["camera_model"] is None
    assert item["orientation"] is None
    assert item["lens_model"] is None
    assert item["image_width"] is None
    assert item["image_height"] is None


def test_storage_index_populates_exif_metadata_via_shared_extraction_path(tmp_path: Path) -> None:
    content = _jpeg_with_exif_bytes(
        width=9,
        height=4,
        capture_timestamp="2026:04:21 10:11:12",
        capture_offset="-05:00",
        camera_make="NIKON CORPORATION",
        camera_model="NIKON Zf",
        orientation=1,
        lens_model="NIKKOR Z 40mm f/2",
    )
    first = tmp_path / "2026" / "04" / "Job_A" / "exif-a.jpg"
    second = tmp_path / "2026" / "04" / "Job_B" / "exif-b.jpg"
    first.parent.mkdir(parents=True, exist_ok=True)
    second.parent.mkdir(parents=True, exist_ok=True)
    first.write_bytes(content)
    second.write_bytes(content)

    client = TestClient(create_app(storage_root=tmp_path))
    first_index = client.post("/v1/storage/index")
    assert first_index.status_code == 200
    second_index = client.post("/v1/storage/index")
    assert second_index.status_code == 200

    catalog_response = client.get("/v1/admin/catalog")
    assert catalog_response.status_code == 200
    payload = catalog_response.json()
    assert payload["total"] == 2
    for item in payload["items"]:
        assert item["extraction_status"] == "succeeded"
        assert item["capture_timestamp_utc"] == "2026-04-21T15:11:12+00:00"
        assert item["camera_make"] == "NIKON CORPORATION"
        assert item["camera_model"] == "NIKON Zf"
        assert item["orientation"] == 1
        assert item["lens_model"] == "NIKKOR Z 40mm f/2"
        assert item["image_width"] == 9
        assert item["image_height"] == 4


def test_admin_retry_extraction_for_failed_asset_succeeds_after_file_is_fixed(tmp_path: Path) -> None:
    broken = tmp_path / "2026" / "04" / "Job_A" / "broken.jpg"
    broken.parent.mkdir(parents=True, exist_ok=True)
    broken.write_bytes(b"not-a-real-jpeg")
    client = TestClient(create_app(storage_root=tmp_path))

    first_index = client.post("/v1/storage/index")
    assert first_index.status_code == 200
    failed_item = client.get("/v1/admin/catalog").json()["items"][0]
    assert failed_item["extraction_status"] == "failed"
    first_failure_detail = failed_item["extraction_failure_detail"]
    assert first_failure_detail is not None

    broken.write_bytes(
        _jpeg_with_exif_bytes(
            width=12,
            height=8,
            capture_timestamp="2026:04:22 07:08:09",
            capture_offset="+00:00",
            camera_make="SONY",
            camera_model="ILCE-7M4",
            orientation=1,
            lens_model="FE 35mm F1.8",
        )
    )
    retry_response = client.post(
        "/v1/admin/catalog/extraction/retry",
        json={"relative_path": "2026/04/Job_A/broken.jpg"},
    )
    assert retry_response.status_code == 200
    retried_item = retry_response.json()["item"]
    assert retried_item["extraction_status"] == "succeeded"
    assert retried_item["extraction_failure_detail"] is None
    assert retried_item["camera_make"] == "SONY"
    assert retried_item["camera_model"] == "ILCE-7M4"
    assert retried_item["lens_model"] == "FE 35mm F1.8"
    assert retried_item["image_width"] == 12
    assert retried_item["image_height"] == 8
    assert retried_item["extraction_last_succeeded_at_utc"] is not None
    assert retried_item["extraction_last_failed_at_utc"] is None

    catalog_item = client.get("/v1/admin/catalog").json()["items"][0]
    assert catalog_item["extraction_status"] == "succeeded"
    assert catalog_item["extraction_failure_detail"] is None


def test_admin_retry_extraction_for_invalid_asset_stays_failed_and_updates_failure_detail(
    tmp_path: Path, monkeypatch
) -> None:
    broken = tmp_path / "2026" / "04" / "Job_A" / "broken.jpg"
    broken.parent.mkdir(parents=True, exist_ok=True)
    broken.write_bytes(b"still-invalid")
    client = TestClient(create_app(storage_root=tmp_path))

    first_index = client.post("/v1/storage/index")
    assert first_index.status_code == 200
    before = client.get("/v1/admin/catalog").json()["items"][0]
    assert before["extraction_status"] == "failed"
    assert before["extraction_last_attempted_at_utc"] is not None

    def _forced_failure(_: Path) -> dict[str, str | int | None]:
        raise ValueError("forced retry failure detail")

    monkeypatch.setattr(app_module, "_extract_media_metadata", _forced_failure)
    retry_response = client.post(
        "/v1/admin/catalog/extraction/retry",
        json={"relative_path": "2026/04/Job_A/broken.jpg"},
    )
    assert retry_response.status_code == 200
    after = retry_response.json()["item"]
    assert after["extraction_status"] == "failed"
    assert after["extraction_failure_detail"] == "forced retry failure detail"
    assert after["extraction_last_attempted_at_utc"] is not None
    assert after["extraction_last_failed_at_utc"] is not None
    assert after["extraction_last_succeeded_at_utc"] is None

    before_attempted = datetime.fromisoformat(before["extraction_last_attempted_at_utc"])
    after_attempted = datetime.fromisoformat(after["extraction_last_attempted_at_utc"])
    assert after_attempted >= before_attempted


def test_admin_backfill_processes_pending_and_failed_assets_via_shared_extraction_path(
    tmp_path: Path,
) -> None:
    pending_path = "2026/04/Job_A/pending.jpg"
    failed_path = "2026/04/Job_A/failed.jpg"
    pending_file = tmp_path / pending_path
    failed_file = tmp_path / failed_path
    pending_file.parent.mkdir(parents=True, exist_ok=True)
    pending_content = _jpeg_with_exif_bytes(camera_make="FUJIFILM", camera_model="X-T5")
    failed_content = _jpeg_with_exif_bytes(camera_make="Nikon", camera_model="Z 6II")
    pending_file.write_bytes(pending_content)
    failed_file.write_bytes(failed_content)

    pending_sha = hashlib.sha256(pending_content).hexdigest()
    failed_sha = hashlib.sha256(failed_content).hexdigest()
    now = "2026-04-22T08:00:00+00:00"
    store = InMemoryUploadStateStore()
    store.mark_sha_verified(pending_sha)
    store.mark_sha_verified(failed_sha)
    store.upsert_stored_file(
        relative_path=pending_path,
        sha256_hex=pending_sha,
        size_bytes=len(pending_content),
        source_kind="index_scan",
        seen_at_utc=now,
    )
    store.upsert_media_asset(
        relative_path=pending_path,
        sha256_hex=pending_sha,
        size_bytes=len(pending_content),
        origin_kind="indexed",
        observed_at_utc=now,
    )
    store.upsert_stored_file(
        relative_path=failed_path,
        sha256_hex=failed_sha,
        size_bytes=len(failed_content),
        source_kind="index_scan",
        seen_at_utc=now,
    )
    store.upsert_media_asset(
        relative_path=failed_path,
        sha256_hex=failed_sha,
        size_bytes=len(failed_content),
        origin_kind="indexed",
        observed_at_utc=now,
    )
    store.upsert_media_asset_extraction(
        relative_path=failed_path,
        extraction_status="failed",
        attempted_at_utc=now,
        succeeded_at_utc=None,
        failed_at_utc=now,
        failure_detail="old failure",
        capture_timestamp_utc=None,
        camera_make=None,
        camera_model=None,
        image_width=None,
        image_height=None,
        orientation=None,
        lens_model=None,
        recorded_at_utc=now,
    )

    client = TestClient(create_app(state_store=store, storage_root=tmp_path))
    response = client.post(
        "/v1/admin/catalog/extraction/backfill",
        json={"target_statuses": ["pending", "failed"], "limit": 10},
    )
    assert response.status_code == 200
    payload = response.json()
    run = payload["run"]
    assert run["backfill_kind"] == "extraction"
    assert run["requested_statuses"] == ["pending", "failed"]
    assert run["selected_count"] == 2
    assert run["processed_count"] == 2
    assert run["succeeded_count"] == 2
    assert run["failed_count"] == 0
    assert run["remaining_pending_count"] == 0
    assert run["remaining_failed_count"] == 0
    assert run["completed_at_utc"] is not None
    assert all(item["extraction_status"] == "succeeded" for item in payload["items"])

    catalog_response = client.get("/v1/admin/catalog")
    assert catalog_response.status_code == 200
    catalog_by_path = {item["relative_path"]: item for item in catalog_response.json()["items"]}
    assert catalog_by_path[pending_path]["extraction_status"] == "succeeded"
    assert catalog_by_path[failed_path]["extraction_status"] == "succeeded"
    assert catalog_by_path[pending_path]["camera_make"] == "FUJIFILM"
    assert catalog_by_path[failed_path]["camera_model"] == "Z 6II"

    latest_runs = client.get("/v1/admin/catalog/backfill/latest")
    assert latest_runs.status_code == 200
    latest_payload = latest_runs.json()
    assert latest_payload["extraction_run"] is not None
    assert latest_payload["extraction_run"]["selected_count"] == 2
    assert latest_payload["preview_run"] is None


def test_admin_backfill_is_sane_for_repeated_runs(tmp_path: Path) -> None:
    file_path = tmp_path / "2026" / "04" / "Job_A" / "one.jpg"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    content = _jpeg_with_exif_bytes()
    file_path.write_bytes(content)

    client = TestClient(create_app(storage_root=tmp_path))
    first_index = client.post("/v1/storage/index")
    assert first_index.status_code == 200
    first_backfill = client.post("/v1/admin/catalog/extraction/backfill", json={})
    assert first_backfill.status_code == 200
    assert first_backfill.json()["run"]["selected_count"] == 0
    assert first_backfill.json()["run"]["processed_count"] == 0

    retry_response = client.post(
        "/v1/admin/catalog/extraction/retry",
        json={"relative_path": "2026/04/Job_A/one.jpg"},
    )
    assert retry_response.status_code == 200
    assert retry_response.json()["item"]["extraction_status"] == "succeeded"

    second_backfill = client.post("/v1/admin/catalog/extraction/backfill", json={})
    assert second_backfill.status_code == 200
    assert second_backfill.json()["run"]["selected_count"] == 0
    assert second_backfill.json()["run"]["processed_count"] == 0


def test_admin_extraction_backfill_can_redo_succeeded_assets(tmp_path: Path) -> None:
    file_path = tmp_path / "2026" / "04" / "Job_A" / "one.jpg"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    content = _jpeg_with_exif_bytes(camera_make="Sony", camera_model="A7 IV")
    file_path.write_bytes(content)

    client = TestClient(create_app(storage_root=tmp_path))
    assert client.post("/v1/storage/index").status_code == 200
    retry_response = client.post(
        "/v1/admin/catalog/extraction/retry",
        json={"relative_path": "2026/04/Job_A/one.jpg"},
    )
    assert retry_response.status_code == 200
    assert retry_response.json()["item"]["extraction_status"] == "succeeded"

    backfill = client.post(
        "/v1/admin/catalog/extraction/backfill",
        json={"target_statuses": ["succeeded"], "limit": 10},
    )
    assert backfill.status_code == 200
    run = backfill.json()["run"]
    assert run["requested_statuses"] == ["succeeded"]
    assert run["selected_count"] == 1
    assert run["processed_count"] == 1
    assert run["succeeded_count"] == 1
    assert run["failed_count"] == 0


def test_admin_preview_backfill_processes_pending_and_failed_assets_with_filters(tmp_path: Path) -> None:
    pending_path = "2026/04/Job_A/pending.jpg"
    failed_path = "2026/04/Job_A/failed.jpg"
    skipped_path = "2026/04/Job_A/skipped.jpg"
    now = "2026-04-22T08:00:00+00:00"

    pending_bytes = _jpeg_with_exif_bytes(width=11, height=7)
    failed_bytes = _jpeg_with_exif_bytes(width=13, height=9)
    skipped_bytes = _jpeg_with_exif_bytes(width=15, height=10)
    for relative_path, payload in (
        (pending_path, pending_bytes),
        (failed_path, failed_bytes),
        (skipped_path, skipped_bytes),
    ):
        target = tmp_path / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)

    store = InMemoryUploadStateStore()
    for relative_path, payload, origin_kind in (
        (pending_path, pending_bytes, "indexed"),
        (failed_path, failed_bytes, "indexed"),
        (skipped_path, skipped_bytes, "uploaded"),
    ):
        sha256_hex = hashlib.sha256(payload).hexdigest()
        store.mark_sha_verified(sha256_hex)
        store.upsert_stored_file(
            relative_path=relative_path,
            sha256_hex=sha256_hex,
            size_bytes=len(payload),
            source_kind="index_scan",
            seen_at_utc=now,
        )
        store.upsert_media_asset(
            relative_path=relative_path,
            sha256_hex=sha256_hex,
            size_bytes=len(payload),
            origin_kind=origin_kind,
            observed_at_utc=now,
        )
    store.upsert_media_asset_preview(
        relative_path=failed_path,
        preview_status="failed",
        preview_relative_path=None,
        attempted_at_utc=now,
        succeeded_at_utc=None,
        failed_at_utc=now,
        failure_detail="old preview failure",
        recorded_at_utc=now,
    )

    client = TestClient(create_app(state_store=store, storage_root=tmp_path))
    response = client.post(
        "/v1/admin/catalog/preview/backfill",
        json={"target_statuses": ["pending", "failed"], "limit": 10, "origin_kind": "indexed"},
    )
    assert response.status_code == 200
    payload = response.json()
    run = payload["run"]
    assert run["backfill_kind"] == "preview"
    assert run["selected_count"] == 2
    assert run["processed_count"] == 2
    assert run["succeeded_count"] == 2
    assert run["failed_count"] == 0
    assert run["remaining_pending_count"] == 0
    assert run["remaining_failed_count"] == 0
    assert all(item["preview_status"] == "succeeded" for item in payload["items"])

    catalog = client.get("/v1/admin/catalog").json()["items"]
    by_path = {item["relative_path"]: item for item in catalog}
    assert by_path[pending_path]["preview_status"] == "succeeded"
    assert by_path[failed_path]["preview_status"] == "succeeded"
    assert by_path[skipped_path]["preview_status"] == "pending"

    latest_runs = client.get("/v1/admin/catalog/backfill/latest")
    assert latest_runs.status_code == 200
    latest_payload = latest_runs.json()
    assert latest_payload["preview_run"] is not None
    assert latest_payload["preview_run"]["selected_count"] == 2
    assert latest_payload["extraction_run"] is None


def test_admin_preview_backfill_can_redo_succeeded_assets(tmp_path: Path) -> None:
    file_path = tmp_path / "2026" / "04" / "Job_A" / "one.jpg"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    content = _jpeg_with_exif_bytes(width=12, height=8)
    file_path.write_bytes(content)

    client = TestClient(create_app(storage_root=tmp_path))
    assert client.post("/v1/storage/index").status_code == 200
    assert (
        client.post(
            "/v1/admin/catalog/extraction/retry",
            json={"relative_path": "2026/04/Job_A/one.jpg"},
        ).status_code
        == 200
    )
    preview_retry = client.post(
        "/v1/admin/catalog/preview/retry",
        json={"relative_path": "2026/04/Job_A/one.jpg"},
    )
    assert preview_retry.status_code == 200
    assert preview_retry.json()["item"]["preview_status"] == "succeeded"

    backfill = client.post(
        "/v1/admin/catalog/preview/backfill",
        json={"target_statuses": ["succeeded"], "limit": 10},
    )
    assert backfill.status_code == 200
    run = backfill.json()["run"]
    assert run["requested_statuses"] == ["succeeded"]
    assert run["selected_count"] == 1
    assert run["processed_count"] == 1
    assert run["succeeded_count"] == 1
    assert run["failed_count"] == 0


def test_mixed_uploaded_and_indexed_assets_converge_after_preview_backfill(tmp_path: Path) -> None:
    upload_bytes = _jpeg_with_exif_bytes(width=10, height=6, camera_make="Canon")
    upload_sha = hashlib.sha256(upload_bytes).hexdigest()
    indexed_path = tmp_path / "2026" / "04" / "Job_B" / "indexed.jpg"
    indexed_path.parent.mkdir(parents=True, exist_ok=True)
    indexed_path.write_bytes(_jpeg_with_exif_bytes(width=8, height=5, camera_make="Nikon"))

    client = TestClient(create_app(storage_root=tmp_path, bootstrap_token="bootstrap-123"))
    auth_headers = _approve_upload_client(client)

    upload_response = client.put(
        f"/v1/upload/content/{upload_sha}",
        content=upload_bytes,
        headers=_with_auth_headers(
            _upload_headers(
                size_bytes=len(upload_bytes),
                job_name="Mixed Upload",
                original_filename="upload.jpg",
            ),
            auth_headers,
        ),
    )
    assert upload_response.status_code == 200
    verify_response = client.post(
        "/v1/upload/verify",
        headers=auth_headers,
        json={"sha256_hex": upload_sha, "size_bytes": len(upload_bytes)},
    )
    assert verify_response.status_code == 200
    assert verify_response.json()["status"] == "VERIFIED"

    index_response = client.post("/v1/storage/index")
    assert index_response.status_code == 200

    pending_before = client.get("/v1/admin/catalog?preview_status=pending")
    assert pending_before.status_code == 200
    assert pending_before.json()["total"] >= 2

    backfill_response = client.post(
        "/v1/admin/catalog/preview/backfill",
        json={"target_statuses": ["pending"], "limit": 10, "preview_capability": "previewable"},
    )
    assert backfill_response.status_code == 200
    backfill_run = backfill_response.json()["run"]
    assert backfill_run["succeeded_count"] >= 2
    assert backfill_run["remaining_pending_count"] == 0

    catalog_response = client.get("/v1/admin/catalog")
    assert catalog_response.status_code == 200
    items = catalog_response.json()["items"]
    uploaded_items = [item for item in items if item["origin_kind"] == "uploaded"]
    indexed_items = [item for item in items if item["origin_kind"] == "indexed"]
    assert uploaded_items
    assert indexed_items
    for item in uploaded_items + indexed_items:
        assert item["extraction_status"] == "succeeded"
        assert item["preview_status"] == "succeeded"
        assert item["preview_relative_path"] is not None


def test_preview_backfill_failed_rows_stay_visible_and_retryable(tmp_path: Path) -> None:
    unsupported_path = tmp_path / "2026" / "04" / "Job_A" / "notes.txt"
    unsupported_path.parent.mkdir(parents=True, exist_ok=True)
    unsupported_path.write_text("not previewable", encoding="utf-8")
    client = TestClient(create_app(storage_root=tmp_path))

    index_response = client.post("/v1/storage/index")
    assert index_response.status_code == 200

    backfill = client.post(
        "/v1/admin/catalog/preview/backfill",
        json={
            "target_statuses": ["pending"],
            "limit": 10,
            "preview_capability": "not_previewable",
        },
    )
    assert backfill.status_code == 200
    run = backfill.json()["run"]
    assert run["selected_count"] == 1
    assert run["succeeded_count"] == 0
    assert run["failed_count"] == 1
    assert run["remaining_failed_count"] == 1

    failed_listing = client.get("/v1/admin/catalog?preview_status=failed&preview_capability=not_previewable")
    assert failed_listing.status_code == 200
    assert failed_listing.json()["total"] == 1
    assert failed_listing.json()["items"][0]["relative_path"] == "2026/04/Job_A/notes.txt"

    retry_response = client.post(
        "/v1/admin/catalog/preview/retry",
        json={"relative_path": "2026/04/Job_A/notes.txt"},
    )
    assert retry_response.status_code == 200
    assert retry_response.json()["item"]["preview_status"] == "failed"
    assert retry_response.json()["item"]["preview_last_attempted_at_utc"] is not None


def test_admin_retry_preview_generates_cache_and_detail_visibility_for_still_image(tmp_path: Path) -> None:
    image_path = tmp_path / "2026" / "04" / "Job_A" / "preview.jpg"
    image_path.parent.mkdir(parents=True, exist_ok=True)
    image_path.write_bytes(_jpeg_with_exif_bytes(width=20, height=10))

    client = TestClient(create_app(storage_root=tmp_path))
    index_response = client.post("/v1/storage/index")
    assert index_response.status_code == 200

    before = client.get("/v1/admin/catalog/asset", params={"relative_path": "2026/04/Job_A/preview.jpg"})
    assert before.status_code == 200
    assert before.json()["item"]["preview_status"] == "pending"

    retry_response = client.post(
        "/v1/admin/catalog/preview/retry",
        json={"relative_path": "2026/04/Job_A/preview.jpg"},
    )
    assert retry_response.status_code == 200
    item = retry_response.json()["item"]
    assert item["preview_status"] == "succeeded"
    assert item["preview_relative_path"]
    assert item["preview_last_attempted_at_utc"] is not None
    assert item["preview_last_succeeded_at_utc"] is not None
    assert item["preview_last_failed_at_utc"] is None
    assert item["preview_failure_detail"] is None

    preview_cache_root = tmp_path.parent / ".photovault_preview_cache"
    preview_file = preview_cache_root / str(item["preview_relative_path"])
    assert preview_file.is_file()
    assert preview_file.suffix.lower() == ".jpg"

    preview_response = client.get(
        "/v1/admin/catalog/preview",
        params={"relative_path": "2026/04/Job_A/preview.jpg"},
    )
    assert preview_response.status_code == 200
    assert preview_response.headers["content-type"] == "image/jpeg"

    detail_response = client.get(
        "/v1/admin/catalog/asset",
        params={"relative_path": "2026/04/Job_A/preview.jpg"},
    )
    assert detail_response.status_code == 200
    assert detail_response.json()["item"]["preview_status"] == "succeeded"
    assert detail_response.json()["item"]["preview_relative_path"] == item["preview_relative_path"]


def test_admin_retry_preview_generates_cache_for_heic_via_explicit_converter(
    tmp_path: Path,
    monkeypatch,
) -> None:
    image_path = tmp_path / "2026" / "04" / "Job_A" / "preview.heic"
    image_path.parent.mkdir(parents=True, exist_ok=True)
    image_path.write_bytes(b"fake-heic-content")

    converted_bytes = _jpeg_with_exif_bytes(width=18, height=12)

    def _fake_find_executable(name: str) -> str | None:
        if name == "heif-convert":
            return "/usr/local/bin/heif-convert"
        return None

    def _fake_run_external_command(command: list[str]):
        assert command[0] == "/usr/local/bin/heif-convert"
        converted_path = Path(command[2])
        converted_path.write_bytes(converted_bytes)
        return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

    monkeypatch.setattr(app_module, "_find_executable", _fake_find_executable)
    monkeypatch.setattr(app_module, "_run_external_command", _fake_run_external_command)

    client = TestClient(create_app(storage_root=tmp_path))
    index_response = client.post("/v1/storage/index")
    assert index_response.status_code == 200

    retry_response = client.post(
        "/v1/admin/catalog/preview/retry",
        json={"relative_path": "2026/04/Job_A/preview.heic"},
    )
    assert retry_response.status_code == 200
    item = retry_response.json()["item"]
    assert item["preview_status"] == "succeeded"
    assert (
        item["preview_relative_path"]
        == "2026/04/Job_A/preview__" + item["sha256_hex"][:12] + "__w1024.jpg"
    )
    assert item["preview_last_failed_at_utc"] is None
    assert item["preview_failure_detail"] is None

    preview_response = client.get(
        "/v1/admin/catalog/preview",
        params={"relative_path": "2026/04/Job_A/preview.heic"},
    )
    assert preview_response.status_code == 200
    assert preview_response.headers["content-type"] == "image/jpeg"


def test_admin_retry_preview_persists_explicit_heic_backend_failure_when_converter_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    image_path = tmp_path / "2026" / "04" / "Job_A" / "unsupported.heic"
    image_path.parent.mkdir(parents=True, exist_ok=True)
    image_path.write_bytes(b"not-really-heic")

    monkeypatch.setattr(app_module, "_find_executable", lambda name: None)

    client = TestClient(create_app(storage_root=tmp_path))
    index_response = client.post("/v1/storage/index")
    assert index_response.status_code == 200

    retry_response = client.post(
        "/v1/admin/catalog/preview/retry",
        json={"relative_path": "2026/04/Job_A/unsupported.heic"},
    )
    assert retry_response.status_code == 200
    item = retry_response.json()["item"]
    assert item["preview_status"] == "failed"
    assert item["preview_last_failed_at_utc"] is not None
    assert "HEIC preview backend unavailable" in str(item["preview_failure_detail"])


def test_admin_retry_preview_generates_cache_for_raw_via_embedded_preview(
    tmp_path: Path,
    monkeypatch,
) -> None:
    image_path = tmp_path / "2026" / "04" / "Job_A" / "raw.cr3"
    image_path.parent.mkdir(parents=True, exist_ok=True)
    image_path.write_bytes(b"fake-raw-content")

    monkeypatch.setattr(
        app_module,
        "_extract_raw_embedded_preview_bytes",
        lambda path: _jpeg_with_exif_bytes(width=22, height=14),
    )

    client = TestClient(create_app(storage_root=tmp_path))
    index_response = client.post("/v1/storage/index")
    assert index_response.status_code == 200

    retry_response = client.post(
        "/v1/admin/catalog/preview/retry",
        json={"relative_path": "2026/04/Job_A/raw.cr3"},
    )
    assert retry_response.status_code == 200
    item = retry_response.json()["item"]
    assert item["preview_status"] == "succeeded"
    assert item["preview_relative_path"] == "2026/04/Job_A/raw__" + item["sha256_hex"][:12] + "__w1024.jpg"
    assert item["preview_failure_detail"] is None


def test_admin_retry_preview_generates_cache_for_raw_via_libraw_fallback_when_embedded_preview_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    image_path = tmp_path / "2026" / "04" / "Job_A" / "raw.raf"
    image_path.parent.mkdir(parents=True, exist_ok=True)
    image_path.write_bytes(b"fake-raf-content")

    rendered_image = Image.new("RGB", (24, 16), color=(50, 90, 130))

    def _raise_missing_embedded_preview(path: Path) -> bytes:
        raise ValueError("RAW embedded preview unavailable: no embedded preview data found")

    def _fake_render_via_libraw(path: Path) -> Image.Image:
        assert Path(path) == image_path
        return rendered_image.copy()

    monkeypatch.setattr(app_module, "_extract_raw_embedded_preview_bytes", _raise_missing_embedded_preview)
    monkeypatch.setattr(app_module, "_render_raw_preview_source_via_libraw", _fake_render_via_libraw)

    client = TestClient(create_app(storage_root=tmp_path))
    index_response = client.post("/v1/storage/index")
    assert index_response.status_code == 200

    retry_response = client.post(
        "/v1/admin/catalog/preview/retry",
        json={"relative_path": "2026/04/Job_A/raw.raf"},
    )
    assert retry_response.status_code == 200
    item = retry_response.json()["item"]
    assert item["preview_status"] == "succeeded"
    assert item["preview_relative_path"] == "2026/04/Job_A/raw__" + item["sha256_hex"][:12] + "__w1024.jpg"
    assert item["preview_failure_detail"] is None


def test_admin_retry_preview_honors_configured_max_long_edge(tmp_path: Path) -> None:
    image_path = tmp_path / "2026" / "04" / "Job_A" / "large.jpg"
    image_path.parent.mkdir(parents=True, exist_ok=True)
    large_image = Image.new("RGB", (4096, 1024), color=(12, 34, 56))
    large_image.save(image_path, format="JPEG")

    client = TestClient(create_app(storage_root=tmp_path, preview_max_long_edge=2048))
    index_response = client.post("/v1/storage/index")
    assert index_response.status_code == 200

    retry_response = client.post(
        "/v1/admin/catalog/preview/retry",
        json={"relative_path": "2026/04/Job_A/large.jpg"},
    )
    assert retry_response.status_code == 200
    item = retry_response.json()["item"]
    assert item["preview_status"] == "succeeded"
    assert item["preview_relative_path"] == "2026/04/Job_A/large__" + item["sha256_hex"][:12] + "__w2048.jpg"

    preview_cache_root = tmp_path.parent / ".photovault_preview_cache"
    preview_file = preview_cache_root / str(item["preview_relative_path"])
    assert preview_file.is_file()
    with Image.open(preview_file) as preview_image:
        assert max(preview_image.size) == 2048
        assert preview_image.size == (2048, 512)


def test_admin_catalog_preview_generates_and_reuses_small_variant(tmp_path: Path) -> None:
    image_path = tmp_path / "2026" / "04" / "Job_A" / "large.jpg"
    image_path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", (4096, 1024), color=(12, 34, 56)).save(image_path, format="JPEG")

    client = TestClient(create_app(storage_root=tmp_path, preview_max_long_edge=2048))
    index_response = client.post("/v1/storage/index")
    assert index_response.status_code == 200

    retry_response = client.post(
        "/v1/admin/catalog/preview/retry",
        json={"relative_path": "2026/04/Job_A/large.jpg"},
    )
    assert retry_response.status_code == 200
    item = retry_response.json()["item"]
    assert item["preview_relative_path"] == "2026/04/Job_A/large__" + item["sha256_hex"][:12] + "__w2048.jpg"

    preview_cache_root = tmp_path.parent / ".photovault_preview_cache"
    large_preview_file = preview_cache_root / str(item["preview_relative_path"])
    assert large_preview_file.is_file()

    preview_response = client.get(
        "/v1/admin/catalog/preview",
        params={"relative_path": "2026/04/Job_A/large.jpg", "max_long_edge": 200},
    )
    assert preview_response.status_code == 200
    assert preview_response.headers["content-type"] == "image/jpeg"

    small_preview_file = (
        preview_cache_root / f"2026/04/Job_A/large__{item['sha256_hex'][:12]}__w200.jpg"
    )
    assert small_preview_file.is_file()
    with Image.open(small_preview_file) as preview_image:
        assert max(preview_image.size) == 200
        assert preview_image.size == (200, 38)

    small_preview_mtime = small_preview_file.stat().st_mtime_ns
    second_preview_response = client.get(
        "/v1/admin/catalog/preview",
        params={"relative_path": "2026/04/Job_A/large.jpg", "max_long_edge": 200},
    )
    assert second_preview_response.status_code == 200
    assert small_preview_file.stat().st_mtime_ns == small_preview_mtime


def test_admin_retry_preview_passthrough_suffix_serves_original_file(tmp_path: Path) -> None:
    image_path = tmp_path / "2026" / "04" / "Job_A" / "passthrough.jpg"
    image_path.parent.mkdir(parents=True, exist_ok=True)
    original_bytes = _jpeg_with_exif_bytes(width=64, height=32)
    image_path.write_bytes(original_bytes)

    client = TestClient(create_app(storage_root=tmp_path, preview_passthrough_suffixes={".jpg"}))
    index_response = client.post("/v1/storage/index")
    assert index_response.status_code == 200

    retry_response = client.post(
        "/v1/admin/catalog/preview/retry",
        json={"relative_path": "2026/04/Job_A/passthrough.jpg"},
    )
    assert retry_response.status_code == 200
    item = retry_response.json()["item"]
    assert item["preview_status"] == "succeeded"
    assert item["preview_relative_path"] is None
    assert item["preview_failure_detail"] is None

    preview_response = client.get(
        "/v1/admin/catalog/preview",
        params={"relative_path": "2026/04/Job_A/passthrough.jpg"},
    )
    assert preview_response.status_code == 200
    assert preview_response.headers["content-type"] == "image/jpeg"
    assert preview_response.content == original_bytes

    small_preview_response = client.get(
        "/v1/admin/catalog/preview",
        params={"relative_path": "2026/04/Job_A/passthrough.jpg", "max_long_edge": 200},
    )
    assert small_preview_response.status_code == 200
    assert small_preview_response.headers["content-type"] == "image/jpeg"
    assert small_preview_response.content == original_bytes

    preview_cache_root = tmp_path.parent / ".photovault_preview_cache"
    expected_cached = list(
        preview_cache_root.rglob(f"passthrough__{item['sha256_hex'][:12]}__w*.jpg")
    )
    assert expected_cached == []


def test_admin_retry_preview_placeholder_suffix_skips_generation(tmp_path: Path) -> None:
    image_path = tmp_path / "2026" / "04" / "Job_A" / "placeholder.jpg"
    image_path.parent.mkdir(parents=True, exist_ok=True)
    image_path.write_bytes(_jpeg_with_exif_bytes(width=40, height=20))

    client = TestClient(create_app(storage_root=tmp_path, preview_placeholder_suffixes={".jpg"}))
    index_response = client.post("/v1/storage/index")
    assert index_response.status_code == 200

    retry_response = client.post(
        "/v1/admin/catalog/preview/retry",
        json={"relative_path": "2026/04/Job_A/placeholder.jpg"},
    )
    assert retry_response.status_code == 200
    item = retry_response.json()["item"]
    assert item["preview_status"] == "failed"
    assert item["preview_relative_path"] is None
    assert "skipped by configuration for suffix: .jpg" in str(item["preview_failure_detail"])

    preview_response = client.get(
        "/v1/admin/catalog/preview",
        params={"relative_path": "2026/04/Job_A/placeholder.jpg"},
    )
    assert preview_response.status_code == 404


def test_admin_retry_preview_persists_failure_for_raw_embedded_preview_errors(
    tmp_path: Path,
    monkeypatch,
) -> None:
    image_path = tmp_path / "2026" / "04" / "Job_A" / "raw_missing_preview.nef"
    image_path.parent.mkdir(parents=True, exist_ok=True)
    image_path.write_bytes(b"fake-raw-content-without-embedded-preview")

    def _raise_missing_embedded_preview(path: Path) -> bytes:
        raise ValueError("RAW embedded preview unavailable: no embedded preview data found")

    def _raise_libraw_unavailable(path: Path) -> Image.Image:
        raise ValueError("libraw fallback unavailable: rawpy is not installed")

    monkeypatch.setattr(app_module, "_extract_raw_embedded_preview_bytes", _raise_missing_embedded_preview)
    monkeypatch.setattr(app_module, "_render_raw_preview_source_via_libraw", _raise_libraw_unavailable)

    client = TestClient(create_app(storage_root=tmp_path))
    index_response = client.post("/v1/storage/index")
    assert index_response.status_code == 200

    retry_response = client.post(
        "/v1/admin/catalog/preview/retry",
        json={"relative_path": "2026/04/Job_A/raw_missing_preview.nef"},
    )
    assert retry_response.status_code == 200
    item = retry_response.json()["item"]
    assert item["preview_status"] == "failed"
    assert item["preview_last_failed_at_utc"] is not None
    assert "RAW embedded preview unavailable" in str(item["preview_failure_detail"])
    assert "libraw fallback unavailable" in str(item["preview_failure_detail"])


def test_admin_retry_preview_persists_failure_for_unsupported_media(tmp_path: Path) -> None:
    bad_path = tmp_path / "2026" / "04" / "Job_A" / "notes.txt"
    bad_path.parent.mkdir(parents=True, exist_ok=True)
    bad_path.write_text("not an image", encoding="utf-8")

    client = TestClient(create_app(storage_root=tmp_path))
    index_response = client.post("/v1/storage/index")
    assert index_response.status_code == 200

    retry_response = client.post(
        "/v1/admin/catalog/preview/retry",
        json={"relative_path": "2026/04/Job_A/notes.txt"},
    )
    assert retry_response.status_code == 200
    item = retry_response.json()["item"]
    assert item["preview_status"] == "failed"
    assert item["preview_last_failed_at_utc"] is not None
    assert "unsupported media format for preview" in str(item["preview_failure_detail"])

    preview_response = client.get(
        "/v1/admin/catalog/preview",
        params={"relative_path": "2026/04/Job_A/notes.txt"},
    )
    assert preview_response.status_code == 404


def test_admin_retry_preview_is_sane_for_repeated_runs(tmp_path: Path) -> None:
    image_path = tmp_path / "2026" / "04" / "Job_A" / "repeat.png"
    image_path.parent.mkdir(parents=True, exist_ok=True)
    image_path.write_bytes(TINY_PNG)

    client = TestClient(create_app(storage_root=tmp_path))
    index_response = client.post("/v1/storage/index")
    assert index_response.status_code == 200

    first = client.post(
        "/v1/admin/catalog/preview/retry",
        json={"relative_path": "2026/04/Job_A/repeat.png"},
    )
    assert first.status_code == 200
    first_item = first.json()["item"]
    assert first_item["preview_status"] == "succeeded"
    first_preview_path = first_item["preview_relative_path"]
    assert first_preview_path is not None

    second = client.post(
        "/v1/admin/catalog/preview/retry",
        json={"relative_path": "2026/04/Job_A/repeat.png"},
    )
    assert second.status_code == 200
    second_item = second.json()["item"]
    assert second_item["preview_status"] == "succeeded"
    assert second_item["preview_relative_path"] == first_preview_path


def test_admin_retry_preview_is_sane_for_repeated_raw_runs(tmp_path: Path, monkeypatch) -> None:
    image_path = tmp_path / "2026" / "04" / "Job_A" / "repeat.arw"
    image_path.parent.mkdir(parents=True, exist_ok=True)
    image_path.write_bytes(b"fake-raw-content")

    monkeypatch.setattr(
        app_module,
        "_extract_raw_embedded_preview_bytes",
        lambda path: _jpeg_with_exif_bytes(width=30, height=20),
    )

    client = TestClient(create_app(storage_root=tmp_path))
    index_response = client.post("/v1/storage/index")
    assert index_response.status_code == 200

    first = client.post(
        "/v1/admin/catalog/preview/retry",
        json={"relative_path": "2026/04/Job_A/repeat.arw"},
    )
    assert first.status_code == 200
    first_item = first.json()["item"]
    first_preview_path = first_item["preview_relative_path"]
    assert first_item["preview_status"] == "succeeded"
    assert first_preview_path is not None

    second = client.post(
        "/v1/admin/catalog/preview/retry",
        json={"relative_path": "2026/04/Job_A/repeat.arw"},
    )
    assert second.status_code == 200
    second_item = second.json()["item"]
    assert second_item["preview_status"] == "succeeded"
    assert second_item["preview_relative_path"] == first_preview_path

def test_storage_index_counts_same_path_conflict_and_updates_metadata(tmp_path: Path) -> None:
    original_content = b"original"
    replacement_content = b"replacement"
    original_sha = hashlib.sha256(original_content).hexdigest()
    replacement_sha = hashlib.sha256(replacement_content).hexdigest()
    target = tmp_path / "2026" / "04" / "Job_A" / "photo.jpg"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(original_content)

    store = InMemoryUploadStateStore()
    client = TestClient(create_app(state_store=store, storage_root=tmp_path))

    first_index = client.post("/v1/storage/index")
    assert first_index.status_code == 200
    assert first_index.json() == {
        "scanned_files": 1,
        "indexed_files": 1,
        "new_sha_entries": 1,
        "existing_sha_matches": 0,
        "path_conflicts": 0,
        "errors": 0,
    }
    assert store.get_stored_file_by_path("2026/04/Job_A/photo.jpg") is not None
    assert store.get_stored_file_by_path("2026/04/Job_A/photo.jpg").sha256_hex == original_sha

    target.write_bytes(replacement_content)

    second_index = client.post("/v1/storage/index")
    assert second_index.status_code == 200
    assert second_index.json() == {
        "scanned_files": 1,
        "indexed_files": 1,
        "new_sha_entries": 1,
        "existing_sha_matches": 0,
        "path_conflicts": 1,
        "errors": 0,
    }

    updated_record = store.get_stored_file_by_path("2026/04/Job_A/photo.jpg")
    assert updated_record is not None
    assert updated_record.sha256_hex == replacement_sha
    assert updated_record.size_bytes == len(replacement_content)


def test_storage_index_ignores_temp_uploads_content(tmp_path: Path) -> None:
    stored_content = b"persisted"
    temp_content = b"temporary"
    stored_sha = hashlib.sha256(stored_content).hexdigest()
    real_file = tmp_path / "2026" / "04" / "Job_A" / "a.jpg"
    temp_file = tmp_path / ".temp_uploads" / "ignored.upload"
    real_file.parent.mkdir(parents=True, exist_ok=True)
    temp_file.parent.mkdir(parents=True, exist_ok=True)
    real_file.write_bytes(stored_content)
    temp_file.write_bytes(temp_content)

    client = TestClient(create_app(storage_root=tmp_path, bootstrap_token="bootstrap-123"))
    auth_headers = _approve_upload_client(client)
    response = client.post("/v1/storage/index")

    assert response.status_code == 200
    assert response.json() == {
        "scanned_files": 1,
        "indexed_files": 1,
        "new_sha_entries": 1,
        "existing_sha_matches": 0,
        "path_conflicts": 0,
        "errors": 0,
    }

    handshake_response = client.post(
        "/v1/upload/metadata-handshake",
        headers=auth_headers,
        json={"files": [{"client_file_id": 9, "sha256_hex": stored_sha, "size_bytes": len(stored_content)}]},
    )
    assert handshake_response.status_code == 200
    assert handshake_response.json()["results"][0]["decision"] == "ALREADY_EXISTS"


def test_storage_index_records_partial_scan_errors_without_aborting(tmp_path: Path, monkeypatch) -> None:
    good_content = b"good"
    bad_content = b"bad"
    good_file = tmp_path / "2026" / "04" / "Job_A" / "good.jpg"
    bad_file = tmp_path / "2026" / "04" / "Job_A" / "bad.jpg"
    good_file.parent.mkdir(parents=True, exist_ok=True)
    good_file.write_bytes(good_content)
    bad_file.write_bytes(bad_content)

    original_compute_sha256 = app_module._compute_sha256

    def _flaky_compute_sha256(path: Path) -> str:
        if path.name == "bad.jpg":
            raise OSError("simulated read failure")
        return original_compute_sha256(path)

    monkeypatch.setattr(app_module, "_compute_sha256", _flaky_compute_sha256)

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post("/v1/storage/index")

    assert response.status_code == 200
    assert response.json() == {
        "scanned_files": 2,
        "indexed_files": 1,
        "new_sha_entries": 1,
        "existing_sha_matches": 0,
        "path_conflicts": 0,
        "errors": 1,
    }


def test_admin_duplicates_returns_duplicate_sha_groups(tmp_path: Path) -> None:
    store = InMemoryUploadStateStore()
    now = "2026-04-21T08:00:00+00:00"
    later = "2026-04-21T09:00:00+00:00"
    store.upsert_stored_file(
        relative_path="2026/04/Job_A/a.jpg",
        sha256_hex="a" * 64,
        size_bytes=10,
        source_kind="index_scan",
        seen_at_utc=now,
    )
    store.upsert_stored_file(
        relative_path="2026/04/Job_B/b.jpg",
        sha256_hex="a" * 64,
        size_bytes=10,
        source_kind="upload_verify",
        seen_at_utc=later,
    )
    store.upsert_stored_file(
        relative_path="2026/04/Job_C/c.jpg",
        sha256_hex="b" * 64,
        size_bytes=11,
        source_kind="index_scan",
        seen_at_utc=later,
    )

    client = TestClient(create_app(state_store=store, storage_root=tmp_path))
    response = client.get("/v1/admin/duplicates")

    assert response.status_code == 200
    assert response.json() == {
        "total": 1,
        "limit": 25,
        "offset": 0,
        "items": [
            {
                "sha256_hex": "a" * 64,
                "file_count": 2,
                "first_seen_at_utc": now,
                "last_seen_at_utc": later,
                "relative_paths": ["2026/04/Job_A/a.jpg", "2026/04/Job_B/b.jpg"],
            }
        ],
    }


def test_admin_path_conflicts_and_latest_run_reflect_index_activity(tmp_path: Path) -> None:
    target = tmp_path / "2026" / "04" / "Job_A" / "photo.jpg"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"first")
    client = TestClient(create_app(storage_root=tmp_path))

    first_index = client.post("/v1/storage/index")
    assert first_index.status_code == 200

    target.write_bytes(b"second")
    second_index = client.post("/v1/storage/index")
    assert second_index.status_code == 200
    assert second_index.json()["path_conflicts"] == 1

    conflict_response = client.get("/v1/admin/path-conflicts")
    assert conflict_response.status_code == 200
    conflict_payload = conflict_response.json()
    assert conflict_payload["total"] == 1
    assert conflict_payload["items"][0]["relative_path"] == "2026/04/Job_A/photo.jpg"
    assert conflict_payload["items"][0]["previous_sha256_hex"] == hashlib.sha256(b"first").hexdigest()
    assert conflict_payload["items"][0]["current_sha256_hex"] == hashlib.sha256(b"second").hexdigest()

    latest_run_response = client.get("/v1/admin/latest-index-run")
    assert latest_run_response.status_code == 200
    latest_run = latest_run_response.json()["latest_run"]
    assert latest_run is not None
    assert latest_run["scanned_files"] == 1
    assert latest_run["indexed_files"] == 1
    assert latest_run["path_conflicts"] == 1
    assert latest_run["errors"] == 0


def test_admin_latest_index_run_is_none_before_any_scan(tmp_path: Path) -> None:
    client = TestClient(create_app(storage_root=tmp_path))
    response = client.get("/v1/admin/latest-index-run")
    assert response.status_code == 200
    assert response.json() == {"latest_run": None}


def test_verify_returns_verify_failed_when_content_not_uploaded(tmp_path: Path) -> None:
    client = TestClient(create_app(storage_root=tmp_path, bootstrap_token="bootstrap-123"))
    auth_headers = _approve_upload_client(client)
    response = client.post(
        "/v1/upload/verify",
        headers=auth_headers,
        json={"sha256_hex": "f" * 64, "size_bytes": 10},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "VERIFY_FAILED"


def test_admin_overview_reports_storage_summary(tmp_path: Path) -> None:
    store = InMemoryUploadStateStore()
    now = datetime.now(UTC).isoformat()
    older = "2020-01-01T00:00:00+00:00"
    store.mark_sha_verified("a" * 64)
    store.mark_sha_verified("b" * 64)
    store.upsert_stored_file(
        relative_path="2026/04/Job_A/a.jpg",
        sha256_hex="a" * 64,
        size_bytes=10,
        source_kind="index_scan",
        seen_at_utc=now,
    )
    store.upsert_stored_file(
        relative_path="2026/04/Job_B/b.jpg",
        sha256_hex="a" * 64,
        size_bytes=11,
        source_kind="upload_verify",
        seen_at_utc=now,
    )
    store.upsert_stored_file(
        relative_path="2026/04/Job_C/c.jpg",
        sha256_hex="b" * 64,
        size_bytes=12,
        source_kind="index_scan",
        seen_at_utc=older,
    )
    client = TestClient(create_app(state_store=store, storage_root=tmp_path))
    response = client.get("/v1/admin/overview")
    assert response.status_code == 200
    assert response.json() == {
        "total_known_sha256": 2,
        "total_stored_files": 3,
        "indexed_files": 2,
        "uploaded_files": 1,
        "duplicate_file_paths": 1,
        "recent_indexed_files_24h": 1,
        "recent_uploaded_files_24h": 1,
        "last_indexed_at_utc": now,
        "last_uploaded_at_utc": now,
    }


def test_admin_files_returns_paged_results(tmp_path: Path) -> None:
    store = InMemoryUploadStateStore()
    t1 = "2026-04-20T10:00:00+00:00"
    t2 = "2026-04-20T11:00:00+00:00"
    t3 = "2026-04-20T12:00:00+00:00"
    store.upsert_stored_file(
        relative_path="2026/04/Job_A/a.jpg",
        sha256_hex="a" * 64,
        size_bytes=100,
        source_kind="index_scan",
        seen_at_utc=t1,
    )
    store.upsert_stored_file(
        relative_path="2026/04/Job_B/b.jpg",
        sha256_hex="b" * 64,
        size_bytes=200,
        source_kind="upload_verify",
        seen_at_utc=t2,
    )
    store.upsert_stored_file(
        relative_path="2026/04/Job_C/c.jpg",
        sha256_hex="c" * 64,
        size_bytes=300,
        source_kind="index_scan",
        seen_at_utc=t3,
    )

    client = TestClient(create_app(state_store=store, storage_root=tmp_path))
    response = client.get("/v1/admin/files?limit=2&offset=1")
    assert response.status_code == 200
    assert response.json() == {
        "total": 3,
        "limit": 2,
        "offset": 1,
        "items": [
            {
                "relative_path": "2026/04/Job_B/b.jpg",
                "sha256_hex": "b" * 64,
                "size_bytes": 200,
                "source_kind": "upload_verify",
                "first_seen_at_utc": t2,
                "last_seen_at_utc": t2,
            },
            {
                "relative_path": "2026/04/Job_A/a.jpg",
                "sha256_hex": "a" * 64,
                "size_bytes": 100,
                "source_kind": "index_scan",
                "first_seen_at_utc": t1,
                "last_seen_at_utc": t1,
            },
        ],
    }


def test_admin_catalog_returns_paged_results(tmp_path: Path) -> None:
    store = InMemoryUploadStateStore()
    t1 = "2026-04-20T10:00:00+00:00"
    t2 = "2026-04-20T11:00:00+00:00"
    store.upsert_stored_file(
        relative_path="2026/04/Job_A/a.jpg",
        sha256_hex="a" * 64,
        size_bytes=100,
        source_kind="upload_verify",
        seen_at_utc=t1,
    )
    store.upsert_media_asset(
        relative_path="2026/04/Job_A/a.jpg",
        sha256_hex="a" * 64,
        size_bytes=100,
        origin_kind="uploaded",
        observed_at_utc=t1,
        provenance_job_name="Job_A",
        provenance_original_filename="a.jpg",
    )
    store.upsert_stored_file(
        relative_path="2026/04/Job_B/b.jpg",
        sha256_hex="b" * 64,
        size_bytes=200,
        source_kind="index_scan",
        seen_at_utc=t2,
    )
    store.upsert_media_asset(
        relative_path="2026/04/Job_B/b.jpg",
        sha256_hex="b" * 64,
        size_bytes=200,
        origin_kind="indexed",
        observed_at_utc=t2,
    )
    store.upsert_media_asset_extraction(
        relative_path="2026/04/Job_B/b.jpg",
        extraction_status="failed",
        attempted_at_utc=t2,
        succeeded_at_utc=None,
        failed_at_utc=t2,
        failure_detail="unsupported media format for extraction: .jpg",
        capture_timestamp_utc=None,
        camera_make=None,
        camera_model=None,
        image_width=None,
        image_height=None,
        orientation=None,
        lens_model=None,
        recorded_at_utc=t2,
    )

    client = TestClient(create_app(state_store=store, storage_root=tmp_path))
    response = client.get("/v1/admin/catalog?limit=1&offset=0")

    assert response.status_code == 200
    assert response.json() == {
        "total": 2,
        "limit": 1,
        "offset": 0,
        "items": [
            {
                "relative_path": "2026/04/Job_B/b.jpg",
                "sha256_hex": "b" * 64,
                "size_bytes": 200,
                "media_type": "jpeg",
                "preview_capability": "previewable",
                "origin_kind": "indexed",
                "last_observed_origin_kind": "indexed",
                "provenance_job_name": None,
                "provenance_original_filename": None,
                "first_cataloged_at_utc": t2,
                "last_cataloged_at_utc": t2,
                "extraction_status": "failed",
                "extraction_last_attempted_at_utc": t2,
                "extraction_last_succeeded_at_utc": None,
                "extraction_last_failed_at_utc": t2,
                "extraction_failure_detail": "unsupported media format for extraction: .jpg",
                "preview_status": "pending",
                "preview_relative_path": None,
                "preview_last_attempted_at_utc": None,
                "preview_last_succeeded_at_utc": None,
                "preview_last_failed_at_utc": None,
                "preview_failure_detail": None,
                "is_favorite": False,
                "is_archived": False,
                "capture_timestamp_utc": None,
                "camera_make": None,
                "camera_model": None,
                "image_width": None,
                "image_height": None,
                "orientation": None,
                "lens_model": None,
                "exposure_time_s": None,
                "f_number": None,
                "iso_speed": None,
                "focal_length_mm": None,
                "focal_length_35mm_mm": None,
                "is_rejected": False,
            }
        ],
    }


def test_admin_catalog_filters_by_extraction_status_and_origin(tmp_path: Path) -> None:
    store = InMemoryUploadStateStore()
    t1 = "2026-04-22T09:00:00+00:00"
    t2 = "2026-04-22T10:00:00+00:00"
    t3 = "2026-04-22T11:00:00+00:00"
    store.upsert_stored_file(
        relative_path="2026/04/Job_A/pending.jpg",
        sha256_hex="a" * 64,
        size_bytes=100,
        source_kind="index_scan",
        seen_at_utc=t1,
    )
    store.upsert_media_asset(
        relative_path="2026/04/Job_A/pending.jpg",
        sha256_hex="a" * 64,
        size_bytes=100,
        origin_kind="indexed",
        observed_at_utc=t1,
    )
    store.upsert_stored_file(
        relative_path="2026/04/Job_A/failed.jpg",
        sha256_hex="b" * 64,
        size_bytes=200,
        source_kind="upload_verify",
        seen_at_utc=t2,
    )
    store.upsert_media_asset(
        relative_path="2026/04/Job_A/failed.jpg",
        sha256_hex="b" * 64,
        size_bytes=200,
        origin_kind="uploaded",
        observed_at_utc=t2,
    )
    store.upsert_media_asset_extraction(
        relative_path="2026/04/Job_A/failed.jpg",
        extraction_status="failed",
        attempted_at_utc=t2,
        succeeded_at_utc=None,
        failed_at_utc=t2,
        failure_detail="invalid media content",
        capture_timestamp_utc=None,
        camera_make=None,
        camera_model=None,
        image_width=None,
        image_height=None,
        orientation=None,
        lens_model=None,
        recorded_at_utc=t2,
    )
    store.upsert_stored_file(
        relative_path="2026/04/Job_A/succeeded.jpg",
        sha256_hex="c" * 64,
        size_bytes=300,
        source_kind="upload_verify",
        seen_at_utc=t3,
    )
    store.upsert_media_asset(
        relative_path="2026/04/Job_A/succeeded.jpg",
        sha256_hex="c" * 64,
        size_bytes=300,
        origin_kind="uploaded",
        observed_at_utc=t3,
    )
    store.upsert_media_asset_extraction(
        relative_path="2026/04/Job_A/succeeded.jpg",
        extraction_status="succeeded",
        attempted_at_utc=t3,
        succeeded_at_utc=t3,
        failed_at_utc=None,
        failure_detail=None,
        capture_timestamp_utc="2026-04-22T10:50:00+00:00",
        camera_make="Canon",
        camera_model="R6",
        image_width=1000,
        image_height=800,
        orientation=1,
        lens_model="RF 24-70",
        recorded_at_utc=t3,
    )

    client = TestClient(create_app(state_store=store, storage_root=tmp_path))
    pending_response = client.get("/v1/admin/catalog?extraction_status=pending")
    assert pending_response.status_code == 200
    assert [item["relative_path"] for item in pending_response.json()["items"]] == [
        "2026/04/Job_A/pending.jpg"
    ]

    failed_response = client.get("/v1/admin/catalog?extraction_status=failed")
    assert failed_response.status_code == 200
    assert [item["relative_path"] for item in failed_response.json()["items"]] == [
        "2026/04/Job_A/failed.jpg"
    ]

    origin_response = client.get("/v1/admin/catalog?origin_kind=indexed")
    assert origin_response.status_code == 200
    assert [item["relative_path"] for item in origin_response.json()["items"]] == [
        "2026/04/Job_A/pending.jpg"
    ]

    media_type_response = client.get("/v1/admin/catalog?media_type=jpeg")
    assert media_type_response.status_code == 200
    assert media_type_response.json()["total"] == 3

    preview_capability_response = client.get("/v1/admin/catalog?preview_capability=previewable")
    assert preview_capability_response.status_code == 200
    assert preview_capability_response.json()["total"] == 3

    preview_failed_response = client.get("/v1/admin/catalog?preview_status=failed")
    assert preview_failed_response.status_code == 200
    assert preview_failed_response.json()["total"] == 0

    mark_favorite = client.post(
        "/v1/admin/catalog/favorite/mark",
        json={"relative_path": "2026/04/Job_A/succeeded.jpg"},
    )
    assert mark_favorite.status_code == 200
    assert mark_favorite.json()["item"]["is_favorite"] is True

    mark_archived = client.post(
        "/v1/admin/catalog/archive/mark",
        json={"relative_path": "2026/04/Job_A/failed.jpg"},
    )
    assert mark_archived.status_code == 200
    assert mark_archived.json()["item"]["is_archived"] is True

    favorite_response = client.get("/v1/admin/catalog?is_favorite=true")
    assert favorite_response.status_code == 200
    assert [item["relative_path"] for item in favorite_response.json()["items"]] == [
        "2026/04/Job_A/succeeded.jpg"
    ]

    archived_response = client.get("/v1/admin/catalog?is_archived=true")
    assert archived_response.status_code == 200
    assert [item["relative_path"] for item in archived_response.json()["items"]] == [
        "2026/04/Job_A/failed.jpg"
    ]


def test_admin_catalog_filters_by_catalog_date_and_pagination(tmp_path: Path) -> None:
    store = InMemoryUploadStateStore()
    t1 = "2026-04-22T09:00:00+00:00"
    t2 = "2026-04-22T10:00:00+00:00"
    t3 = "2026-04-22T11:00:00+00:00"
    for idx, seen_at in enumerate((t1, t2, t3), start=1):
        rel = f"2026/04/Job_A/{idx}.jpg"
        sha = f"{idx:x}" * 64
        store.upsert_stored_file(
            relative_path=rel,
            sha256_hex=sha,
            size_bytes=100 + idx,
            source_kind="index_scan",
            seen_at_utc=seen_at,
        )
        store.upsert_media_asset(
            relative_path=rel,
            sha256_hex=sha,
            size_bytes=100 + idx,
            origin_kind="indexed",
            observed_at_utc=seen_at,
        )

    client = TestClient(create_app(state_store=store, storage_root=tmp_path))
    filtered = client.get("/v1/admin/catalog?cataloged_since_utc=2026-04-22T10:00:00+00:00")
    assert filtered.status_code == 200
    assert filtered.json()["total"] == 2
    assert [item["relative_path"] for item in filtered.json()["items"]] == [
        "2026/04/Job_A/3.jpg",
        "2026/04/Job_A/2.jpg",
    ]

    paged = client.get(
        "/v1/admin/catalog?cataloged_since_utc=2026-04-22T09:00:00+00:00&limit=1&offset=1"
    )
    assert paged.status_code == 200
    assert paged.json()["total"] == 3
    assert [item["relative_path"] for item in paged.json()["items"]] == ["2026/04/Job_A/2.jpg"]


def test_admin_catalog_rejects_invalid_filter_values(tmp_path: Path) -> None:
    client = TestClient(create_app(storage_root=tmp_path))
    bad_status = client.get("/v1/admin/catalog?extraction_status=unknown")
    assert bad_status.status_code == 400
    assert "invalid extraction_status filter" in str(bad_status.json().get("detail"))

    bad_preview_status = client.get("/v1/admin/catalog?preview_status=unknown")
    assert bad_preview_status.status_code == 400
    assert "invalid preview_status filter" in str(bad_preview_status.json().get("detail"))

    bad_origin = client.get("/v1/admin/catalog?origin_kind=other")
    assert bad_origin.status_code == 400
    assert "invalid origin_kind filter" in str(bad_origin.json().get("detail"))

    bad_media_type = client.get("/v1/admin/catalog?media_type=gif")
    assert bad_media_type.status_code == 400
    assert "invalid media_type filter" in str(bad_media_type.json().get("detail"))

    bad_preview_capability = client.get("/v1/admin/catalog?preview_capability=maybe")
    assert bad_preview_capability.status_code == 400
    assert "invalid preview_capability filter" in str(bad_preview_capability.json().get("detail"))

    bad_favorite = client.get("/v1/admin/catalog?is_favorite=maybe")
    assert bad_favorite.status_code == 400
    assert "invalid is_favorite filter" in str(bad_favorite.json().get("detail"))

    bad_archived = client.get("/v1/admin/catalog?is_archived=maybe")
    assert bad_archived.status_code == 400
    assert "invalid is_archived filter" in str(bad_archived.json().get("detail"))


def test_admin_catalog_organization_mark_unmark_endpoints(tmp_path: Path) -> None:
    store = InMemoryUploadStateStore()
    seen_at = "2026-04-22T11:00:00+00:00"
    store.upsert_stored_file(
        relative_path="2026/04/Job_A/a.jpg",
        sha256_hex="a" * 64,
        size_bytes=100,
        source_kind="upload_verify",
        seen_at_utc=seen_at,
    )
    store.upsert_media_asset(
        relative_path="2026/04/Job_A/a.jpg",
        sha256_hex="a" * 64,
        size_bytes=100,
        origin_kind="uploaded",
        observed_at_utc=seen_at,
    )

    client = TestClient(create_app(state_store=store, storage_root=tmp_path))
    mark_favorite = client.post(
        "/v1/admin/catalog/favorite/mark",
        json={"relative_path": "2026/04/Job_A/a.jpg"},
    )
    assert mark_favorite.status_code == 200
    assert mark_favorite.json()["item"]["is_favorite"] is True

    unmark_favorite = client.post(
        "/v1/admin/catalog/favorite/unmark",
        json={"relative_path": "2026/04/Job_A/a.jpg"},
    )
    assert unmark_favorite.status_code == 200
    assert unmark_favorite.json()["item"]["is_favorite"] is False

    mark_archived = client.post(
        "/v1/admin/catalog/archive/mark",
        json={"relative_path": "2026/04/Job_A/a.jpg"},
    )
    assert mark_archived.status_code == 200
    assert mark_archived.json()["item"]["is_archived"] is True

    unmark_archived = client.post(
        "/v1/admin/catalog/archive/unmark",
        json={"relative_path": "2026/04/Job_A/a.jpg"},
    )
    assert unmark_archived.status_code == 200
    assert unmark_archived.json()["item"]["is_archived"] is False


def test_admin_files_empty_state(tmp_path: Path) -> None:
    client = TestClient(create_app(storage_root=tmp_path))
    response = client.get("/v1/admin/files")
    assert response.status_code == 200
    assert response.json() == {"total": 0, "limit": 50, "offset": 0, "items": []}


def test_create_app_requires_storage_root_when_not_provided(monkeypatch) -> None:
    monkeypatch.delenv("PHOTOVAULT_API_STORAGE_ROOT", raising=False)
    try:
        create_app()
        raise AssertionError("expected RuntimeError when storage root is missing")
    except RuntimeError as exc:
        assert "PHOTOVAULT_API_STORAGE_ROOT" in str(exc)


def test_create_app_uses_preview_max_long_edge_from_env(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("PHOTOVAULT_API_STORAGE_ROOT", str(tmp_path))
    monkeypatch.setenv("PHOTOVAULT_API_PREVIEW_MAX_LONG_EDGE", "2048")

    app = create_app()
    assert app.state.preview_max_long_edge == 2048


def test_create_app_rejects_invalid_preview_max_long_edge_env(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("PHOTOVAULT_API_STORAGE_ROOT", str(tmp_path))
    monkeypatch.setenv("PHOTOVAULT_API_PREVIEW_MAX_LONG_EDGE", "0")

    try:
        create_app()
        raise AssertionError("expected RuntimeError for invalid preview max long edge")
    except RuntimeError as exc:
        assert "PHOTOVAULT_API_PREVIEW_MAX_LONG_EDGE" in str(exc)


def test_create_app_reads_preview_suffix_sets_from_env(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("PHOTOVAULT_API_STORAGE_ROOT", str(tmp_path))
    monkeypatch.setenv("PHOTOVAULT_API_PREVIEW_PASSTHROUGH_SUFFIXES", "jpg,.jpeg")
    monkeypatch.setenv("PHOTOVAULT_API_PREVIEW_PLACEHOLDER_SUFFIXES", ".avi, mp4")

    app = create_app()
    assert app.state.preview_passthrough_suffixes == frozenset({".jpg", ".jpeg"})
    assert app.state.preview_placeholder_suffixes == frozenset({".avi", ".mp4"})


def test_create_app_rejects_invalid_preview_suffix_set_env(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("PHOTOVAULT_API_STORAGE_ROOT", str(tmp_path))
    monkeypatch.setenv("PHOTOVAULT_API_PREVIEW_PASSTHROUGH_SUFFIXES", "bad/suffix")

    try:
        create_app()
        raise AssertionError("expected RuntimeError for invalid preview suffix configuration")
    except RuntimeError as exc:
        assert "PHOTOVAULT_API_PREVIEW_PASSTHROUGH_SUFFIXES" in str(exc)


def test_create_app_uses_postgres_store_when_database_url_env_set(monkeypatch, tmp_path: Path) -> None:
    class _FakePostgresStore:
        def __init__(self, *, database_url: str) -> None:
            self.database_url = database_url
            self.initialized = False

        def initialize(self) -> None:
            self.initialized = True

        def has_sha(self, sha256_hex: str) -> bool:
            return False

        def has_shas(self, sha256_hex_values: list[str]) -> set[str]:
            return set()

        def get_temp_upload(self, sha256_hex: str):
            return None

        def upsert_temp_upload(self, **kwargs) -> None:
            return None

        def mark_sha_verified(self, sha256_hex: str) -> bool:
            return False

        def upsert_stored_file(self, **kwargs) -> None:
            return None

        def get_stored_file_by_path(self, relative_path: str):
            return None

        def list_stored_files(self, *, limit: int, offset: int):
            return 0, []

        def list_duplicate_sha_groups(self, *, limit: int, offset: int):
            return 0, []

        def record_path_conflict(self, **kwargs) -> None:
            return None

        def list_path_conflicts(self, *, limit: int, offset: int):
            return 0, []

        def record_storage_index_run(self, record) -> None:
            return None

        def get_latest_storage_index_run(self):
            return None

        def summarize_storage(self):
            class _Summary:
                total_known_sha256 = 0
                total_stored_files = 0
                indexed_files = 0
                uploaded_files = 0
                duplicate_file_paths = 0
                recent_indexed_files_24h = 0
                recent_uploaded_files_24h = 0
                last_indexed_at_utc = None
                last_uploaded_at_utc = None

            return _Summary()

        def remove_temp_upload(self, sha256_hex: str) -> None:
            return None

    monkeypatch.setenv("PHOTOVAULT_API_STORAGE_ROOT", str(tmp_path))
    monkeypatch.setenv("PHOTOVAULT_API_DATABASE_URL", "postgresql://photovault:pw@db/photovault")
    monkeypatch.setattr(app_module, "PostgresUploadStateStore", _FakePostgresStore)

    app = create_app()
    store = app.state.upload_state_store
    assert isinstance(store, _FakePostgresStore)
    assert store.database_url == "postgresql://photovault:pw@db/photovault"
    assert store.initialized is True


def _seed_folder_tree(store: InMemoryUploadStateStore) -> None:
    """Populate a store with assets spread across a year/month/job layout.

    Used by /v1/admin/catalog/folders and prefix-filter tests. We seed the
    store directly via upsert_media_asset so we don't have to drive the
    full upload pipeline just to observe how the folder aggregation works.
    """
    now = "2026-04-22T10:00:00+00:00"
    fixtures = [
        ("2026/04/Job_A/a1.jpg", "a1" * 32),
        ("2026/04/Job_A/a2.jpg", "a2" * 32),
        ("2026/04/Job_B/b1.jpg", "b1" * 32),
        ("2026/03/Job_C/c1.jpg", "c1" * 32),
    ]
    for relative_path, sha in fixtures:
        store.upsert_media_asset(
            relative_path=relative_path,
            sha256_hex=sha,
            size_bytes=1024,
            origin_kind="indexed",
            observed_at_utc=now,
        )


def test_admin_catalog_folders_reports_counts_at_every_depth(tmp_path: Path) -> None:
    store = InMemoryUploadStateStore()
    _seed_folder_tree(store)
    client = TestClient(
        create_app(state_store=store, storage_root=tmp_path, bootstrap_token="bootstrap-123")
    )

    response = client.get("/v1/admin/catalog/folders")
    assert response.status_code == 200
    payload = response.json()
    by_path = {folder["path"]: folder for folder in payload["folders"]}

    # Root year has total_count summing both months.
    assert by_path["2026"] == {"path": "2026", "depth": 1, "direct_count": 0, "total_count": 4}
    # Month nodes aggregate their jobs.
    assert by_path["2026/04"]["total_count"] == 3
    assert by_path["2026/03"]["total_count"] == 1
    # Leaf job folders count direct assets.
    assert by_path["2026/04/Job_A"]["direct_count"] == 2
    assert by_path["2026/04/Job_A"]["total_count"] == 2
    assert by_path["2026/04/Job_B"]["direct_count"] == 1


def test_admin_catalog_prefix_filter_limits_to_subtree(tmp_path: Path) -> None:
    store = InMemoryUploadStateStore()
    _seed_folder_tree(store)
    client = TestClient(
        create_app(state_store=store, storage_root=tmp_path, bootstrap_token="bootstrap-123")
    )

    # Prefix filter is applied as a subtree match: assets in that folder or
    # any descendant folder.
    response = client.get(
        "/v1/admin/catalog", params={"relative_path_prefix": "2026/04/Job_A"}
    )
    assert response.status_code == 200
    payload = response.json()
    paths = sorted(item["relative_path"] for item in payload["items"])
    assert paths == ["2026/04/Job_A/a1.jpg", "2026/04/Job_A/a2.jpg"]
    assert payload["total"] == 2

    # A higher-level prefix pulls in everything under that subtree.
    broader = client.get(
        "/v1/admin/catalog", params={"relative_path_prefix": "2026/04"}
    )
    assert broader.status_code == 200
    assert broader.json()["total"] == 3

    # Trailing slashes are tolerated.
    with_trailing = client.get(
        "/v1/admin/catalog", params={"relative_path_prefix": "2026/04/"}
    )
    assert with_trailing.status_code == 200
    assert with_trailing.json()["total"] == 3


def test_admin_catalog_prefix_filter_rejects_invalid_values(tmp_path: Path) -> None:
    store = InMemoryUploadStateStore()
    _seed_folder_tree(store)
    client = TestClient(
        create_app(state_store=store, storage_root=tmp_path, bootstrap_token="bootstrap-123")
    )

    for bad in ["/etc", "..", "2026/..", "\\system", "foo//bar"]:
        response = client.get(
            "/v1/admin/catalog", params={"relative_path_prefix": bad}
        )
        assert response.status_code == 400, f"expected 400 for prefix {bad!r}"


# ---------------------------------------------------------------------------
# Phase 3.A: Exposure-metadata EXIF extraction coverage.
# ---------------------------------------------------------------------------


def test_normalize_exif_rational_handles_common_exif_shapes() -> None:
    # IFDRational-like objects (anything with __float__) including float/int.
    assert app_module._normalize_exif_rational(2.8) == 2.8
    assert app_module._normalize_exif_rational(200) == 200.0
    # Tuples: (numerator, denominator) — shutter speeds.
    assert app_module._normalize_exif_rational((1, 200)) == 1 / 200
    assert app_module._normalize_exif_rational((0, 0)) is None
    # String forms written by some cameras.
    assert app_module._normalize_exif_rational("1/200") == 1 / 200
    assert app_module._normalize_exif_rational("2.8") == 2.8
    assert app_module._normalize_exif_rational("") is None
    assert app_module._normalize_exif_rational("abc") is None
    assert app_module._normalize_exif_rational(None) is None
    # Booleans are excluded so a stray `True` doesn't turn into 1.0.
    assert app_module._normalize_exif_rational(True) is None


def test_normalize_exif_iso_speed_handles_tuples_and_scalars() -> None:
    assert app_module._normalize_exif_iso_speed(400) == 400
    assert app_module._normalize_exif_iso_speed((400,)) == 400
    assert app_module._normalize_exif_iso_speed((0, 800)) == 800
    assert app_module._normalize_exif_iso_speed("1600") == 1600
    assert app_module._normalize_exif_iso_speed(None) is None
    assert app_module._normalize_exif_iso_speed(True) is None
    assert app_module._normalize_exif_iso_speed(-10) is None


def test_extract_media_metadata_returns_exposure_fields(tmp_path: Path) -> None:
    content = _jpeg_with_exif_bytes(
        exposure_time=(1, 200),
        f_number=(28, 10),
        iso_speed=400,
        focal_length_mm=(50, 1),
        focal_length_35mm=75,
    )
    file_path = tmp_path / "photo.jpg"
    file_path.write_bytes(content)
    metadata = app_module._extract_media_metadata(file_path)
    assert metadata["exposure_time_s"] == 1 / 200
    assert metadata["f_number"] == 2.8
    assert metadata["iso_speed"] == 400
    assert metadata["focal_length_mm"] == 50.0
    assert metadata["focal_length_35mm_mm"] == 75


def test_extract_media_metadata_returns_partial_fields_for_raw_preview(monkeypatch, tmp_path: Path) -> None:
    preview_bytes = _jpeg_with_exif_bytes(
        width=4416,
        height=2944,
        camera_make="FUJIFILM",
        camera_model="X-T5",
        orientation=1,
        capture_timestamp="",
        capture_offset="",
        lens_model="",
    )

    class FakeRawSizes:
        width = 7752
        height = 5178

    class FakeThumbnail:
        format = "jpeg"
        data = preview_bytes

    class FakeRawReader:
        sizes = FakeRawSizes()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def extract_thumb(self):
            return FakeThumbnail()

    class FakeThumbFormat:
        JPEG = "jpeg"

    class FakeRawPyModule:
        ThumbFormat = FakeThumbFormat
        LibRawError = RuntimeError

        @staticmethod
        def imread(path: str) -> FakeRawReader:
            assert path.endswith("photo.raf")
            return FakeRawReader()

    monkeypatch.setitem(sys.modules, "rawpy", FakeRawPyModule())

    file_path = tmp_path / "photo.raf"
    file_path.write_bytes(b"fake-raw-content")

    metadata = app_module._extract_media_metadata(file_path)
    assert metadata["camera_make"] == "FUJIFILM"
    assert metadata["camera_model"] == "X-T5"
    assert metadata["image_width"] == 7752
    assert metadata["image_height"] == 5178
    assert metadata["orientation"] == 1
    assert metadata["capture_timestamp_utc"] is None
    assert metadata["f_number"] is None
    assert metadata["iso_speed"] is None


def test_attempt_media_extraction_logs_failure(tmp_path: Path, caplog) -> None:
    store = InMemoryUploadStateStore()
    store.initialize()
    relative_path = "2026/04/Job_A/broken.raf"
    store.upsert_media_asset(
        relative_path=relative_path,
        sha256_hex="a" * 64,
        size_bytes=123,
        origin_kind="uploaded",
        observed_at_utc="2026-04-29T00:00:00+00:00",
        provenance_job_name="Job_A",
        provenance_original_filename="broken.raf",
    )
    asset_path = tmp_path / relative_path
    asset_path.parent.mkdir(parents=True, exist_ok=True)
    asset_path.write_bytes(b"broken")

    caplog.set_level(logging.WARNING, logger="photovault-api.app")
    media_preview_module.attempt_media_extraction(
        store=store,
        storage_root_path=tmp_path,
        relative_path=relative_path,
        extract_media_metadata=lambda path: (_ for _ in ()).throw(
            ValueError("unsupported media format for extraction: .raf")
        ),
    )

    assert (
        f"media metadata extraction failed for {relative_path}: "
        "unsupported media format for extraction: .raf"
    ) in caplog.text
    item = store.get_media_asset_by_path(relative_path)
    assert item is not None
    assert item.extraction_status == "failed"
    assert item.extraction_failure_detail == "unsupported media format for extraction: .raf"


def test_upload_verify_populates_exposure_fields_on_catalog(tmp_path: Path) -> None:
    store = InMemoryUploadStateStore()
    content = _jpeg_with_exif_bytes(
        exposure_time=(1, 125),
        f_number=(40, 10),
        iso_speed=800,
        focal_length_mm=(35, 1),
        focal_length_35mm=52,
    )
    sha256_hex = hashlib.sha256(content).hexdigest()
    client = TestClient(
        create_app(
            state_store=store, storage_root=tmp_path, bootstrap_token="bootstrap-123"
        )
    )
    auth_headers = _approve_upload_client(client)

    upload_response = client.put(
        f"/v1/upload/content/{sha256_hex}",
        content=content,
        headers=_with_auth_headers(
            _upload_headers(
                size_bytes=len(content),
                job_name="Exposure Upload",
                original_filename="IMG_9999.JPG",
            ),
            auth_headers,
        ),
    )
    assert upload_response.status_code == 200
    verify_response = client.post(
        "/v1/upload/verify",
        headers=auth_headers,
        json={"sha256_hex": sha256_hex, "size_bytes": len(content)},
    )
    assert verify_response.status_code == 200

    item = client.get("/v1/admin/catalog").json()["items"][0]
    assert item["extraction_status"] == "succeeded"
    assert item["exposure_time_s"] == 1 / 125
    assert item["f_number"] == 4.0
    assert item["iso_speed"] == 800
    assert item["focal_length_mm"] == 35.0
    assert item["focal_length_35mm_mm"] == 52

    # Inspect-detail endpoint surfaces the same fields.
    asset_item = client.get(
        "/v1/admin/catalog/asset",
        params={"relative_path": item["relative_path"]},
    ).json()["item"]
    assert asset_item["exposure_time_s"] == 1 / 125
    assert asset_item["f_number"] == 4.0
    assert asset_item["iso_speed"] == 800
    assert asset_item["focal_length_mm"] == 35.0
    assert asset_item["focal_length_35mm_mm"] == 52


def test_upload_verify_handles_missing_exposure_fields_gracefully(tmp_path: Path) -> None:
    # When EXIF exposure tags are absent the pipeline must succeed but emit
    # None for every new field rather than failing extraction.
    store = InMemoryUploadStateStore()
    content = _jpeg_with_exif_bytes()  # no exposure tags requested
    sha256_hex = hashlib.sha256(content).hexdigest()
    client = TestClient(
        create_app(
            state_store=store, storage_root=tmp_path, bootstrap_token="bootstrap-123"
        )
    )
    auth_headers = _approve_upload_client(client)
    client.put(
        f"/v1/upload/content/{sha256_hex}",
        content=content,
        headers=_with_auth_headers(
            _upload_headers(
                size_bytes=len(content),
                job_name="No-exposure",
                original_filename="IMG_5555.JPG",
            ),
            auth_headers,
        ),
    )
    verify_response = client.post(
        "/v1/upload/verify",
        headers=auth_headers,
        json={"sha256_hex": sha256_hex, "size_bytes": len(content)},
    )
    assert verify_response.status_code == 200

    item = client.get("/v1/admin/catalog").json()["items"][0]
    assert item["extraction_status"] == "succeeded"
    assert item["exposure_time_s"] is None
    assert item["f_number"] is None
    assert item["iso_speed"] is None
    assert item["focal_length_mm"] is None
    assert item["focal_length_35mm_mm"] is None


# ---------------------------------------------------------------------------
# Phase 3.B: reject queue (mark/list/unmark + path validation + flag surfacing)
# ---------------------------------------------------------------------------


def _seed_reject_queue_store() -> InMemoryUploadStateStore:
    store = InMemoryUploadStateStore()
    seen_at = "2026-04-22T11:00:00+00:00"
    store.upsert_stored_file(
        relative_path="2026/04/Job_A/a.jpg",
        sha256_hex="a" * 64,
        size_bytes=100,
        source_kind="upload_verify",
        seen_at_utc=seen_at,
    )
    store.upsert_media_asset(
        relative_path="2026/04/Job_A/a.jpg",
        sha256_hex="a" * 64,
        size_bytes=100,
        origin_kind="uploaded",
        observed_at_utc=seen_at,
    )
    store.upsert_stored_file(
        relative_path="2026/04/Job_B/b.jpg",
        sha256_hex="b" * 64,
        size_bytes=200,
        source_kind="upload_verify",
        seen_at_utc=seen_at,
    )
    store.upsert_media_asset(
        relative_path="2026/04/Job_B/b.jpg",
        sha256_hex="b" * 64,
        size_bytes=200,
        origin_kind="uploaded",
        observed_at_utc=seen_at,
    )
    return store


def test_reject_queue_mark_list_unmark_round_trip(tmp_path: Path) -> None:
    store = _seed_reject_queue_store()
    client = TestClient(create_app(state_store=store, storage_root=tmp_path))

    # Before marking, the queue is empty and the catalog shows is_rejected=False.
    list_before = client.get("/v1/admin/catalog/rejects")
    assert list_before.status_code == 200
    assert list_before.json() == {"total": 0, "limit": 50, "offset": 0, "items": []}

    mark = client.post(
        "/v1/admin/catalog/reject",
        json={"relative_path": "2026/04/Job_A/a.jpg", "marked_reason": "blurry"},
    )
    assert mark.status_code == 200
    mark_body = mark.json()
    assert mark_body["relative_path"] == "2026/04/Job_A/a.jpg"
    assert mark_body["sha256_hex"] == "a" * 64
    assert mark_body["is_rejected"] is True
    assert mark_body["marked_reason"] == "blurry"
    assert mark_body["marked_at_utc"]  # ISO string present

    # Queue lists the marked row + embeds the catalog item flagged as rejected.
    list_after = client.get("/v1/admin/catalog/rejects").json()
    assert list_after["total"] == 1
    assert len(list_after["items"]) == 1
    row = list_after["items"][0]
    assert row["relative_path"] == "2026/04/Job_A/a.jpg"
    assert row["sha256_hex"] == "a" * 64
    assert row["marked_reason"] == "blurry"
    assert row["item"] is not None
    assert row["item"]["is_rejected"] is True

    # The catalog list surfaces is_rejected for the marked row and only that row.
    catalog = client.get("/v1/admin/catalog").json()
    flags = {item["relative_path"]: item["is_rejected"] for item in catalog["items"]}
    assert flags["2026/04/Job_A/a.jpg"] is True
    assert flags["2026/04/Job_B/b.jpg"] is False

    # The per-asset endpoint mirrors the flag.
    asset = client.get(
        "/v1/admin/catalog/asset",
        params={"relative_path": "2026/04/Job_A/a.jpg"},
    ).json()
    assert asset["item"]["is_rejected"] is True

    unmark = client.post(
        "/v1/admin/catalog/reject/unmark",
        json={"relative_path": "2026/04/Job_A/a.jpg"},
    )
    assert unmark.status_code == 200
    assert unmark.json() == {
        "relative_path": "2026/04/Job_A/a.jpg",
        "is_rejected": False,
    }

    list_restored = client.get("/v1/admin/catalog/rejects").json()
    assert list_restored["total"] == 0
    assert list_restored["items"] == []


def test_reject_queue_mark_is_idempotent_and_preserves_first_marked(
    tmp_path: Path,
) -> None:
    store = _seed_reject_queue_store()
    client = TestClient(create_app(state_store=store, storage_root=tmp_path))

    first = client.post(
        "/v1/admin/catalog/reject",
        json={"relative_path": "2026/04/Job_A/a.jpg", "marked_reason": "blurry"},
    )
    assert first.status_code == 200
    first_body = first.json()

    second = client.post(
        "/v1/admin/catalog/reject",
        json={"relative_path": "2026/04/Job_A/a.jpg", "marked_reason": "duplicate"},
    )
    assert second.status_code == 200
    second_body = second.json()

    # Idempotent: timestamp stays pinned to the first mark; reason can update.
    assert second_body["marked_at_utc"] == first_body["marked_at_utc"]
    assert second_body["marked_reason"] == "duplicate"
    # Exactly one queue row for this path.
    list_after = client.get("/v1/admin/catalog/rejects").json()
    assert list_after["total"] == 1


def test_reject_queue_mark_missing_asset_returns_404(tmp_path: Path) -> None:
    store = InMemoryUploadStateStore()
    client = TestClient(create_app(state_store=store, storage_root=tmp_path))

    response = client.post(
        "/v1/admin/catalog/reject",
        json={"relative_path": "2026/04/nope/missing.jpg"},
    )
    assert response.status_code == 404
    assert "catalog asset not found" in str(response.json().get("detail"))


def test_reject_queue_unmark_is_idempotent_when_absent(tmp_path: Path) -> None:
    # Unmarking a path that isn't in the queue must be a quiet 200 with
    # is_rejected=False, not a 404 — two reviewers may race on the same triage.
    store = _seed_reject_queue_store()
    client = TestClient(create_app(state_store=store, storage_root=tmp_path))

    response = client.post(
        "/v1/admin/catalog/reject/unmark",
        json={"relative_path": "2026/04/Job_A/a.jpg"},
    )
    assert response.status_code == 200
    assert response.json() == {
        "relative_path": "2026/04/Job_A/a.jpg",
        "is_rejected": False,
    }


def test_reject_queue_rejects_unsafe_relative_paths(tmp_path: Path) -> None:
    store = _seed_reject_queue_store()
    client = TestClient(create_app(state_store=store, storage_root=tmp_path))

    for unsafe in ["../escape", "/leading/slash.jpg", "bad\\segment.jpg", "a/./b.jpg", ""]:
        response = client.post(
            "/v1/admin/catalog/reject",
            json={"relative_path": unsafe} if unsafe else {"relative_path": " "},
        )
        # Empty string triggers Pydantic min_length=1 (422); the other
        # malicious shapes are rejected by _require_safe_relative_path (400).
        assert response.status_code in (400, 422)


def test_reject_queue_list_paginates(tmp_path: Path) -> None:
    store = _seed_reject_queue_store()
    client = TestClient(create_app(state_store=store, storage_root=tmp_path))

    for path in ("2026/04/Job_A/a.jpg", "2026/04/Job_B/b.jpg"):
        assert (
            client.post(
                "/v1/admin/catalog/reject",
                json={"relative_path": path},
            ).status_code
            == 200
        )

    page_one = client.get("/v1/admin/catalog/rejects?limit=1&offset=0").json()
    page_two = client.get("/v1/admin/catalog/rejects?limit=1&offset=1").json()
    assert page_one["total"] == 2
    assert page_two["total"] == 2
    assert len(page_one["items"]) == 1
    assert len(page_two["items"]) == 1
    assert page_one["items"][0]["relative_path"] != page_two["items"][0]["relative_path"]

    # limit bounds are enforced.
    assert client.get("/v1/admin/catalog/rejects?limit=0").status_code == 422
    assert client.get("/v1/admin/catalog/rejects?limit=1000").status_code == 422
    assert client.get("/v1/admin/catalog/rejects?offset=-1").status_code == 422


# ---------------------------------------------------------------------------
# Phase 3.C: execute delete, SHA tombstones, client handshake
# ---------------------------------------------------------------------------


def test_tombstone_created_by_execute_move_and_row_inserted(tmp_path: Path) -> None:
    """Happy path: execute on a single queued path moves the file to .trash,
    removes the api_media_assets row, and inserts a tombstone row with the
    correct sha256_hex."""
    store = _seed_reject_queue_store()
    client = TestClient(create_app(state_store=store, storage_root=tmp_path))

    # Create the physical file inside tmp_path so the move can succeed.
    source_file = tmp_path / "2026" / "04" / "Job_A" / "a.jpg"
    source_file.parent.mkdir(parents=True, exist_ok=True)
    source_file.write_bytes(b"fake-image-data")

    # Mark the asset as rejected first.
    mark = client.post(
        "/v1/admin/catalog/reject",
        json={"relative_path": "2026/04/Job_A/a.jpg", "marked_reason": "blurry"},
    )
    assert mark.status_code == 200

    # Execute the delete.
    resp = client.post(
        "/v1/admin/catalog/rejects/execute",
        json={"relative_paths": ["2026/04/Job_A/a.jpg"]},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "2026/04/Job_A/a.jpg" in body["executed"]
    assert body["skipped"] == []

    # Source file must be gone from the catalog subtree.
    assert not source_file.exists()

    # A file must appear somewhere under .trash/.
    trash_root = tmp_path / ".trash"
    trash_files = list(trash_root.rglob("a.jpg"))
    assert len(trash_files) == 1, f"expected 1 trashed file, found {trash_files}"

    # api_media_assets row must be gone (get_media_asset_by_path returns None).
    assert store.get_media_asset_by_path("2026/04/Job_A/a.jpg") is None

    # Tombstone row must be present with the correct sha.
    assert store.is_sha_tombstoned("a" * 64)
    tombstones = store.list_sha_tombstones(["a" * 64])
    assert len(tombstones) == 1
    assert tombstones[0].sha256_hex == "a" * 64
    assert tombstones[0].relative_path == "2026/04/Job_A/a.jpg"


def test_execute_is_idempotent_for_missing_source_file(tmp_path: Path) -> None:
    """If the source file was already removed out of band, execute still records
    the tombstone and clears the reject-queue row — it does NOT raise an error."""
    store = _seed_reject_queue_store()
    client = TestClient(create_app(state_store=store, storage_root=tmp_path))

    # Mark as rejected — but do NOT create the physical file (simulates OOB deletion).
    mark = client.post(
        "/v1/admin/catalog/reject",
        json={"relative_path": "2026/04/Job_A/a.jpg", "marked_reason": "already gone"},
    )
    assert mark.status_code == 200

    resp = client.post(
        "/v1/admin/catalog/rejects/execute",
        json={"relative_paths": ["2026/04/Job_A/a.jpg"]},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "2026/04/Job_A/a.jpg" in body["executed"]
    assert body["skipped"] == []

    # Tombstone must still be written.
    assert store.is_sha_tombstoned("a" * 64)

    # Queue must be cleared.
    total, rows = store.list_catalog_rejects(limit=50, offset=0)
    paths_in_queue = [r.relative_path for r in rows]
    assert "2026/04/Job_A/a.jpg" not in paths_in_queue


def test_execute_rejects_unsafe_relative_paths(tmp_path: Path) -> None:
    """Path traversal and other unsafe inputs must be rejected with 400/422."""
    store = _seed_reject_queue_store()
    client = TestClient(create_app(state_store=store, storage_root=tmp_path))

    for unsafe in ["../escape.jpg", "/leading/slash.jpg", "bad\\segment.jpg", "a/./b.jpg"]:
        resp = client.post(
            "/v1/admin/catalog/rejects/execute",
            json={"relative_paths": [unsafe]},
        )
        assert resp.status_code in (400, 422), (
            f"expected 400/422 for {unsafe!r}, got {resp.status_code}"
        )


def test_execute_without_request_body_drains_whole_queue(tmp_path: Path) -> None:
    """Posting with an empty body (no relative_paths key) executes ALL queued rejects."""
    store = _seed_reject_queue_store()
    client = TestClient(create_app(state_store=store, storage_root=tmp_path))

    # Mark both assets.
    for path in ("2026/04/Job_A/a.jpg", "2026/04/Job_B/b.jpg"):
        assert (
            client.post("/v1/admin/catalog/reject", json={"relative_path": path}).status_code == 200
        )

    # Execute with no relative_paths supplied (drain all).
    resp = client.post("/v1/admin/catalog/rejects/execute", json={})
    assert resp.status_code == 200
    body = resp.json()
    assert set(body["executed"]) == {"2026/04/Job_A/a.jpg", "2026/04/Job_B/b.jpg"}
    assert body["skipped"] == []

    # Queue must now be empty.
    total, _ = store.list_catalog_rejects(limit=50, offset=0)
    assert total == 0

    # Both SHAs tombstoned.
    assert store.is_sha_tombstoned("a" * 64)
    assert store.is_sha_tombstoned("b" * 64)


def test_execute_removes_deleted_asset_from_duplicate_groups(tmp_path: Path) -> None:
    """Deleting one path from a duplicate SHA group should remove that group
    from /v1/admin/duplicates when only one file remains."""
    store = InMemoryUploadStateStore()
    seen_at = "2026-04-22T11:00:00+00:00"
    shared_sha = "d" * 64
    for relative_path in ("2026/04/Job_A/a.jpg", "2026/04/Job_B/b.jpg"):
        store.upsert_stored_file(
            relative_path=relative_path,
            sha256_hex=shared_sha,
            size_bytes=100,
            source_kind="upload_verify",
            seen_at_utc=seen_at,
        )
        store.upsert_media_asset(
            relative_path=relative_path,
            sha256_hex=shared_sha,
            size_bytes=100,
            origin_kind="uploaded",
            observed_at_utc=seen_at,
        )

    client = TestClient(create_app(state_store=store, storage_root=tmp_path))

    before = client.get("/v1/admin/duplicates")
    assert before.status_code == 200
    assert before.json()["total"] == 1
    assert before.json()["items"][0]["file_count"] == 2

    # Queue one of the duplicates, then execute delete.
    mark = client.post(
        "/v1/admin/catalog/reject",
        json={"relative_path": "2026/04/Job_A/a.jpg"},
    )
    assert mark.status_code == 200
    execute = client.post(
        "/v1/admin/catalog/rejects/execute",
        json={"relative_paths": ["2026/04/Job_A/a.jpg"]},
    )
    assert execute.status_code == 200
    assert execute.json()["executed"] == ["2026/04/Job_A/a.jpg"]

    after = client.get("/v1/admin/duplicates")
    assert after.status_code == 200
    assert after.json()["total"] == 0
    assert after.json()["items"] == []


def test_upload_verify_returns_409_for_tombstoned_sha(tmp_path: Path) -> None:
    store = InMemoryUploadStateStore()
    content = b"test content"
    sha256_hex = hashlib.sha256(content).hexdigest()

    # Pre-seed a tombstone for this SHA.
    store.add_tombstone(
        relative_path="2026/04/Job_A/a.jpg",
        sha256_hex=sha256_hex,
        trashed_at_utc="2026-04-22T10:00:00+00:00",
        marked_reason="test deletion",
        trash_relative_path=".trash/2026/04/22/aabbccdd/2026/04/Job_A/a.jpg",
        original_size_bytes=len(content),
    )

    client = TestClient(
        create_app(state_store=store, storage_root=tmp_path, bootstrap_token="bootstrap-123")
    )
    auth_headers = _approve_upload_client(client)

    # Upload and verify should fail with 409 Conflict.
    verify_response = client.post(
        "/v1/upload/verify",
        headers=auth_headers,
        json={"sha256_hex": sha256_hex, "size_bytes": len(content)},
    )
    assert verify_response.status_code == 409
    body = verify_response.json()
    assert "sha_tombstoned" in str(body.get("detail", {}))


def test_client_tombstone_report_returns_matches_only_for_reported_shas(
    tmp_path: Path,
) -> None:
    store = InMemoryUploadStateStore()
    client = TestClient(create_app(state_store=store, storage_root=tmp_path, bootstrap_token="bootstrap-123"))

    auth_headers = _approve_upload_client(client)

    # Seed two tombstones.
    store.add_tombstone(
        relative_path="2026/04/Job_A/a.jpg",
        sha256_hex="a" * 64,
        trashed_at_utc="2026-04-22T10:00:00+00:00",
        marked_reason="test",
        trash_relative_path=".trash/2026/04/22/aaaa/2026/04/Job_A/a.jpg",
        original_size_bytes=100,
    )
    store.add_tombstone(
        relative_path="2026/04/Job_B/b.jpg",
        sha256_hex="b" * 64,
        trashed_at_utc="2026-04-22T10:00:00+00:00",
        marked_reason="test",
        trash_relative_path=".trash/2026/04/22/bbbb/2026/04/Job_B/b.jpg",
        original_size_bytes=200,
    )

    # Report SHAs: include "a", "b", and a non-existent one.
    response = client.post(
        "/v1/client/tombstone-report",
        headers=auth_headers,
        json={"sha256_hex": ["a" * 64, "c" * 64]},
    )
    assert response.status_code == 200
    body = response.json()
    assert len(body["tombstoned"]) == 1
    assert body["tombstoned"][0]["sha256_hex"] == "a" * 64
    # "b" is not reported since it wasn't queried; "c" is simply not in the DB.


def test_client_tombstone_report_requires_client_auth(tmp_path: Path) -> None:
    store = InMemoryUploadStateStore()
    client = TestClient(
        create_app(state_store=store, storage_root=tmp_path, bootstrap_token="bootstrap-123")
    )

    # No auth headers.
    response = client.post(
        "/v1/client/tombstone-report",
        json={"sha256_hex": ["a" * 64]},
    )
    assert response.status_code == 401


# ---------------------------------------------------------------------------
# Phase 3.D: tombstone list + restore
# ---------------------------------------------------------------------------


def _seed_tombstone_store(tmp_path: Path) -> tuple[InMemoryUploadStateStore, Path]:
    """Create a store with two tombstoned assets and their physical trash files."""
    store = InMemoryUploadStateStore()
    now = "2026-04-10T03:15:00+00:00"  # 14 days before 2026-04-24

    for rel_path, sha, trash_rel in [
        (
            "2026/04/Job_A/a.jpg",
            "a" * 64,
            ".trash/2026/04/10/aaaaaaaaaaaa/2026/04/Job_A/a.jpg",
        ),
        (
            "2026/04/Job_B/b.jpg",
            "b" * 64,
            ".trash/2026/04/10/bbbbbbbbbbbb/2026/04/Job_B/b.jpg",
        ),
    ]:
        trash_path = tmp_path / trash_rel
        trash_path.parent.mkdir(parents=True, exist_ok=True)
        trash_path.write_bytes(b"fake-image-data")

        store.add_tombstone(
            relative_path=rel_path,
            sha256_hex=sha,
            trashed_at_utc=now,
            marked_reason="test",
            trash_relative_path=trash_rel,
            original_size_bytes=100,
        )

    return store, tmp_path


def test_admin_catalog_tombstones_list_returns_rows_sorted_oldest_first(
    tmp_path: Path,
) -> None:
    store = InMemoryUploadStateStore()
    # Insert two tombstones with different trashed_at times.
    store.add_tombstone(
        relative_path="2026/04/Job_B/b.jpg",
        sha256_hex="b" * 64,
        trashed_at_utc="2026-04-12T00:00:00+00:00",
        marked_reason=None,
        trash_relative_path=".trash/2026/04/12/bbb/2026/04/Job_B/b.jpg",
        original_size_bytes=200,
    )
    store.add_tombstone(
        relative_path="2026/04/Job_A/a.jpg",
        sha256_hex="a" * 64,
        trashed_at_utc="2026-04-10T00:00:00+00:00",
        marked_reason=None,
        trash_relative_path=".trash/2026/04/10/aaa/2026/04/Job_A/a.jpg",
        original_size_bytes=100,
    )

    client = TestClient(create_app(state_store=store, storage_root=tmp_path))
    resp = client.get("/v1/admin/catalog/tombstones")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 2
    items = body["items"]
    assert len(items) == 2
    # Oldest first.
    assert items[0]["relative_path"] == "2026/04/Job_A/a.jpg"
    assert items[1]["relative_path"] == "2026/04/Job_B/b.jpg"
    # Both items have age_days and days_until_purge.
    assert "age_days" in items[0]
    assert "days_until_purge" in items[0]


def test_admin_catalog_tombstones_list_filters_by_older_than_days(
    tmp_path: Path,
) -> None:
    store = InMemoryUploadStateStore()
    # "old" row: 20 days ago; "new" row: 5 days ago.
    from datetime import UTC, datetime, timedelta

    old_ts = (datetime.now(UTC) - timedelta(days=20)).isoformat()
    new_ts = (datetime.now(UTC) - timedelta(days=5)).isoformat()

    store.add_tombstone(
        relative_path="2026/04/Job_A/a.jpg",
        sha256_hex="a" * 64,
        trashed_at_utc=old_ts,
        marked_reason=None,
        trash_relative_path=".trash/old/a.jpg",
        original_size_bytes=100,
    )
    store.add_tombstone(
        relative_path="2026/04/Job_B/b.jpg",
        sha256_hex="b" * 64,
        trashed_at_utc=new_ts,
        marked_reason=None,
        trash_relative_path=".trash/new/b.jpg",
        original_size_bytes=200,
    )

    client = TestClient(create_app(state_store=store, storage_root=tmp_path))
    # Filtering for tombstones older than 14 days: only the 20-day one matches.
    resp = client.get("/v1/admin/catalog/tombstones?older_than_days=14")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["items"][0]["relative_path"] == "2026/04/Job_A/a.jpg"


def test_admin_catalog_tombstones_restore_round_trip(tmp_path: Path) -> None:
    """Tombstone a file via execute, then restore via the restore endpoint.

    Verifies:
    - Physical file returns to its original relative_path.
    - api_media_assets row is re-inserted with origin_kind='restored'.
    - Tombstone row is removed.
    """
    store = _seed_reject_queue_store()
    client = TestClient(create_app(state_store=store, storage_root=tmp_path))

    source_file = tmp_path / "2026" / "04" / "Job_A" / "a.jpg"
    source_file.parent.mkdir(parents=True, exist_ok=True)
    source_file.write_bytes(b"fake-image-data")

    # Mark and execute.
    assert (
        client.post(
            "/v1/admin/catalog/reject",
            json={"relative_path": "2026/04/Job_A/a.jpg"},
        ).status_code
        == 200
    )
    exec_resp = client.post(
        "/v1/admin/catalog/rejects/execute",
        json={"relative_paths": ["2026/04/Job_A/a.jpg"]},
    )
    assert exec_resp.status_code == 200
    assert not source_file.exists()
    assert store.is_sha_tombstoned("a" * 64)

    # Restore.
    restore_resp = client.post(
        "/v1/admin/catalog/tombstones/restore",
        json={"relative_path": "2026/04/Job_A/a.jpg"},
    )
    assert restore_resp.status_code == 200
    body = restore_resp.json()
    assert body["restored"] is True
    assert body["relative_path"] == "2026/04/Job_A/a.jpg"
    assert body["sha256_hex"] == "a" * 64

    # Physical file must be back at its original location.
    assert source_file.is_file()
    assert source_file.read_bytes() == b"fake-image-data"

    # api_media_assets row must be back with origin_kind='restored'.
    asset = store.get_media_asset_by_path("2026/04/Job_A/a.jpg")
    assert asset is not None
    assert asset.origin_kind == "restored"

    # Tombstone must be gone — re-upload of the same SHA is now accepted.
    assert not store.is_sha_tombstoned("a" * 64)


def test_admin_catalog_tombstones_restore_returns_409_when_trash_file_missing(
    tmp_path: Path,
) -> None:
    """If the physical trash file is missing, restore returns 409 with trash_gone
    and leaves the tombstone intact (operator must investigate)."""
    store = InMemoryUploadStateStore()
    store.add_tombstone(
        relative_path="2026/04/Job_A/a.jpg",
        sha256_hex="a" * 64,
        trashed_at_utc="2026-04-10T00:00:00+00:00",
        marked_reason=None,
        # Points to a non-existent file.
        trash_relative_path=".trash/2026/04/10/aaa/2026/04/Job_A/a.jpg",
        original_size_bytes=100,
    )

    client = TestClient(create_app(state_store=store, storage_root=tmp_path))
    resp = client.post(
        "/v1/admin/catalog/tombstones/restore",
        json={"relative_path": "2026/04/Job_A/a.jpg"},
    )
    assert resp.status_code == 409
    body = resp.json()
    assert body["detail"]["code"] == "trash_gone"

    # Tombstone must still be intact.
    assert store.is_sha_tombstoned("a" * 64)


def test_admin_catalog_tombstones_restore_rejects_unsafe_relative_paths(
    tmp_path: Path,
) -> None:
    """Path-traversal attempts in restore must be rejected with 400."""
    store = InMemoryUploadStateStore()
    client = TestClient(create_app(state_store=store, storage_root=tmp_path))

    for unsafe in ["../escape.jpg", "/leading/slash.jpg", "bad\\segment.jpg"]:
        resp = client.post(
            "/v1/admin/catalog/tombstones/restore",
            json={"relative_path": unsafe},
        )
        assert resp.status_code in (400, 422), (
            f"expected 400/422 for {unsafe!r}, got {resp.status_code}"
        )


def test_upload_verify_accepts_sha_again_after_restore(tmp_path: Path) -> None:
    """After restoring a tombstoned asset the tombstone row is removed, so
    the strict-permanence rule is explicitly relaxed: a subsequent verify for
    the same SHA must succeed (returning ALREADY_EXISTS) rather than 409.

    This is by design — restore is an operator decision to reverse the
    soft-delete and re-admit the content.
    """
    store = _seed_reject_queue_store()
    # Seed the SHA into known_sha256 so has_sha() returns True after restore,
    # as it would in production after a successful upload+verify cycle.
    store.mark_sha_verified("a" * 64)
    client = TestClient(
        create_app(state_store=store, storage_root=tmp_path, bootstrap_token="bootstrap-123")
    )

    # Approve a client so upload/verify is accessible (standard pattern).
    auth_headers = _approve_upload_client(client, client_id="restore-client")

    # Delete the file via the reject-execute path.
    source_file = tmp_path / "2026" / "04" / "Job_A" / "a.jpg"
    source_file.parent.mkdir(parents=True, exist_ok=True)
    source_file.write_bytes(b"fake-image-data")

    client.post(
        "/v1/admin/catalog/reject",
        json={"relative_path": "2026/04/Job_A/a.jpg"},
    )
    client.post(
        "/v1/admin/catalog/rejects/execute",
        json={"relative_paths": ["2026/04/Job_A/a.jpg"]},
    )
    assert store.is_sha_tombstoned("a" * 64)

    # Verify must now return 409 (tombstoned).
    verify_tombstoned = client.post(
        "/v1/upload/verify",
        headers=auth_headers,
        json={"sha256_hex": "a" * 64, "size_bytes": 100},
    )
    assert verify_tombstoned.status_code == 409

    # Restore the asset.
    restore_resp = client.post(
        "/v1/admin/catalog/tombstones/restore",
        json={"relative_path": "2026/04/Job_A/a.jpg"},
    )
    assert restore_resp.status_code == 200

    # Now verify must succeed (tombstone removed, SHA still known).
    verify_after_restore = client.post(
        "/v1/upload/verify",
        headers=auth_headers,
        json={"sha256_hex": "a" * 64, "size_bytes": 100},
    )
    assert verify_after_restore.status_code == 200
    assert verify_after_restore.json()["status"] == "ALREADY_EXISTS"


def test_unhandled_exception_returns_structured_500_detail(tmp_path: Path, monkeypatch) -> None:
    client = TestClient(create_app(storage_root=tmp_path), raise_server_exceptions=False)

    def _raise_unhandled(*_args, **_kwargs):
        raise RuntimeError("forced unhandled api failure")

    monkeypatch.setattr(app_module, "_require_approved_client", _raise_unhandled)
    response = client.post(
        "/v1/upload/metadata-handshake",
        json={
            "files": [
                {
                    "client_file_id": 1,
                    "sha256_hex": "a" * 64,
                    "size_bytes": 1,
                }
            ]
        },
    )
    assert response.status_code == 500
    detail = response.json()["detail"]
    assert detail["request_id"]
    assert detail["timestamp_utc"]
    assert detail["message"] == "forced unhandled api failure"
    assert isinstance(detail["traceback"], list)
    assert any("forced unhandled api failure" in line for line in detail["traceback"])


def test_admin_reject_execute_logs_start_finish_and_failure_reason(
    tmp_path: Path, monkeypatch, caplog
) -> None:
    store = _seed_reject_queue_store()
    client = TestClient(create_app(state_store=store, storage_root=tmp_path))
    source_file = tmp_path / "2026" / "04" / "Job_A" / "a.jpg"
    source_file.parent.mkdir(parents=True, exist_ok=True)
    source_file.write_bytes(b"fake-image-data")

    mark = client.post(
        "/v1/admin/catalog/reject",
        json={"relative_path": "2026/04/Job_A/a.jpg", "marked_reason": "log-check"},
    )
    assert mark.status_code == 200

    caplog.set_level(logging.INFO, logger="photovault-api.app")
    monkeypatch.setattr(
        app_module.os,
        "replace",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("forced replace failure")),
    )
    monkeypatch.setattr(
        app_module.shutil,
        "copy2",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("forced fallback failure")),
    )

    response = client.post(
        "/v1/admin/catalog/rejects/execute",
        json={"relative_paths": ["2026/04/Job_A/a.jpg"]},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["executed"] == []
    assert payload["skipped"] == ["2026/04/Job_A/a.jpg"]

    assert "admin_reject_execute_started" in caplog.text
    assert "admin_reject_execute_item_failed" in caplog.text
    assert "forced fallback failure" in caplog.text
    assert "admin_reject_execute_finished" in caplog.text


def test_backfill_workflows_log_counts_and_failure_details(tmp_path: Path, caplog) -> None:
    unsupported_path = tmp_path / "2026" / "04" / "Job_A" / "notes.txt"
    unsupported_path.parent.mkdir(parents=True, exist_ok=True)
    unsupported_path.write_text("not previewable", encoding="utf-8")

    client = TestClient(create_app(storage_root=tmp_path))
    index_response = client.post("/v1/storage/index")
    assert index_response.status_code == 200

    caplog.set_level(logging.INFO, logger="photovault-api.app")

    extraction_backfill = client.post(
        "/v1/admin/catalog/extraction/backfill",
        json={
            "target_statuses": ["failed"],
            "limit": 10,
            "preview_capability": "not_previewable",
        },
    )
    assert extraction_backfill.status_code == 200
    extraction_run = extraction_backfill.json()["run"]
    assert extraction_run["selected_count"] == 1
    assert extraction_run["failed_count"] == 1

    preview_backfill = client.post(
        "/v1/admin/catalog/preview/backfill",
        json={
            "target_statuses": ["pending"],
            "limit": 10,
            "preview_capability": "not_previewable",
        },
    )
    assert preview_backfill.status_code == 200
    preview_run = preview_backfill.json()["run"]
    assert preview_run["selected_count"] == 1
    assert preview_run["failed_count"] == 1

    assert "admin_extraction_backfill_started" in caplog.text
    assert "admin_extraction_backfill_finished" in caplog.text
    assert "unsupported media format for extraction" in caplog.text
    assert "admin_preview_backfill_started" in caplog.text
    assert "admin_preview_backfill_finished" in caplog.text
    assert "unsupported media format for preview" in caplog.text
