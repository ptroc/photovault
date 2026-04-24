import httpx
from photovault_client_ui.app import _derive_job_operator_view, _parse_nmcli_multiline, create_app


def _network_snapshot() -> dict[str, object]:
    return {
        "general": {
            "state": "connected",
            "connectivity": "full",
            "wifi": "enabled",
        },
        "devices": [
            {
                "device": "wlan1",
                "type": "wifi",
                "state": "connected",
                "connection": "photovault-ap",
            },
            {
                "device": "wlan0",
                "type": "wifi",
                "state": "connected",
                "connection": "studio-wifi",
            }
        ],
        "wifi_networks": [
            {
                "in_use": "*",
                "ssid": "studio-wifi",
                "signal": "76",
                "security": "WPA2",
                "channel": "40",
                "rate": "540 Mbit/s",
            }
        ],
        "sta_connected": True,
        "ap_device_names": ["wlan1"],
        "sta_device_names": ["wlan0"],
        "sta_connection_names": ["studio-wifi"],
        "local_ap_ready": True,
        "upstream_connectivity": "full",
        "upstream_status": "internet_reachable",
        "upstream_no_usable_internet": False,
        "upstream_internet_reachable": True,
        "captive_portal_detected": False,
        "portal_handoff_active": False,
        "portal_handoff_started_at_utc": None,
        "next_operator_action": "Local AP is available and upstream Internet is reachable.",
    }


def _dependency_snapshot() -> list[dict[str, str]]:
    return [
        {
            "name": "SQLite",
            "status": "ready",
            "detail": "/var/lib/photovault-clientd/state.sqlite3",
        },
        {
            "name": "Storage",
            "status": "ready",
            "detail": "/var/lib/photovault-clientd/staging",
        },
        {
            "name": "photovault-clientd.service",
            "status": "active",
            "detail": "local daemon API at http://127.0.0.1:9101",
        },
        {
            "name": "NetworkManager.service",
            "status": "active",
            "detail": "network connectivity and Wi-Fi control",
        },
        {
            "name": "photovault-api.service",
            "status": "inactive",
            "detail": "server upload and verify API at http://127.0.0.1:9301",
        },
    ]


def _overview_payloads(
    daemon_state: str = "WAIT_NETWORK",
    *,
    server_auth: dict[str, object] | None = None,
) -> dict[str, object]:
    effective_server_auth: dict[str, object] = (
        server_auth
        if server_auth is not None
        else {
            "client_id": "pi-test",
            "display_name": "Pi Test",
            "enrollment_status": "approved",
            "auth_token": "token-1",
            "last_error": None,
        }
    )
    return {
        "/state": {
            "current_state": daemon_state,
            "updated_at_utc": "2026-04-19T16:51:44.120670+00:00",
            "server_auth": effective_server_auth,
        },
        "/diagnostics/m0": {
            "ok": True,
            "invariant_issue_count": 0,
            "pending_bootstrap_entries": 0,
        },
        "/ingest/jobs": {
            "jobs": [
                {
                    "job_id": 1,
                    "media_label": "pi-test-sd",
                    "status": "ERROR_FILE",
                    "local_ingest_complete": True,
                    "upload_pending": True,
                    "status_counts": {
                        "READY_TO_UPLOAD": 2,
                        "UPLOADED": 1,
                        "VERIFIED_REMOTE": 1,
                        "DUPLICATE_SHA_GLOBAL": 1,
                        "ERROR_FILE": 1,
                        "DUPLICATE_SESSION_SHA": 1,
                    },
                },
                {
                    "job_id": 2,
                    "media_label": "camera-2",
                    "status": "WAIT_NETWORK",
                    "local_ingest_complete": False,
                    "upload_pending": True,
                    "status_counts": {
                        "READY_TO_UPLOAD": 4,
                    },
                },
                {
                    "job_id": 4,
                    "media_label": "camera-4",
                    "status": "HASHING",
                    "local_ingest_complete": False,
                    "upload_pending": False,
                    "status_counts": {
                        "STAGED": 2,
                    },
                },
                {
                    "job_id": 3,
                    "media_label": "camera-archive",
                    "status": "JOB_COMPLETE_REMOTE",
                    "local_ingest_complete": True,
                    "upload_pending": False,
                    "status_counts": {
                        "VERIFIED_REMOTE": 6,
                    },
                },
            ]
        },
        "/events?limit=10": {
            "events": [
                {
                    "level": "WARN",
                    "category": "QUEUE_UPLOAD_PREPARED",
                    "created_at_utc": "2026-04-19T16:51:44.120670+00:00",
                    "message": "job_id=1, ready_to_upload=2",
                }
            ]
        },
        "/events?limit=30": {
            "events": [
                {
                    "level": "WARN",
                    "category": "QUEUE_UPLOAD_PREPARED",
                    "created_at_utc": "2026-04-19T16:51:44.120670+00:00",
                    "message": "job_id=1, ready_to_upload=2",
                },
                {
                    "level": "ERROR",
                    "category": "UPLOAD_RETRY",
                    "created_at_utc": "2026-04-19T16:55:44.120670+00:00",
                    "message": "file_id=6, retry=2",
                },
            ]
        },
        "/block-devices": {
            "count": 1,
            "devices": [
                {
                    "name": "sda",
                    "path": "/dev/sda",
                    "size_bytes": 64000000000,
                    "transport": "usb",
                    "removable": True,
                    "vendor": "Generic",
                    "model": "MassStorageClass",
                    "partitions": [
                        {
                            "name": "sda1",
                            "path": "/dev/sda1",
                            "size_bytes": 63847792640,
                            "filesystem_type": "exfat",
                            "filesystem_label": "CARD_A",
                            "filesystem_uuid": "AAAA-BBBB",
                            "current_mountpoints": ["/mnt/sda_1"],
                            "target_mount_path": "/mnt/sda_1",
                            "mount_active": True,
                            "can_mount": False,
                            "can_unmount": True,
                        }
                    ],
                }
            ],
        },
        "/ingest/jobs/1": {
            "job_id": 1,
            "media_label": "pi-test-sd",
            "status": "ERROR_FILE",
            "updated_at_utc": "2026-04-19T16:51:44.120670+00:00",
            "local_ingest_complete": True,
            "upload_pending": True,
            "status_counts": {
                "READY_TO_UPLOAD": 2,
                "UPLOADED": 1,
                "VERIFIED_REMOTE": 1,
                "DUPLICATE_SHA_GLOBAL": 1,
                "ERROR_FILE": 1,
                "DUPLICATE_SESSION_SHA": 1,
            },
            "files": [
                {
                    "file_id": 1,
                    "source_path": "/var/lib/photovault-clientd/test-media/001.jpg",
                    "status": "READY_TO_UPLOAD",
                    "sha256_hex": "abc123",
                    "retry_count": 0,
                    "last_error": None,
                },
                {
                    "file_id": 2,
                    "source_path": "/var/lib/photovault-clientd/test-media/002.jpg",
                    "status": "UPLOADED",
                    "sha256_hex": "ffcc22",
                    "retry_count": 1,
                    "last_error": "verify pending after reconnect",
                },
                {
                    "file_id": 3,
                    "source_path": "/var/lib/photovault-clientd/test-media/003.jpg",
                    "status": "VERIFIED_REMOTE",
                    "sha256_hex": "deadbeef",
                    "retry_count": 0,
                    "last_error": None,
                },
                {
                    "file_id": 4,
                    "source_path": "/var/lib/photovault-clientd/test-media/004.jpg",
                    "status": "DUPLICATE_SHA_GLOBAL",
                    "sha256_hex": "cafebabe",
                    "retry_count": 0,
                    "last_error": None,
                },
                {
                    "file_id": 5,
                    "source_path": "/var/lib/photovault-clientd/test-media/002.jpg",
                    "status": "DUPLICATE_SESSION_SHA",
                    "sha256_hex": "abc123",
                    "retry_count": 0,
                    "last_error": None,
                },
                {
                    "file_id": 6,
                    "source_path": "/var/lib/photovault-clientd/test-media/006.jpg",
                    "status": "ERROR_FILE",
                    "sha256_hex": "deadbeef",
                    "retry_count": 3,
                    "last_error": "server verification failed retries exhausted",
                },
            ],
        },
        "/network/status": {
            "snapshot": {
                "general": {
                    "state": "connected",
                    "connectivity": "full",
                    "wifi": "enabled",
                },
                "devices": [
                    {
                        "device": "wlan0",
                        "type": "wifi",
                        "state": "connected",
                        "connection": "studio-wifi",
                    }
                ],
                "wifi_networks": [
                    {
                        "in_use": "*",
                        "ssid": "studio-wifi",
                        "signal": "76",
                        "security": "WPA2",
                        "channel": "40",
                        "rate": "540 Mbit/s",
                    }
                ],
                "ap_profile": {
                    "profile_name": "photovault-ap",
                    "exists": True,
                    "active": True,
                    "ssid": "photovault-ap",
                    "autoconnect": "yes",
                    "mode": "ap",
                    "key_mgmt": "wpa-psk",
                },
                "sta_connected": True,
                "ap_device_names": ["wlan1"],
                "sta_device_names": ["wlan0"],
                "sta_connection_names": ["studio-wifi"],
                "local_ap_ready": True,
                "upstream_connectivity": "full",
                "upstream_status": "internet_reachable",
                "upstream_no_usable_internet": False,
                "upstream_internet_reachable": True,
                "captive_portal_detected": False,
                "portal_handoff_active": False,
                "portal_handoff_started_at_utc": None,
                "next_operator_action": (
                    "Upstream network is connected. Verify AP settings and continue operations."
                ),
            },
            "ap_config": {
                "profile_name": "photovault-ap",
                "ssid": "photovault-ap",
                "password_set": True,
                "updated_at_utc": "2026-04-19T16:51:44.120670+00:00",
                "last_applied_at_utc": "2026-04-19T16:51:44.120670+00:00",
                "last_apply_error": None,
            },
        },
        "/network/ap-config": {
            "profile_name": "photovault-ap",
            "ssid": "photovault-ap",
            "password_set": True,
            "updated_at_utc": "2026-04-19T16:51:44.120670+00:00",
            "last_applied_at_utc": "2026-04-19T16:51:44.120670+00:00",
            "last_apply_error": None,
        },
    }


def test_parse_nmcli_multiline_splits_records_without_blank_lines() -> None:
    output = """IN-USE:
SSID: :)
SIGNAL: 89
SECURITY: WPA2
CHAN: 6
RATE: 260 Mbit/s
IN-USE:
SSID: :))
SIGNAL: 89
SECURITY: WPA2
CHAN: 40
RATE: 540 Mbit/s
"""

    records = _parse_nmcli_multiline(output)

    assert len(records) == 2
    assert records[0]["SSID"] == ":)"
    assert records[1]["SSID"] == ":))"


def test_index_route_renders_dashboard_not_raw_tables() -> None:
    payloads = _overview_payloads()

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    app = create_app(
        daemon_get=fake_daemon_get,
        network_snapshot_get=_network_snapshot,
        dependency_snapshot_get=_dependency_snapshot,
    )
    response = app.test_client().get("/")
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert 'href="/static/vendor/bootstrap/css/bootstrap.min.css"' in body
    assert 'src="/static/vendor/bootstrap/js/bootstrap.bundle.min.js"' in body
    assert "nav-link active" in body
    assert ">Overview<" in body
    assert "Operator dashboard" in body
    assert "Top actions" in body
    assert "Attention and blockers" in body
    assert "Recent daemon activity" in body
    assert "Create ingest job" in body
    assert "Source path" in body
    assert "Active job summary" in body
    assert "Waiting for network connectivity" in body
    assert 'Current state: <code class="inline-code">WAIT_NETWORK</code>' in body
    assert "Auto progression active" in body
    assert "Waiting jobs" in body
    assert "Run daemon tick (auto progression active)" in body
    assert "Quick routes" not in body
    assert "disabled" in body
    assert "Ingest Jobs" not in body
    assert "Recent Events" not in body
    assert "Visible Wi-Fi Networks" not in body


def test_overview_surfaces_blocked_state_guidance_for_storage_pause() -> None:
    payloads = _overview_payloads(daemon_state="PAUSED_STORAGE")

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    app = create_app(
        daemon_get=fake_daemon_get,
        network_snapshot_get=_network_snapshot,
        dependency_snapshot_get=_dependency_snapshot,
    )
    body = app.test_client().get("/").get_data(as_text=True)

    assert "Storage health pause" in body
    assert "Restore storage health, then run one daemon tick to resume." in body
    assert "Resolve blocked conditions first, then run one daemon tick to confirm recovery." in body


def test_overview_surfaces_pending_client_auth_block_guidance() -> None:
    payloads = _overview_payloads(
        server_auth={
            "client_id": "pi-test",
            "display_name": "Pi Test",
            "enrollment_status": "pending",
            "auth_token": None,
            "last_error": "CLIENT_PENDING_APPROVAL",
        }
    )

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    app = create_app(
        daemon_get=fake_daemon_get,
        network_snapshot_get=_network_snapshot,
        dependency_snapshot_get=_dependency_snapshot,
    )
    body = app.test_client().get("/").get_data(as_text=True)

    assert "Client enrollment pending approval" in body
    assert "Approve this client from the server UI, then run one daemon tick." in body


def test_overview_surfaces_revoked_client_auth_block_guidance() -> None:
    payloads = _overview_payloads(
        server_auth={
            "client_id": "pi-test",
            "display_name": "Pi Test",
            "enrollment_status": "revoked",
            "auth_token": "token-1",
            "last_error": "CLIENT_REVOKED",
        }
    )

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    app = create_app(
        daemon_get=fake_daemon_get,
        network_snapshot_get=_network_snapshot,
        dependency_snapshot_get=_dependency_snapshot,
    )
    body = app.test_client().get("/").get_data(as_text=True)

    assert "Client access revoked" in body
    assert "Re-approve the client on the server if access should be restored." in body


def test_overview_surfaces_invalid_client_auth_block_guidance() -> None:
    payloads = _overview_payloads(
        server_auth={
            "client_id": "pi-test",
            "display_name": "Pi Test",
            "enrollment_status": "approved",
            "auth_token": "token-1",
            "last_error": "CLIENT_AUTH_INVALID",
        }
    )

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    app = create_app(
        daemon_get=fake_daemon_get,
        network_snapshot_get=_network_snapshot,
        dependency_snapshot_get=_dependency_snapshot,
    )
    body = app.test_client().get("/").get_data(as_text=True)

    assert "Client auth rejected by server" in body
    assert "CLIENT_AUTH_INVALID" in body


def test_jobs_page_renders_filtered_views() -> None:
    payloads = _overview_payloads()

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    app = create_app(
        daemon_get=fake_daemon_get,
        network_snapshot_get=_network_snapshot,
        dependency_snapshot_get=_dependency_snapshot,
    )
    client = app.test_client()

    active = client.get("/jobs?filter=active").get_data(as_text=True)
    waiting = client.get("/jobs?filter=waiting").get_data(as_text=True)
    blocked = client.get("/jobs?filter=blocked").get_data(as_text=True)
    completed = client.get("/jobs?filter=completed").get_data(as_text=True)

    assert "Jobs" in active
    assert "Open job detail" in active
    assert "Transfer progress" in active
    assert "Transferred files" in active
    assert "Pending files" in active
    assert "Job #4" in active
    assert "Job #2" not in active
    assert "Job #2" in waiting
    assert "Retry backoff is active while the daemon waits in WAIT_NETWORK." in waiting
    assert "Local ingest is still in progress before remote completion can finish." in waiting
    assert "4 file(s) are queued for remote upload/verify once connectivity returns." in waiting
    assert "Job #1" not in active
    assert "Job #1" in blocked
    assert "manual retry or isolation" in blocked
    assert "Job #3" in completed


def test_jobs_filter_returns_fragment_for_ajax_requests() -> None:
    payloads = _overview_payloads()

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    app = create_app(
        daemon_get=fake_daemon_get,
        network_snapshot_get=_network_snapshot,
        dependency_snapshot_get=_dependency_snapshot,
    )
    response = app.test_client().get(
        "/jobs?filter=blocked",
        headers={"X-Requested-With": "XMLHttpRequest"},
    )
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert response.headers["X-Client-Location"].endswith("/jobs?filter=blocked")
    assert 'id="jobs-shell"' in body
    assert "<!doctype html>" not in body
    assert "Job #1" in body


def test_derive_job_operator_view_reports_transferred_and_pending_file_counts() -> None:
    operator_view = _derive_job_operator_view(
        {
            "status": "WAIT_NETWORK",
            "status_counts": {
                "READY_TO_UPLOAD": 2,
                "UPLOADED": 1,
                "VERIFIED_REMOTE": 3,
                "DUPLICATE_SHA_GLOBAL": 2,
                "DUPLICATE_SESSION_SHA": 1,
            },
            "files": [],
        }
    )

    assert operator_view["transferred_file_count"] == 3
    assert operator_view["total_file_count"] == 9
    assert operator_view["pending_file_count"] == 3
    assert operator_view["wait_summary"] == (
        "6 file(s) are queued for remote upload/verify once connectivity returns."
    )
    assert operator_view["retry_summary"] == "Retry backoff is active while the daemon waits in WAIT_NETWORK."


def test_job_detail_route_renders_file_level_state() -> None:
    payloads = _overview_payloads()

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    app = create_app(
        daemon_get=fake_daemon_get,
        network_snapshot_get=_network_snapshot,
        dependency_snapshot_get=_dependency_snapshot,
    )
    body = app.test_client().get("/jobs/1").get_data(as_text=True)

    assert "Job #1" in body
    assert "Files" in body
    assert "/var/lib/photovault-clientd/test-media/001.jpg" in body
    assert "DUPLICATE_SESSION_SHA" in body
    assert "already existed remotely" in body
    assert "upload required" in body
    assert "uploaded; waiting for server verify" in body
    assert "Retry upload" in body
    assert "Upload and retry posture" in body
    assert "Local ingest complete" in body
    assert "Retry exhausted files" in body
    assert "Files needing attention" in body
    assert "Local ingest is complete; only remote upload, verify, or cleanup work remains." in body
    assert "Operator action is required before this job can continue remote progression." in body
    assert "1 file(s) need manual retry or isolation after upload/verify failure." in body


def test_events_page_shows_diagnostics_and_events() -> None:
    payloads = _overview_payloads()

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    app = create_app(
        daemon_get=fake_daemon_get,
        network_snapshot_get=_network_snapshot,
        dependency_snapshot_get=_dependency_snapshot,
    )
    body = app.test_client().get("/events").get_data(as_text=True)

    assert "Diagnostics summary" in body
    assert "Recent event digest" in body
    assert "Detailed daemon events" in body
    assert "QUEUE_UPLOAD_PREPARED" in body
    assert "UPLOAD_RETRY" in body


def test_daemon_tick_returns_partial_notice_for_ajax_requests() -> None:
    payloads = _overview_payloads()

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    def fake_daemon_post(
        _: str,
        path: str,
        payload: dict[str, object],
        *,
        timeout_seconds: float = 2.0,
    ) -> object:
        assert path == "/daemon/tick"
        assert payload == {}
        assert timeout_seconds == 2.0
        return {"handled": True, "next_state": "HASHING", "progressed": True}

    app = create_app(
        daemon_get=fake_daemon_get,
        daemon_post=fake_daemon_post,
        network_snapshot_get=_network_snapshot,
        dependency_snapshot_get=_dependency_snapshot,
    )
    client = app.test_client()
    response = client.post(
        "/actions/daemon/tick",
        data={"return_to": "/"},
        headers={"X-Requested-With": "XMLHttpRequest"},
    )
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert response.headers["X-Client-Location"].endswith("/")
    assert "Action complete" in body
    assert "Daemon tick completed in state HASHING." in body


def test_block_devices_page_renders_inventory() -> None:
    payloads = _overview_payloads(daemon_state="IDLE")

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    app = create_app(
        daemon_get=fake_daemon_get,
        network_snapshot_get=_network_snapshot,
        dependency_snapshot_get=_dependency_snapshot,
    )
    response = app.test_client().get("/block-devices")
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Block Devices" in body
    assert "/dev/sda1" in body
    assert "Storage size" in body
    assert "Mounted" in body
    assert "/mnt/sda_1" in body
    assert "Use as ingest source" in body


def test_block_device_mount_action_routes_to_daemon_endpoint() -> None:
    payloads = _overview_payloads(daemon_state="IDLE")
    observed: dict[str, object] = {}

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    def fake_daemon_post(_: str, path: str, payload: dict[str, object]) -> object:
        observed["path"] = path
        observed["payload"] = payload
        if path == "/block-devices/mount":
            return {"device_path": "/dev/sda1", "mount_path": "/mnt/sda_1"}
        raise AssertionError(f"unexpected daemon_post path: {path}")

    app = create_app(
        daemon_get=fake_daemon_get,
        daemon_post=fake_daemon_post,
        network_snapshot_get=_network_snapshot,
        dependency_snapshot_get=_dependency_snapshot,
    )
    response = app.test_client().post(
        "/actions/block-devices/mount",
        data={"device_path": "/dev/sda1"},
    )
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert observed["path"] == "/block-devices/mount"
    assert observed["payload"] == {"device_path": "/dev/sda1"}
    assert "Mounted /dev/sda1 at /mnt/sda_1." in body


def test_block_device_mount_action_returns_fragment_for_ajax_requests() -> None:
    payloads = _overview_payloads(daemon_state="IDLE")

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    def fake_daemon_post(_: str, path: str, payload: dict[str, object]) -> object:
        assert path == "/block-devices/mount"
        assert payload == {"device_path": "/dev/sda1"}
        return {"device_path": "/dev/sda1", "mount_path": "/mnt/sda_1"}

    app = create_app(
        daemon_get=fake_daemon_get,
        daemon_post=fake_daemon_post,
        network_snapshot_get=_network_snapshot,
        dependency_snapshot_get=_dependency_snapshot,
    )
    response = app.test_client().post(
        "/actions/block-devices/mount",
        data={"device_path": "/dev/sda1"},
        headers={"X-Requested-With": "XMLHttpRequest"},
    )
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert response.headers["X-Client-Location"].endswith("/block-devices")
    assert 'id="block-devices-shell"' in body
    assert "<!doctype html>" not in body
    assert "Mounted /dev/sda1 at /mnt/sda_1." in body


def test_use_block_device_as_ingest_source_prefills_overview_form() -> None:
    payloads = _overview_payloads(daemon_state="IDLE")

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    app = create_app(
        daemon_get=fake_daemon_get,
        network_snapshot_get=_network_snapshot,
        dependency_snapshot_get=_dependency_snapshot,
    )
    response = app.test_client().post(
        "/actions/block-devices/use-as-ingest-source",
        data={"mount_path": "/mnt/sda_1", "media_label": "CARD_A"},
        headers={"X-Requested-With": "XMLHttpRequest"},
    )
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Prepared ingest form for mounted source /mnt/sda_1." in body
    assert 'value="CARD_A"' in body
    assert "/mnt/sda_1" in body


def test_daemon_tick_busy_response_is_rendered_as_wait_notice() -> None:
    payloads = _overview_payloads()

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    def fake_daemon_post(
        _: str,
        path: str,
        payload: dict[str, object],
        *,
        timeout_seconds: float = 2.0,
    ) -> object:
        assert path == "/daemon/tick"
        assert payload == {}
        assert timeout_seconds == 2.0
        return {
            "handled": True,
            "progressed": False,
            "already_progressing": True,
            "next_state": "WAIT_NETWORK",
            "state": "WAIT_NETWORK",
        }

    app = create_app(
        daemon_get=fake_daemon_get,
        daemon_post=fake_daemon_post,
        network_snapshot_get=_network_snapshot,
        dependency_snapshot_get=_dependency_snapshot,
    )
    response = app.test_client().post("/actions/daemon/tick", data={"return_to": "/"})
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Action in progress" in body
    assert "Daemon is already progressing in state WAIT_NETWORK; wait and refresh instead" in body


def test_job_detail_disables_manual_tick_while_auto_progress_active() -> None:
    payloads = _overview_payloads()

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    app = create_app(
        daemon_get=fake_daemon_get,
        network_snapshot_get=_network_snapshot,
        dependency_snapshot_get=_dependency_snapshot,
    )
    body = app.test_client().get("/jobs/1").get_data(as_text=True)

    assert "Run daemon tick (auto progression active)" in body
    assert "Upload/completion progression runs automatically." in body


def test_retry_upload_action_renders_notice_on_job_detail() -> None:
    payloads = _overview_payloads()
    observed: dict[str, object] = {}

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    def fake_daemon_post(_: str, path: str, payload: dict[str, object]) -> object:
        observed["path"] = path
        observed["payload"] = payload
        return {"handled": True, "next_state": "UPLOAD_PREPARE"}

    app = create_app(
        daemon_get=fake_daemon_get,
        daemon_post=fake_daemon_post,
        network_snapshot_get=_network_snapshot,
        dependency_snapshot_get=_dependency_snapshot,
    )
    response = app.test_client().post("/actions/retry-upload", data={"job_id": "1", "file_id": "6"})
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert observed["path"] == "/ingest/files/6/retry-upload"
    assert observed["payload"] == {}
    assert "Action complete" in body
    assert "File #6 requeued for upload; daemon moved to UPLOAD_PREPARE." in body


def test_create_ingest_job_shows_friendly_source_path_validation_error() -> None:
    payloads = _overview_payloads(daemon_state="IDLE")

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    def failing_daemon_post(_: str, path: str, payload: dict[str, object]) -> object:
        assert path == "/ingest/jobs"
        assert payload["media_label"] == "usb-root"
        req = httpx.Request("POST", "http://127.0.0.1:9101/ingest/jobs")
        resp = httpx.Response(
            422,
            request=req,
            json={
                "detail": {
                    "code": "INGEST_SOURCE_PATH_INVALID",
                    "message": "One or more source paths could not be used for ingest discovery.",
                    "invalid_sources": [
                        {
                            "source_path": "/mnt/usb/missing.jpg",
                            "reason": "Path does not exist.",
                        }
                    ],
                    "suggestion": "Fix the listed paths, then retry ingest creation.",
                }
            },
        )
        raise httpx.HTTPStatusError("unprocessable", request=req, response=resp)

    app = create_app(
        daemon_get=fake_daemon_get,
        daemon_post=failing_daemon_post,
        network_snapshot_get=_network_snapshot,
        dependency_snapshot_get=_dependency_snapshot,
    )
    response = app.test_client().post(
        "/ingest/jobs",
        data={"media_label": "usb-root", "source_paths": "/mnt/usb/missing.jpg"},
    )
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Ingest request failed" in body
    assert "One or more source paths could not be used for ingest discovery." in body
    assert "Fix the listed paths, then retry ingest creation." in body
    assert "/mnt/usb/missing.jpg: Path does not exist." in body


def test_create_ingest_job_notice_reports_filtered_files() -> None:
    payloads = _overview_payloads(daemon_state="IDLE")

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    def success_daemon_post(_: str, path: str, payload: dict[str, object]) -> object:
        assert path == "/ingest/jobs"
        assert payload["media_label"] == "usb-root"
        return {
            "job_id": 11,
            "discovered_count": 3,
            "filtered_count": 4,
            "filtered_sources": [
                {
                    "source_path": "/mnt/usb/.DS_Store",
                    "reason": "Excluded by ingest policy: file name .DS_Store",
                },
                {
                    "source_path": "/mnt/usb/readme.txt",
                    "reason": "Skipped by ingest policy: unsupported file extension .txt",
                },
            ],
            "state": "STAGING_COPY",
        }

    app = create_app(
        daemon_get=fake_daemon_get,
        daemon_post=success_daemon_post,
        network_snapshot_get=_network_snapshot,
        dependency_snapshot_get=_dependency_snapshot,
    )
    response = app.test_client().post(
        "/ingest/jobs",
        data={"media_label": "usb-root", "source_paths": "/mnt/usb"},
        headers={"X-Requested-With": "XMLHttpRequest"},
    )
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert (
        "Created ingest job #11 with 3 discovered file(s). "
        "Skipped 4 file(s) by the v1 ingest policy." in body
    )
    assert "Filtered files" in body
    assert "/mnt/usb/.DS_Store: Excluded by ingest policy: file name .DS_Store" in body
    assert "/mnt/usb/readme.txt: Skipped by ingest policy: unsupported file extension .txt" in body


def test_create_ingest_job_rejects_multiple_paths_in_ui() -> None:
    payloads = _overview_payloads(daemon_state="IDLE")

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    app = create_app(
        daemon_get=fake_daemon_get,
        network_snapshot_get=_network_snapshot,
        dependency_snapshot_get=_dependency_snapshot,
    )
    response = app.test_client().post(
        "/ingest/jobs",
        data={"media_label": "usb-root", "source_paths": "/mnt/usb\n/mnt/usb-2"},
    )
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Use one absolute source path per ingest job." in body


def test_network_page_and_errors_render() -> None:
    payloads = _overview_payloads()

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    app = create_app(
        daemon_get=fake_daemon_get,
        network_snapshot_get=_network_snapshot,
        dependency_snapshot_get=_dependency_snapshot,
    )
    client = app.test_client()
    page = client.get("/network").get_data(as_text=True)
    assert "nav-link active" in page
    assert ">Network<" in page
    assert "Update AP config" in page
    assert "Join upstream Wi-Fi" in page
    assert "Recheck upstream status" in page
    assert "Visible Wi-Fi Networks" in page
    assert "Upstream STA connectivity: full" in page
    assert "Upstream status: internet_reachable" in page
    assert "Local AP ready: yes" in page
    assert "Upstream Internet reachable: yes" in page
    assert "Next action:" in page

    def failing_daemon_post(_: str, path: str, payload: dict[str, object]) -> object:
        assert path == "/network/wifi-scan"
        request = httpx.Request("POST", "http://127.0.0.1:9101/network/wifi-scan")
        response = httpx.Response(
            status_code=503,
            json={
                "detail": {
                    "code": "NM_PERMISSION_DENIED",
                    "message": (
                        "Failed to trigger Wi-Fi scan: NetworkManager denied the photovault "
                        "service user."
                    ),
                    "suggestion": "configure polkit",
                }
            },
            request=request,
        )
        raise httpx.HTTPStatusError("scan failed", request=request, response=response)

    app_with_scan_error = create_app(
        daemon_get=fake_daemon_get,
        daemon_post=failing_daemon_post,
        dependency_snapshot_get=_dependency_snapshot,
    )
    scan_error_body = app_with_scan_error.test_client().post("/network/scan").get_data(as_text=True)
    assert "Failed to scan Wi-Fi: daemon API returned HTTP 503" in scan_error_body


def test_network_scan_returns_fragment_for_ajax_requests() -> None:
    payloads = _overview_payloads()

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    def fake_daemon_post(_: str, path: str, payload: dict[str, object]) -> object:
        assert path == "/network/wifi-scan"
        assert payload == {}
        return {"ok": True}

    app = create_app(
        daemon_get=fake_daemon_get,
        daemon_post=fake_daemon_post,
        dependency_snapshot_get=_dependency_snapshot,
    )
    response = app.test_client().post(
        "/network/scan",
        headers={"X-Requested-With": "XMLHttpRequest"},
    )
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert response.headers["X-Client-Location"].endswith("/network")
    assert 'id="network-shell"' in body
    assert "Triggered Wi-Fi scan and refreshed network status." in body


def test_network_page_renders_captive_portal_guidance() -> None:
    payloads = _overview_payloads()
    network_status = dict(payloads["/network/status"])
    snapshot = dict(network_status["snapshot"])
    snapshot.update(
        {
            "general": {
                "state": "connected",
                "connectivity": "portal",
                "wifi": "enabled",
            },
            "sta_connection_names": ["cpa-test"],
            "upstream_connectivity": "portal",
            "upstream_status": "captive_portal_likely",
            "upstream_no_usable_internet": True,
            "upstream_internet_reachable": False,
            "captive_portal_detected": True,
            "next_operator_action": (
                "Local AP remains available. Upstream Wi-Fi likely requires captive-portal login."
            ),
        }
    )
    network_status["snapshot"] = snapshot
    payloads["/network/status"] = network_status

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    app = create_app(
        daemon_get=fake_daemon_get,
        dependency_snapshot_get=_dependency_snapshot,
    )
    page = app.test_client().get("/network").get_data(as_text=True)
    assert "Captive portal login likely required." in page
    assert "http://neverssl.com" in page
    assert "cpa-test" in page
    assert "Upstream status: captive_portal_likely" in page
    assert "Start portal handoff" in page


def test_network_page_renders_active_portal_handoff_guidance() -> None:
    payloads = _overview_payloads()
    network_status = dict(payloads["/network/status"])
    snapshot = dict(network_status["snapshot"])
    snapshot.update(
        {
            "general": {
                "state": "connected",
                "connectivity": "portal",
                "wifi": "enabled",
            },
            "upstream_connectivity": "portal",
            "upstream_status": "captive_portal_likely",
            "upstream_no_usable_internet": True,
            "upstream_internet_reachable": False,
            "captive_portal_detected": True,
            "portal_handoff_active": True,
            "portal_handoff_started_at_utc": "2026-04-22T21:00:00+00:00",
        }
    )
    network_status["snapshot"] = snapshot
    payloads["/network/status"] = network_status

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    app = create_app(
        daemon_get=fake_daemon_get,
        dependency_snapshot_get=_dependency_snapshot,
    )
    page = app.test_client().get("/network").get_data(as_text=True)
    assert "Portal handoff active." in page
    assert "Stop portal handoff" in page
    assert "wired remote admin access may be temporarily affected" in page


def test_network_ap_update_success_and_error_render() -> None:
    payloads = _overview_payloads()

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    def success_daemon_put(_: str, path: str, payload: dict[str, object]) -> object:
        assert path == "/network/ap-config"
        assert payload["ssid"] == "field-ap"
        return {
            "ap_config": {
                "profile_name": "photovault-ap",
                "ssid": "field-ap",
                "password_set": True,
                "updated_at_utc": "2026-04-20T11:12:00+00:00",
                "last_applied_at_utc": "2026-04-20T11:12:00+00:00",
                "last_apply_error": None,
            }
        }

    app = create_app(
        daemon_get=fake_daemon_get,
        daemon_put=success_daemon_put,
        dependency_snapshot_get=_dependency_snapshot,
    )
    success_body = app.test_client().post(
        "/network/ap-config",
        data={"ssid": "field-ap", "password": "validpass11"},
    ).get_data(as_text=True)
    assert "AP configuration updated and applied via NetworkManager." in success_body

    def failing_daemon_put(_: str, path: str, payload: dict[str, object]) -> object:
        assert path == "/network/ap-config"
        request = httpx.Request("PUT", "http://127.0.0.1:9101/network/ap-config")
        response = httpx.Response(
            status_code=422,
            json={
                "detail": {
                    "code": "AP_CONFIG_INVALID",
                    "message": "AP password must be between 8 and 63 characters.",
                }
            },
            request=request,
        )
        raise httpx.HTTPStatusError("bad request", request=request, response=response)

    app_with_error = create_app(
        daemon_get=fake_daemon_get,
        daemon_put=failing_daemon_put,
        dependency_snapshot_get=_dependency_snapshot,
    )
    error_body = app_with_error.test_client().post(
        "/network/ap-config",
        data={"ssid": "field-ap", "password": "short"},
    ).get_data(as_text=True)
    assert "Failed to update AP config: daemon API returned HTTP 422" in error_body


def test_network_sta_connect_success_and_error_render() -> None:
    payloads = _overview_payloads()

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    def success_daemon_post(_: str, path: str, payload: dict[str, object]) -> object:
        assert path == "/network/sta-connect"
        assert payload["ssid"] == "studio-wifi"
        assert payload["password"] == "validpass11"
        return {"ssid": "studio-wifi", "target_device": "wlan0"}

    app = create_app(
        daemon_get=fake_daemon_get,
        daemon_post=success_daemon_post,
        dependency_snapshot_get=_dependency_snapshot,
    )
    success_body = app.test_client().post(
        "/network/sta-connect",
        data={"sta_ssid": "studio-wifi", "sta_password": "validpass11"},
    ).get_data(as_text=True)
    assert "Upstream Wi-Fi connect requested for SSID" in success_body
    assert "studio-wifi" in success_body

    def failing_daemon_post(_: str, path: str, payload: dict[str, object]) -> object:
        assert path == "/network/sta-connect"
        request = httpx.Request("POST", "http://127.0.0.1:9101/network/sta-connect")
        response = httpx.Response(
            status_code=503,
            json={
                "detail": {
                    "code": "NM_WIFI_AUTH_FAILED",
                    "message": "Wi-Fi authentication failed.",
                }
            },
            request=request,
        )
        raise httpx.HTTPStatusError("bad gateway", request=request, response=response)

    app_with_error = create_app(
        daemon_get=fake_daemon_get,
        daemon_post=failing_daemon_post,
        dependency_snapshot_get=_dependency_snapshot,
    )
    error_body = app_with_error.test_client().post(
        "/network/sta-connect",
        data={"sta_ssid": "studio-wifi", "sta_password": "badpass"},
    ).get_data(as_text=True)
    assert "Failed to connect upstream Wi-Fi: daemon API returned HTTP 503" in error_body


def test_network_upstream_recheck_success_and_error_render() -> None:
    payloads = _overview_payloads()

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    def success_daemon_post(_: str, path: str, payload: dict[str, object]) -> object:
        assert path == "/network/upstream-recheck"
        return {
            "connectivity_check": "full",
            "snapshot": {
                "upstream_status": "internet_reachable",
            },
        }

    app = create_app(
        daemon_get=fake_daemon_get,
        daemon_post=success_daemon_post,
        dependency_snapshot_get=_dependency_snapshot,
    )
    success_body = app.test_client().post("/network/upstream-recheck").get_data(as_text=True)
    assert "Rechecked upstream connectivity: Internet is reachable now." in success_body
    assert "NetworkManager check=full." in success_body

    def failing_daemon_post(_: str, path: str, payload: dict[str, object]) -> object:
        assert path == "/network/upstream-recheck"
        request = httpx.Request("POST", "http://127.0.0.1:9101/network/upstream-recheck")
        response = httpx.Response(
            status_code=503,
            json={
                "detail": {
                    "code": "NM_COMMAND_FAILED",
                    "message": "Failed to recheck upstream internet connectivity: nmcli command failed.",
                }
            },
            request=request,
        )
        raise httpx.HTTPStatusError("bad gateway", request=request, response=response)

    app_with_error = create_app(
        daemon_get=fake_daemon_get,
        daemon_post=failing_daemon_post,
        dependency_snapshot_get=_dependency_snapshot,
    )
    error_body = app_with_error.test_client().post("/network/upstream-recheck").get_data(as_text=True)
    assert "Failed to recheck upstream connectivity: daemon API returned HTTP 503" in error_body


def test_network_portal_handoff_start_and_stop_render() -> None:
    payloads = _overview_payloads()

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    def success_daemon_post(_: str, path: str, payload: dict[str, object]) -> object:
        if path == "/network/portal-handoff/start":
            return {"started": True}
        if path == "/network/portal-handoff/stop":
            return {"stopped": True}
        raise AssertionError(f"unexpected path: {path}")

    app = create_app(
        daemon_get=fake_daemon_get,
        daemon_post=success_daemon_post,
        dependency_snapshot_get=_dependency_snapshot,
    )
    start_body = app.test_client().post("/network/portal-handoff/start").get_data(as_text=True)
    assert "Portal handoff started." in start_body
    stop_body = app.test_client().post("/network/portal-handoff/stop").get_data(as_text=True)
    assert "Portal handoff stopped and Ethernet route preferences were restored." in stop_body


def test_network_portal_handoff_start_and_stop_error_render() -> None:
    payloads = _overview_payloads()

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    def failing_daemon_post(_: str, path: str, payload: dict[str, object]) -> object:
        request = httpx.Request("POST", f"http://127.0.0.1:9101{path}")
        response = httpx.Response(
            status_code=503,
            json={
                "detail": {
                    "code": "NM_COMMAND_FAILED",
                    "message": "forced failure",
                }
            },
            request=request,
        )
        raise httpx.HTTPStatusError("failure", request=request, response=response)

    app = create_app(
        daemon_get=fake_daemon_get,
        daemon_post=failing_daemon_post,
        dependency_snapshot_get=_dependency_snapshot,
    )
    start_error = app.test_client().post("/network/portal-handoff/start").get_data(as_text=True)
    assert "Failed to start portal handoff: daemon API returned HTTP 503" in start_error
    stop_error = app.test_client().post("/network/portal-handoff/stop").get_data(as_text=True)
    assert "Failed to stop portal handoff: daemon API returned HTTP 503" in stop_error
