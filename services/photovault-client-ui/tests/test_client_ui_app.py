import subprocess

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


def _overview_payloads(daemon_state: str = "WAIT_NETWORK") -> dict[str, object]:
    return {
        "/state": {
            "current_state": daemon_state,
            "updated_at_utc": "2026-04-19T16:51:44.120670+00:00",
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
    assert 'class="active">Overview<' in body
    assert "Operator dashboard" in body
    assert "Top actions" in body
    assert "Attention and blockers" in body
    assert "Recent daemon activity" in body
    assert "Create ingest job" in body
    assert "Active job summary" in body
    assert "Waiting for network connectivity" in body
    assert "Current state: <code>WAIT_NETWORK</code>" in body
    assert "Auto progression active" in body
    assert "Waiting jobs" in body
    assert "Run daemon tick (auto progression active)" in body
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
    assert "Retry backoff is active while the daemon remains in WAIT_NETWORK." in waiting
    assert "Job #1" not in active
    assert "Job #1" in blocked
    assert "Job #3" in completed


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
    assert "Files needing attention" in body


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
        data={"media_label": "usb-root", "source_paths": "/mnt/usb\n/mnt/usb/missing.jpg"},
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
    assert 'class="active">Network<' in page
    assert "Connect Wi-Fi" in page
    assert "Visible Wi-Fi Networks" in page

    def failing_network_scan() -> None:
        raise subprocess.CalledProcessError(
            10,
            ["nmcli", "device", "wifi", "rescan"],
            stderr="Error: org.freedesktop.NetworkManager.wifi.scan request failed: not authorized.",
        )

    app_with_scan_error = create_app(
        daemon_get=fake_daemon_get,
        network_snapshot_get=_network_snapshot,
        network_scan=failing_network_scan,
        dependency_snapshot_get=_dependency_snapshot,
    )
    scan_error_body = app_with_scan_error.test_client().post("/network/scan").get_data(as_text=True)
    assert "Failed to scan Wi-Fi: NetworkManager denied the photovault service user." in scan_error_body
