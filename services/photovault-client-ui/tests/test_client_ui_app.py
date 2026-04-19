import subprocess

import httpx
from photovault_client_ui.app import _parse_nmcli_multiline, create_app


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


def test_parse_nmcli_multiline_splits_records_without_blank_lines() -> None:
    output = """IN-USE:                                  
SSID:                                   :)
SIGNAL:                                 89
SECURITY:                               WPA2
CHAN:                                   6
RATE:                                   260 Mbit/s
IN-USE:                                  
SSID:                                   :))
SIGNAL:                                 89
SECURITY:                               WPA2
CHAN:                                   40
RATE:                                   540 Mbit/s
IN-USE:                                  
SSID:                                   :(
SIGNAL:                                 54
SECURITY:                               WPA2
CHAN:                                   11
RATE:                                   195 Mbit/s
"""

    records = _parse_nmcli_multiline(output)

    assert len(records) == 3
    assert records[0]["SSID"] == ":)"
    assert records[1]["SSID"] == ":))"
    assert records[2]["SSID"] == ":("


def _overview_payloads() -> dict[str, object]:
    return {
        "/state": {
            "current_state": "WAIT_NETWORK",
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
                    "status": "WAIT_NETWORK",
                    "local_ingest_complete": True,
                    "upload_pending": True,
                    "status_counts": {
                        "READY_TO_UPLOAD": 2,
                        "DUPLICATE_SESSION_SHA": 1,
                    },
                }
            ]
        },
        "/events?limit=10": {
            "events": [
                {
                    "category": "QUEUE_UPLOAD_PREPARED",
                    "created_at_utc": "2026-04-19T16:51:44.120670+00:00",
                    "message": "job_id=1, ready_to_upload=2",
                }
            ]
        },
        "/ingest/jobs/1": {
            "job_id": 1,
            "media_label": "pi-test-sd",
            "status": "WAIT_NETWORK",
            "updated_at_utc": "2026-04-19T16:51:44.120670+00:00",
            "local_ingest_complete": True,
            "upload_pending": True,
            "status_counts": {
                "READY_TO_UPLOAD": 2,
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
                    "status": "DUPLICATE_SESSION_SHA",
                    "sha256_hex": "abc123",
                    "retry_count": 0,
                    "last_error": None,
                },
            ],
        },
    }


def test_index_route_renders_overview_sections() -> None:
    payloads = _overview_payloads()

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    app = create_app(
        daemon_get=fake_daemon_get,
        network_snapshot_get=_network_snapshot,
    )
    client = app.test_client()
    response = client.get("/")
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert 'class="active">Overview<' in body
    assert 'href="/network"' in body
    assert "Create ingest job" in body
    assert "WAIT_NETWORK" in body
    assert "PASS" in body
    assert "QUEUE_UPLOAD_PREPARED" in body
    assert 'href="/jobs/1"' in body
    assert 'data-ajax-target="#overview-shell"' in body
    assert "Connect Wi-Fi" not in body
    assert "Visible Wi-Fi Networks" not in body


def test_network_page_renders_network_sections() -> None:
    payloads = _overview_payloads()

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    app = create_app(
        daemon_get=fake_daemon_get,
        network_snapshot_get=_network_snapshot,
    )
    client = app.test_client()
    response = client.get("/network")
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert 'class="active">Network<' in body
    assert "Connect Wi-Fi" in body
    assert "Scan networks" in body
    assert "Network Devices" in body
    assert "Visible Wi-Fi Networks" in body
    assert "studio-wifi" in body
    assert "Create ingest job" not in body


def test_create_ingest_job_redirects_to_detail_page() -> None:
    payloads = _overview_payloads()
    observed: dict[str, object] = {}

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    def fake_daemon_post(_: str, path: str, payload: dict[str, object]) -> object:
        observed["path"] = path
        observed["payload"] = payload
        return {"job_id": 7}

    app = create_app(
        daemon_get=fake_daemon_get,
        daemon_post=fake_daemon_post,
        network_snapshot_get=_network_snapshot,
    )
    client = app.test_client()
    response = client.post(
        "/ingest/jobs",
        data={
            "media_label": "sd-new",
            "source_paths": "/media/sd/001.jpg\n/media/sd/002.jpg\n",
        },
        follow_redirects=False,
    )

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/jobs/7")
    assert observed["path"] == "/ingest/jobs"
    assert observed["payload"] == {
        "media_label": "sd-new",
        "source_paths": ["/media/sd/001.jpg", "/media/sd/002.jpg"],
    }


def test_create_ingest_job_returns_partial_for_ajax_requests() -> None:
    payloads = _overview_payloads()
    payloads["/ingest/jobs/7"] = {
        "job_id": 7,
        "media_label": "sd-new",
        "status": "STAGING_COPY",
        "updated_at_utc": "2026-04-19T17:10:00+00:00",
        "local_ingest_complete": False,
        "upload_pending": False,
        "status_counts": {"PENDING_COPY": 2},
        "files": [],
    }

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    def fake_daemon_post(_: str, path: str, payload: dict[str, object]) -> object:
        assert path == "/ingest/jobs"
        return {"job_id": 7, "discovered_count": 2}

    app = create_app(
        daemon_get=fake_daemon_get,
        daemon_post=fake_daemon_post,
        network_snapshot_get=_network_snapshot,
    )
    client = app.test_client()
    response = client.post(
        "/ingest/jobs",
        data={"media_label": "sd-new", "source_paths": "/media/sd/001.jpg\n/media/sd/002.jpg"},
        headers={"X-Requested-With": "XMLHttpRequest"},
    )
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert response.headers["X-Client-Location"].endswith("/jobs/7")
    assert "<!doctype html>" not in body.lower()
    assert "Created ingest job #7 with 2 discovered file(s)." in body
    assert "Job #7 Detail" in body


def test_create_ingest_job_shows_validation_error() -> None:
    payloads = _overview_payloads()

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    app = create_app(daemon_get=fake_daemon_get, network_snapshot_get=_network_snapshot)
    client = app.test_client()
    response = client.post("/ingest/jobs", data={"media_label": "", "source_paths": ""})
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Ingest request failed." in body
    assert "Media label is required." in body


def test_network_scan_redirects_to_network_page() -> None:
    payloads = _overview_payloads()
    observed: dict[str, object] = {"scanned": False}

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    def fake_network_scan() -> None:
        observed["scanned"] = True

    app = create_app(
        daemon_get=fake_daemon_get,
        network_snapshot_get=_network_snapshot,
        network_scan=fake_network_scan,
    )
    client = app.test_client()
    response = client.post("/network/scan", follow_redirects=False)

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/network")
    assert observed["scanned"] is True


def test_network_scan_shows_error_when_nmcli_fails() -> None:
    payloads = _overview_payloads()

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    def failing_network_scan() -> None:
        raise subprocess.CalledProcessError(
            10,
            ["nmcli", "device", "wifi", "rescan"],
            stderr="Error: org.freedesktop.NetworkManager.wifi.scan request failed: not authorized.",
        )

    app = create_app(
        daemon_get=fake_daemon_get,
        network_snapshot_get=_network_snapshot,
        network_scan=failing_network_scan,
    )
    client = app.test_client()
    response = client.post("/network/scan")
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Failed to scan Wi-Fi: NetworkManager denied the photovault service user." in body
    assert "polkit rule" in body


def test_network_connect_redirects_to_network_page() -> None:
    payloads = _overview_payloads()
    observed: dict[str, object] = {}

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    def fake_network_connect(ssid: str, password: str | None) -> None:
        observed["ssid"] = ssid
        observed["password"] = password

    app = create_app(
        daemon_get=fake_daemon_get,
        network_snapshot_get=_network_snapshot,
        network_connect=fake_network_connect,
    )
    client = app.test_client()
    response = client.post(
        "/network/connect",
        data={"ssid": "studio-wifi", "password": "secretpass"},
        follow_redirects=False,
    )

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/network")
    assert observed == {"ssid": "studio-wifi", "password": "secretpass"}


def test_network_connect_shows_error_when_nmcli_fails() -> None:
    payloads = _overview_payloads()

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    def failing_network_connect(_: str, __: str | None) -> None:
        raise subprocess.CalledProcessError(
            10,
            ["nmcli", "device", "wifi", "connect"],
            stderr="Error: Connection activation failed: not authorized",
        )

    app = create_app(
        daemon_get=fake_daemon_get,
        network_snapshot_get=_network_snapshot,
        network_connect=failing_network_connect,
    )
    client = app.test_client()
    response = client.post(
        "/network/connect",
        data={"ssid": "studio-wifi", "password": "secretpass"},
    )
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Network action failed." in body
    assert "Failed to connect Wi-Fi: NetworkManager denied the photovault service user." in body


def test_network_snapshot_shows_friendly_error_when_nmcli_is_missing() -> None:
    payloads = _overview_payloads()

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    def missing_nmcli() -> dict[str, object]:
        raise FileNotFoundError("nmcli")

    app = create_app(
        daemon_get=fake_daemon_get,
        network_snapshot_get=missing_nmcli,
    )
    client = app.test_client()
    response = client.get("/network")
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Failed to load NetworkManager status: nmcli is not installed on this device." in body


def test_job_detail_route_renders_file_level_status() -> None:
    payloads = _overview_payloads()

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    app = create_app(
        daemon_get=fake_daemon_get,
        network_snapshot_get=_network_snapshot,
    )
    client = app.test_client()
    response = client.get("/jobs/1")
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Job #1 Detail" in body
    assert "Files" in body
    assert "/var/lib/photovault-clientd/test-media/001.jpg" in body
    assert "DUPLICATE_SESSION_SHA" in body
    assert "Back to overview" in body


def test_job_detail_route_returns_partial_for_ajax_requests() -> None:
    payloads = _overview_payloads()

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    app = create_app(
        daemon_get=fake_daemon_get,
        network_snapshot_get=_network_snapshot,
    )
    client = app.test_client()
    response = client.get("/jobs/1", headers={"X-Requested-With": "XMLHttpRequest"})
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert response.headers["X-Client-Location"].endswith("/jobs/1")
    assert "<!doctype html>" not in body.lower()
    assert "Job #1 Detail" in body


def test_daemon_tick_returns_partial_notice_for_ajax_requests() -> None:
    payloads = _overview_payloads()

    def fake_daemon_get(_: str, path: str) -> object:
        return payloads[path]

    def fake_daemon_post(_: str, path: str, payload: dict[str, object]) -> object:
        assert path == "/daemon/tick"
        assert payload == {}
        return {"handled": True, "next_state": "HASHING", "progressed": True}

    app = create_app(
        daemon_get=fake_daemon_get,
        daemon_post=fake_daemon_post,
        network_snapshot_get=_network_snapshot,
    )
    client = app.test_client()
    response = client.post(
        "/actions/daemon/tick",
        data={"selected_job_id": "1"},
        headers={"X-Requested-With": "XMLHttpRequest"},
    )
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert response.headers["X-Client-Location"].endswith("/jobs/1")
    assert "Action complete." in body
    assert "Daemon tick completed in state HASHING." in body


def test_job_detail_route_returns_404_for_unknown_job() -> None:
    payloads = _overview_payloads()

    def fake_daemon_get(_: str, path: str) -> object:
        if path == "/ingest/jobs/9":
            request = httpx.Request("GET", "http://127.0.0.1:9101/ingest/jobs/9")
            response = httpx.Response(404, request=request)
            raise httpx.HTTPStatusError("not found", request=request, response=response)
        return payloads[path]

    app = create_app(daemon_get=fake_daemon_get, network_snapshot_get=_network_snapshot)
    client = app.test_client()
    response = client.get("/jobs/9")

    assert response.status_code == 404


def test_index_route_surfaces_daemon_unreachable_error() -> None:
    def failing_daemon_get(_: str, __: str) -> object:
        raise httpx.ConnectError("connection refused")

    app = create_app(daemon_get=failing_daemon_get, network_snapshot_get=_network_snapshot)
    client = app.test_client()
    response = client.get("/")
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Daemon unreachable." in body
    assert "connection refused" in body
    assert "No ingest jobs are currently tracked." in body
