import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient
from photovault_clientd.app import create_app
from photovault_clientd.networking import (
    AccessPointProfile,
    NetworkDevice,
    NetworkGeneral,
    NetworkManagerError,
    NetworkStatusSnapshot,
    OperatorError,
    WifiNetwork,
)


def _snapshot(profile_name: str = "photovault-ap") -> NetworkStatusSnapshot:
    return NetworkStatusSnapshot(
        general=NetworkGeneral(state="connected", connectivity="full", wifi="enabled"),
        devices=[NetworkDevice(device="wlan0", type="wifi", state="connected", connection="studio-wifi")],
        wifi_networks=[
            WifiNetwork(
                in_use="*",
                ssid="studio-wifi",
                signal="72",
                security="WPA2",
                channel="40",
                rate="540 Mbit/s",
            )
        ],
        ap_profile=AccessPointProfile(
            profile_name=profile_name,
            exists=True,
            active=True,
            ssid="photovault-ap",
            autoconnect="yes",
            mode="ap",
            key_mgmt="wpa-psk",
        ),
        sta_connected=True,
        next_operator_action="Upstream network is connected. Verify AP settings and continue operations.",
    )


class _FakeNetworkManager:
    def __init__(self) -> None:
        self.ensure_calls: list[dict[str, str]] = []
        self.scan_called = False
        self.fail_scan = False

    def ensure_ap_profile(self, *, profile_name: str, ssid: str, password: str) -> dict[str, object]:
        self.ensure_calls.append(
            {
                "profile_name": profile_name,
                "ssid": ssid,
                "password": password,
            }
        )
        return {
            "created": False,
            "profile_name": profile_name,
            "ap_profile": _snapshot(profile_name).ap_profile.__dict__,
        }

    def status_snapshot(self, profile_name: str = "photovault-ap") -> NetworkStatusSnapshot:
        return _snapshot(profile_name)

    def trigger_wifi_scan(self) -> None:
        if self.fail_scan:
            raise NetworkManagerError(
                OperatorError(
                    code="NM_COMMAND_FAILED",
                    message="Failed to trigger Wi-Fi scan: nmcli command failed.",
                    detail="forced failure",
                    suggestion="retry",
                )
            )
        self.scan_called = True


def test_startup_ensures_ap_profile_idempotently(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    manager = _FakeNetworkManager()

    app = create_app(db_path=db_path, network_manager=manager)
    with TestClient(app):
        pass

    app_second = create_app(db_path=db_path, network_manager=manager)
    with TestClient(app_second):
        pass

    assert len(manager.ensure_calls) == 2
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT COUNT(1) FROM network_ap_config;").fetchone()
        assert row is not None
        assert int(row[0]) == 1


def test_network_status_and_ap_config_endpoints_return_normalized_payload(tmp_path: Path) -> None:
    app = create_app(db_path=tmp_path / "state.sqlite3", network_manager=_FakeNetworkManager())
    with TestClient(app) as client:
        status_response = client.get("/network/status")
        assert status_response.status_code == 200
        status_payload = status_response.json()
        assert status_payload["snapshot"]["general"]["connectivity"] == "full"
        assert status_payload["snapshot"]["ap_profile"]["profile_name"] == "photovault-ap"
        assert "next_operator_action" in status_payload["snapshot"]

        config_response = client.get("/network/ap-config")
        assert config_response.status_code == 200
        config_payload = config_response.json()
        assert config_payload["profile_name"] == "photovault-ap"
        assert config_payload["password_set"] is True


def test_network_ap_config_update_validation_returns_422(tmp_path: Path) -> None:
    app = create_app(db_path=tmp_path / "state.sqlite3", network_manager=_FakeNetworkManager())
    with TestClient(app) as client:
        response = client.put("/network/ap-config", json={"ssid": "  ", "password": "short"})
        assert response.status_code == 422
        payload = response.json()
        assert payload["detail"]["code"] == "AP_CONFIG_INVALID"


def test_network_ap_config_update_applies_and_persists(tmp_path: Path) -> None:
    manager = _FakeNetworkManager()
    app = create_app(db_path=tmp_path / "state.sqlite3", network_manager=manager)
    with TestClient(app) as client:
        response = client.put(
            "/network/ap-config",
            json={"ssid": "field-unit-ap", "password": "newpassword42"},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["ap_config"]["ssid"] == "field-unit-ap"
        assert payload["ap_config"]["password_set"] is True
        assert payload["apply_result"]["profile_name"] == "photovault-ap"

    assert manager.ensure_calls[-1]["ssid"] == "field-unit-ap"
    with sqlite3.connect(tmp_path / "state.sqlite3") as conn:
        row = conn.execute("SELECT ssid, password_plaintext FROM network_ap_config WHERE id = 1;").fetchone()
    assert row is not None
    assert row[0] == "field-unit-ap"
    assert row[1] == "newpassword42"


def test_network_wifi_scan_failure_surfaces_operator_error(tmp_path: Path) -> None:
    manager = _FakeNetworkManager()
    manager.fail_scan = True
    app = create_app(db_path=tmp_path / "state.sqlite3", network_manager=manager)
    with TestClient(app) as client:
        response = client.post("/network/wifi-scan")
        assert response.status_code == 503
        payload = response.json()
        assert payload["detail"]["code"] == "NM_COMMAND_FAILED"
