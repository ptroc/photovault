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
        devices=[
            NetworkDevice(device="wlan1", type="wifi", state="connected", connection=profile_name),
            NetworkDevice(device="wlan0", type="wifi", state="connected", connection="studio-wifi"),
        ],
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
        ap_device_names=["wlan1"],
        sta_device_names=["wlan0"],
        sta_connection_names=["studio-wifi"],
        local_ap_ready=True,
        upstream_connectivity="full",
        upstream_status="internet_reachable",
        upstream_no_usable_internet=False,
        upstream_internet_reachable=True,
        captive_portal_detected=False,
        next_operator_action="Upstream network is connected. Verify AP settings and continue operations.",
    )


class _FakeNetworkManager:
    def __init__(self) -> None:
        self.ensure_calls: list[dict[str, str]] = []
        self.scan_called = False
        self.fail_scan = False
        self.connect_calls: list[dict[str, str | None]] = []
        self.fail_connect = False
        self.recheck_called = False
        self.fail_recheck = False

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

    def connect_sta_network(
        self,
        *,
        ssid: str,
        password: str | None,
        ap_profile_name: str,
    ) -> dict[str, object]:
        self.connect_calls.append(
            {"ssid": ssid, "password": password, "ap_profile_name": ap_profile_name}
        )
        if self.fail_connect:
            raise NetworkManagerError(
                OperatorError(
                    code="NM_WIFI_AUTH_FAILED",
                    message="Failed to connect upstream Wi-Fi SSID studio-wifi: Wi-Fi authentication failed.",
                    detail="forced auth failure",
                    suggestion="Retry with correct password.",
                )
            )
        return {
            "ssid": ssid,
            "target_device": "wlan0",
            "snapshot": _snapshot(ap_profile_name).to_dict(),
        }

    def recheck_upstream_status(self, ap_profile_name: str = "photovault-ap") -> dict[str, object]:
        if self.fail_recheck:
            raise NetworkManagerError(
                OperatorError(
                    code="NM_COMMAND_FAILED",
                    message="Failed to recheck upstream internet connectivity: nmcli command failed.",
                    detail="forced recheck failure",
                    suggestion="retry",
                )
            )
        self.recheck_called = True
        return {
            "connectivity_check": "full",
            "snapshot": _snapshot(ap_profile_name).to_dict(),
        }


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
        assert status_payload["snapshot"]["local_ap_ready"] is True
        assert status_payload["snapshot"]["upstream_status"] == "internet_reachable"
        assert status_payload["snapshot"]["upstream_internet_reachable"] is True

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


def test_network_sta_connect_updates_upstream_via_network_manager(tmp_path: Path) -> None:
    manager = _FakeNetworkManager()
    app = create_app(db_path=tmp_path / "state.sqlite3", network_manager=manager)
    with TestClient(app) as client:
        response = client.post(
            "/network/sta-connect",
            json={"ssid": "studio-wifi", "password": "validpass11"},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["ssid"] == "studio-wifi"
        assert payload["target_device"] == "wlan0"
        assert payload["snapshot"]["sta_connected"] is True

    assert manager.connect_calls[-1] == {
        "ssid": "studio-wifi",
        "password": "validpass11",
        "ap_profile_name": "photovault-ap",
    }


def test_network_sta_connect_validation_and_failure_handling(tmp_path: Path) -> None:
    manager = _FakeNetworkManager()
    manager.fail_connect = True
    app = create_app(db_path=tmp_path / "state.sqlite3", network_manager=manager)
    with TestClient(app) as client:
        invalid = client.post("/network/sta-connect", json={"ssid": "  ", "password": "short"})
        assert invalid.status_code == 422
        assert invalid.json()["detail"]["code"] == "STA_CONNECT_INVALID"

        failure = client.post("/network/sta-connect", json={"ssid": "studio-wifi", "password": "validpass11"})
        assert failure.status_code == 503
        assert failure.json()["detail"]["code"] == "NM_WIFI_AUTH_FAILED"


def test_network_upstream_recheck_endpoint_returns_snapshot_and_error_handling(tmp_path: Path) -> None:
    manager = _FakeNetworkManager()
    app = create_app(db_path=tmp_path / "state.sqlite3", network_manager=manager)
    with TestClient(app) as client:
        ok_response = client.post("/network/upstream-recheck")
        assert ok_response.status_code == 200
        payload = ok_response.json()
        assert payload["connectivity_check"] == "full"
        assert payload["snapshot"]["upstream_status"] == "internet_reachable"
        assert manager.recheck_called is True

    manager_failure = _FakeNetworkManager()
    manager_failure.fail_recheck = True
    app_failure = create_app(db_path=tmp_path / "state-failure.sqlite3", network_manager=manager_failure)
    with TestClient(app_failure) as client:
        failure = client.post("/network/upstream-recheck")
        assert failure.status_code == 503
        assert failure.json()["detail"]["code"] == "NM_COMMAND_FAILED"
