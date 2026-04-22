import subprocess

import pytest
from photovault_clientd.networking import NetworkManagerAdapter, NetworkManagerError, parse_nmcli_multiline


def test_parse_nmcli_multiline_handles_records_without_blank_lines() -> None:
    output = """IN-USE:
SSID: :)
SIGNAL: 89
IN-USE:
SSID: :))
SIGNAL: 66
"""
    rows = parse_nmcli_multiline(output)
    assert len(rows) == 2
    assert rows[0]["SSID"] == ":)"
    assert rows[1]["SSID"] == ":))"


def test_adapter_normalizes_permission_denied_errors() -> None:
    def runner(_args: list[str]) -> str:
        raise subprocess.CalledProcessError(
            10,
            ["nmcli"],
            stderr="Error: org.freedesktop.NetworkManager.wifi.scan request failed: not authorized.",
        )

    adapter = NetworkManagerAdapter(command_runner=runner)
    with pytest.raises(NetworkManagerError) as exc:
        adapter.trigger_wifi_scan()
    assert exc.value.operator_error.code == "NM_PERMISSION_DENIED"


def test_adapter_normalizes_missing_nmcli() -> None:
    def runner(_args: list[str]) -> str:
        raise FileNotFoundError("nmcli")

    adapter = NetworkManagerAdapter(command_runner=runner)
    with pytest.raises(NetworkManagerError) as exc:
        adapter.status_snapshot("photovault-ap")
    assert exc.value.operator_error.code == "NMCLI_MISSING"


def test_adapter_builds_status_snapshot_from_nmcli_outputs() -> None:
    outputs = {
        tuple(["-m", "multiline", "-f", "STATE,CONNECTIVITY,WIFI", "general"]): (
            "STATE: connected\nCONNECTIVITY: full\nWIFI: enabled\n"
        ),
        tuple(["-m", "multiline", "-f", "DEVICE,TYPE,STATE,CONNECTION", "device", "status"]): (
            "DEVICE: wlan7\nTYPE: wifi\nSTATE: connected\nCONNECTION: home-wifi\n"
            "DEVICE: wlan9\nTYPE: wifi\nSTATE: connected\nCONNECTION: photovault-ap\n"
        ),
        tuple(
            ["-m", "multiline", "-f", "IN-USE,SSID,SIGNAL,SECURITY,CHAN,RATE", "device", "wifi", "list"]
        ): "IN-USE: *\nSSID: home-wifi\nSIGNAL: 71\nSECURITY: WPA2\nCHAN: 40\nRATE: 540 Mbit/s\n",
        tuple(["-t", "-f", "NAME", "connection", "show"]): "photovault-ap\nhome-wifi\n",
        tuple(
            [
                "-m",
                "multiline",
                "-f",
                (
                    "connection.id,connection.autoconnect,802-11-wireless.ssid,"
                    "802-11-wireless.mode,802-11-wireless-security.key-mgmt,GENERAL.STATE"
                ),
                "connection",
                "show",
                "photovault-ap",
            ]
        ): (
            "connection.id: photovault-ap\nconnection.autoconnect: yes\n"
            "802-11-wireless.ssid: photovault-ap\n802-11-wireless.mode: ap\n"
            "802-11-wireless-security.key-mgmt: wpa-psk\nGENERAL.STATE: activated\n"
        ),
    }

    def runner(args: list[str]) -> str:
        key = tuple(args)
        if key not in outputs:
            raise AssertionError(f"unexpected nmcli args: {args}")
        return outputs[key]

    adapter = NetworkManagerAdapter(command_runner=runner)
    snapshot = adapter.status_snapshot("photovault-ap")
    payload = snapshot.to_dict()
    assert payload["general"]["connectivity"] == "full"
    assert payload["ap_profile"]["exists"] is True
    assert payload["sta_connected"] is True
    assert payload["local_ap_ready"] is True
    assert payload["upstream_internet_reachable"] is True
    assert payload["captive_portal_detected"] is False
    assert payload["ap_device_names"] == ["wlan9"]
    assert payload["sta_device_names"] == ["wlan7"]
    assert payload["sta_connection_names"] == ["home-wifi"]


def test_adapter_deduplicates_wifi_scan_rows_and_filters_hidden_entries() -> None:
    outputs = {
        tuple(["-m", "multiline", "-f", "STATE,CONNECTIVITY,WIFI", "general"]): (
            "STATE: connected\nCONNECTIVITY: limited\nWIFI: enabled\n"
        ),
        tuple(["-m", "multiline", "-f", "DEVICE,TYPE,STATE,CONNECTION", "device", "status"]): (
            "DEVICE: wlan1\nTYPE: wifi\nSTATE: connected\nCONNECTION: photovault-ap\n"
            "DEVICE: wlan0\nTYPE: wifi\nSTATE: connected\nCONNECTION: hotel-wifi\n"
        ),
        tuple(
            ["-m", "multiline", "-f", "IN-USE,SSID,SIGNAL,SECURITY,CHAN,RATE", "device", "wifi", "list"]
        ): (
            "IN-USE: \nSSID: --\nSIGNAL: 90\nSECURITY: WPA2\nCHAN: 1\nRATE: 130 Mbit/s\n"
            "IN-USE: \nSSID: hotel-wifi\nSIGNAL: 40\nSECURITY: WPA2\nCHAN: 11\nRATE: 260 Mbit/s\n"
            "IN-USE: *\nSSID: hotel-wifi\nSIGNAL: 38\nSECURITY: WPA2\nCHAN: 11\nRATE: 260 Mbit/s\n"
            "IN-USE: \nSSID: hotel-wifi\nSIGNAL: 61\nSECURITY: WPA2\nCHAN: 11\nRATE: 260 Mbit/s\n"
        ),
        tuple(["-t", "-f", "NAME", "connection", "show"]): "photovault-ap\nhotel-wifi\n",
        tuple(
            [
                "-m",
                "multiline",
                "-f",
                (
                    "connection.id,connection.autoconnect,802-11-wireless.ssid,"
                    "802-11-wireless.mode,802-11-wireless-security.key-mgmt,GENERAL.STATE"
                ),
                "connection",
                "show",
                "photovault-ap",
            ]
        ): (
            "connection.id: photovault-ap\nconnection.autoconnect: yes\n"
            "802-11-wireless.ssid: photovault-ap\n802-11-wireless.mode: ap\n"
            "802-11-wireless-security.key-mgmt: wpa-psk\nGENERAL.STATE: activated\n"
        ),
    }

    def runner(args: list[str]) -> str:
        key = tuple(args)
        if key not in outputs:
            raise AssertionError(f"unexpected nmcli args: {args}")
        return outputs[key]

    adapter = NetworkManagerAdapter(command_runner=runner)
    snapshot = adapter.status_snapshot("photovault-ap").to_dict()
    assert len(snapshot["wifi_networks"]) == 1
    assert snapshot["wifi_networks"][0]["ssid"] == "hotel-wifi"
    assert snapshot["wifi_networks"][0]["in_use"] == "*"
    assert snapshot["captive_portal_detected"] is True


def test_connect_sta_network_uses_non_ap_wifi_device() -> None:
    observed: list[list[str]] = []
    outputs = {
        tuple(["-m", "multiline", "-f", "DEVICE,TYPE,STATE,CONNECTION", "device", "status"]): (
            "DEVICE: wlan1\nTYPE: wifi\nSTATE: connected\nCONNECTION: photovault-ap\n"
            "DEVICE: wlan0\nTYPE: wifi\nSTATE: connected\nCONNECTION: hotel-wifi\n"
        ),
        tuple(["device", "wifi", "connect", "hotel-wifi", "ifname", "wlan0", "password", "validpass11"]): "",
        tuple(["-m", "multiline", "-f", "STATE,CONNECTIVITY,WIFI", "general"]): (
            "STATE: connected\nCONNECTIVITY: full\nWIFI: enabled\n"
        ),
        tuple(
            ["-m", "multiline", "-f", "IN-USE,SSID,SIGNAL,SECURITY,CHAN,RATE", "device", "wifi", "list"]
        ): "IN-USE: *\nSSID: hotel-wifi\nSIGNAL: 70\nSECURITY: WPA2\nCHAN: 11\nRATE: 260 Mbit/s\n",
        tuple(["-t", "-f", "NAME", "connection", "show"]): "photovault-ap\nhotel-wifi\n",
        tuple(
            [
                "-m",
                "multiline",
                "-f",
                (
                    "connection.id,connection.autoconnect,802-11-wireless.ssid,"
                    "802-11-wireless.mode,802-11-wireless-security.key-mgmt,GENERAL.STATE"
                ),
                "connection",
                "show",
                "photovault-ap",
            ]
        ): (
            "connection.id: photovault-ap\nconnection.autoconnect: yes\n"
            "802-11-wireless.ssid: photovault-ap\n802-11-wireless.mode: ap\n"
            "802-11-wireless-security.key-mgmt: wpa-psk\nGENERAL.STATE: activated\n"
        ),
    }

    def runner(args: list[str]) -> str:
        observed.append(args)
        key = tuple(args)
        if key not in outputs:
            raise AssertionError(f"unexpected nmcli args: {args}")
        return outputs[key]

    adapter = NetworkManagerAdapter(command_runner=runner)
    payload = adapter.connect_sta_network(
        ssid="hotel-wifi",
        password="validpass11",
        ap_profile_name="photovault-ap",
    )
    assert payload["target_device"] == "wlan0"
    assert payload["snapshot"]["sta_connected"] is True
    assert [
        "device",
        "wifi",
        "connect",
        "hotel-wifi",
        "ifname",
        "wlan0",
        "password",
        "validpass11",
    ] in observed


def test_connect_sta_network_requires_non_ap_device() -> None:
    outputs = {
        tuple(["-m", "multiline", "-f", "DEVICE,TYPE,STATE,CONNECTION", "device", "status"]): (
            "DEVICE: wlan1\nTYPE: wifi\nSTATE: connected\nCONNECTION: photovault-ap\n"
        ),
    }

    def runner(args: list[str]) -> str:
        key = tuple(args)
        if key not in outputs:
            raise AssertionError(f"unexpected nmcli args: {args}")
        return outputs[key]

    adapter = NetworkManagerAdapter(command_runner=runner)
    with pytest.raises(NetworkManagerError) as exc:
        adapter.connect_sta_network(ssid="hotel-wifi", password=None, ap_profile_name="photovault-ap")
    assert exc.value.operator_error.code == "NM_STA_DEVICE_UNAVAILABLE"
