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
            "DEVICE: wlan0\nTYPE: wifi\nSTATE: connected\nCONNECTION: home-wifi\n"
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
