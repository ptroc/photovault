import subprocess

import pytest
from photovault_clientd.networking import (
    NetworkManagerAdapter,
    NetworkManagerError,
    _extract_http_status_code,
    parse_nmcli_multiline,
)


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


def test_ensure_ap_profile_enforces_wpa2_rsn_ccmp() -> None:
    observed: list[list[str]] = []
    expected_modify = [
        "connection",
        "modify",
        "photovault-ap",
        "802-11-wireless.mode",
        "ap",
        "802-11-wireless.band",
        "bg",
        "connection.autoconnect",
        "yes",
        "802-11-wireless-security.key-mgmt",
        "wpa-psk",
        "802-11-wireless-security.proto",
        "rsn",
        "802-11-wireless-security.pairwise",
        "ccmp",
        "802-11-wireless-security.group",
        "ccmp",
        "802-11-wireless-security.psk",
        "photovault123",
        "ipv4.method",
        "shared",
        "ipv6.method",
        "ignore",
    ]
    outputs = {
        tuple(["-t", "-f", "NAME", "connection", "show"]): "photovault-ap\n",
        tuple(expected_modify): "",
        tuple(["connection", "modify", "photovault-ap", "802-11-wireless.ssid", "photovault-ap"]): "",
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
    payload = adapter.ensure_ap_profile(
        profile_name="photovault-ap",
        ssid="photovault-ap",
        password="photovault123",
    )
    assert expected_modify in observed
    assert payload["ap_profile"]["key_mgmt"] == "wpa-psk"


def test_adapter_builds_status_snapshot_from_nmcli_outputs() -> None:
    outputs = {
        tuple(["-m", "multiline", "-f", "STATE,CONNECTIVITY,WIFI", "general"]): (
            "STATE: connected\nCONNECTIVITY: full\nWIFI: enabled\n"
        ),
        tuple(["-m", "multiline", "-f", "DEVICE,TYPE,STATE,CONNECTION", "device", "status"]): (
            "DEVICE: wlan7\nTYPE: wifi\nSTATE: connected\nCONNECTION: home-wifi\n"
            "DEVICE: wlan9\nTYPE: wifi\nSTATE: connected\nCONNECTION: photovault-ap\n"
        ),
        tuple(["-m", "multiline", "-f", "GENERAL.IP4-CONNECTIVITY", "device", "show", "wlan7"]): (
            "GENERAL.IP4-CONNECTIVITY: 4 (full)\n"
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
    assert payload["upstream_connectivity"] == "full"
    assert payload["upstream_status"] == "internet_reachable"
    assert payload["upstream_no_usable_internet"] is False
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
        tuple(["-m", "multiline", "-f", "GENERAL.IP4-CONNECTIVITY", "device", "show", "wlan0"]): (
            "GENERAL.IP4-CONNECTIVITY: 3 (limited)\n"
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
    assert snapshot["upstream_connectivity"] == "limited"
    assert snapshot["upstream_status"] == "no_usable_internet"
    assert snapshot["upstream_no_usable_internet"] is True
    assert snapshot["captive_portal_detected"] is False


def test_adapter_marks_portal_connectivity_as_captive_portal_likely() -> None:
    outputs = {
        tuple(["-m", "multiline", "-f", "STATE,CONNECTIVITY,WIFI", "general"]): (
            "STATE: connected\nCONNECTIVITY: portal\nWIFI: enabled\n"
        ),
        tuple(["-m", "multiline", "-f", "DEVICE,TYPE,STATE,CONNECTION", "device", "status"]): (
            "DEVICE: wlan1\nTYPE: wifi\nSTATE: connected\nCONNECTION: photovault-ap\n"
            "DEVICE: wlan0\nTYPE: wifi\nSTATE: connected\nCONNECTION: cpa-test\n"
        ),
        tuple(["-m", "multiline", "-f", "GENERAL.IP4-CONNECTIVITY", "device", "show", "wlan0"]): (
            "GENERAL.IP4-CONNECTIVITY: 2 (portal)\n"
        ),
        tuple(
            ["-m", "multiline", "-f", "IN-USE,SSID,SIGNAL,SECURITY,CHAN,RATE", "device", "wifi", "list"]
        ): (
            "IN-USE: *\nSSID: cpa-test\nSIGNAL: 63\nSECURITY: WPA2\nCHAN: 6\nRATE: 150 Mbit/s\n"
        ),
        tuple(["-t", "-f", "NAME", "connection", "show"]): "photovault-ap\ncpa-test\n",
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
    assert snapshot["upstream_connectivity"] == "portal"
    assert snapshot["upstream_status"] == "captive_portal_likely"
    assert snapshot["upstream_no_usable_internet"] is True
    assert snapshot["captive_portal_detected"] is True


def test_adapter_uses_sta_probe_when_nm_reports_full_connectivity() -> None:
    outputs = {
        tuple(["-m", "multiline", "-f", "STATE,CONNECTIVITY,WIFI", "general"]): (
            "STATE: connected\nCONNECTIVITY: full\nWIFI: enabled\n"
        ),
        tuple(["-m", "multiline", "-f", "DEVICE,TYPE,STATE,CONNECTION", "device", "status"]): (
            "DEVICE: wlan1\nTYPE: wifi\nSTATE: connected\nCONNECTION: photovault-ap\n"
            "DEVICE: wlan0\nTYPE: wifi\nSTATE: connected\nCONNECTION: cpa-test\n"
        ),
        tuple(["-m", "multiline", "-f", "GENERAL.IP4-CONNECTIVITY", "device", "show", "wlan0"]): (
            "GENERAL.IP4-CONNECTIVITY: 4 (full)\n"
        ),
        tuple(
            ["-m", "multiline", "-f", "IN-USE,SSID,SIGNAL,SECURITY,CHAN,RATE", "device", "wifi", "list"]
        ): "IN-USE: *\nSSID: cpa-test\nSIGNAL: 70\nSECURITY: --\nCHAN: 6\nRATE: 260 Mbit/s\n",
        tuple(["-t", "-f", "NAME", "connection", "show"]): "photovault-ap\ncpa-test\n",
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

    adapter = NetworkManagerAdapter(command_runner=runner, connectivity_probe=lambda _name: "portal")
    snapshot = adapter.status_snapshot("photovault-ap").to_dict()
    assert snapshot["upstream_connectivity"] == "portal"
    assert snapshot["upstream_status"] == "captive_portal_likely"
    assert snapshot["captive_portal_detected"] is True


def test_extract_http_status_code_from_headers() -> None:
    headers = (
        "HTTP/1.1 302 Moved Temporarily\n"
        "Connection: close\n"
        "Location: http://10.0.0.1/guest\n"
    )
    assert _extract_http_status_code(headers) == 302
    assert _extract_http_status_code("Server: unit-test\n") is None


def test_recheck_upstream_status_runs_connectivity_check_then_snapshot() -> None:
    observed: list[list[str]] = []
    outputs = {
        tuple(["networking", "connectivity", "check"]): "portal\n",
        tuple(["-m", "multiline", "-f", "STATE,CONNECTIVITY,WIFI", "general"]): (
            "STATE: connected\nCONNECTIVITY: portal\nWIFI: enabled\n"
        ),
        tuple(["-m", "multiline", "-f", "DEVICE,TYPE,STATE,CONNECTION", "device", "status"]): (
            "DEVICE: wlan1\nTYPE: wifi\nSTATE: connected\nCONNECTION: photovault-ap\n"
            "DEVICE: wlan0\nTYPE: wifi\nSTATE: connected\nCONNECTION: cpa-test\n"
        ),
        tuple(["-m", "multiline", "-f", "GENERAL.IP4-CONNECTIVITY", "device", "show", "wlan0"]): (
            "GENERAL.IP4-CONNECTIVITY: 2 (portal)\n"
        ),
        tuple(
            ["-m", "multiline", "-f", "IN-USE,SSID,SIGNAL,SECURITY,CHAN,RATE", "device", "wifi", "list"]
        ): (
            "IN-USE: *\nSSID: cpa-test\nSIGNAL: 63\nSECURITY: WPA2\nCHAN: 6\nRATE: 150 Mbit/s\n"
        ),
        tuple(["-t", "-f", "NAME", "connection", "show"]): "photovault-ap\ncpa-test\n",
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
    payload = adapter.recheck_upstream_status("photovault-ap")
    assert payload["connectivity_check"] == "portal"
    assert payload["snapshot"]["upstream_connectivity"] == "portal"
    assert payload["snapshot"]["upstream_status"] == "captive_portal_likely"
    assert observed[0] == ["networking", "connectivity", "check"]


def test_connect_sta_network_uses_non_ap_wifi_device() -> None:
    observed: list[list[str]] = []
    outputs = {
        tuple(["-m", "multiline", "-f", "DEVICE,TYPE,STATE,CONNECTION", "device", "status"]): (
            "DEVICE: wlan1\nTYPE: wifi\nSTATE: connected\nCONNECTION: photovault-ap\n"
            "DEVICE: wlan0\nTYPE: wifi\nSTATE: connected\nCONNECTION: hotel-wifi\n"
        ),
        tuple(
            [
                "connection",
                "add",
                "type",
                "wifi",
                "ifname",
                "wlan0",
                "con-name",
                "photovault-sta-hotel-wifi",
                "ssid",
                "hotel-wifi",
            ]
        ): "",
        tuple(
            [
                "connection",
                "modify",
                "photovault-sta-hotel-wifi",
                "connection.interface-name",
                "wlan0",
                "connection.autoconnect",
                "yes",
                "802-11-wireless.ssid",
                "hotel-wifi",
                "802-11-wireless.mode",
                "infrastructure",
                "802-11-wireless-security.key-mgmt",
                "wpa-psk",
                "802-11-wireless-security.psk",
                "validpass11",
                "ipv4.method",
                "auto",
                "ipv6.method",
                "ignore",
            ]
        ): "",
        tuple(["connection", "up", "photovault-sta-hotel-wifi", "ifname", "wlan0"]): "",
        tuple(["-m", "multiline", "-f", "GENERAL.IP4-CONNECTIVITY", "device", "show", "wlan0"]): (
            "GENERAL.IP4-CONNECTIVITY: 4 (full)\n"
        ),
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
    assert payload["snapshot"]["upstream_connectivity"] == "full"
    assert [
        "connection",
        "add",
        "type",
        "wifi",
        "ifname",
        "wlan0",
        "con-name",
        "photovault-sta-hotel-wifi",
        "ssid",
        "hotel-wifi",
    ] in observed
    assert ["connection", "up", "photovault-sta-hotel-wifi", "ifname", "wlan0"] in observed


def test_connect_sta_network_without_password_uses_nmcli_direct_connect() -> None:
    observed: list[list[str]] = []
    outputs = {
        tuple(["-m", "multiline", "-f", "DEVICE,TYPE,STATE,CONNECTION", "device", "status"]): (
            "DEVICE: wlan1\nTYPE: wifi\nSTATE: connected\nCONNECTION: photovault-ap\n"
            "DEVICE: wlan0\nTYPE: wifi\nSTATE: connected\nCONNECTION: cafe-open\n"
        ),
        tuple(["device", "wifi", "connect", "cafe-open", "ifname", "wlan0"]): "",
        tuple(["-m", "multiline", "-f", "GENERAL.IP4-CONNECTIVITY", "device", "show", "wlan0"]): (
            "GENERAL.IP4-CONNECTIVITY: 4 (full)\n"
        ),
        tuple(["-m", "multiline", "-f", "STATE,CONNECTIVITY,WIFI", "general"]): (
            "STATE: connected\nCONNECTIVITY: full\nWIFI: enabled\n"
        ),
        tuple(
            ["-m", "multiline", "-f", "IN-USE,SSID,SIGNAL,SECURITY,CHAN,RATE", "device", "wifi", "list"]
        ): "IN-USE: *\nSSID: cafe-open\nSIGNAL: 65\nSECURITY: --\nCHAN: 1\nRATE: 130 Mbit/s\n",
        tuple(["-t", "-f", "NAME", "connection", "show"]): "photovault-ap\ncafe-open\n",
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
        ssid="cafe-open",
        password=None,
        ap_profile_name="photovault-ap",
    )
    assert payload["target_device"] == "wlan0"
    assert payload["snapshot"]["upstream_status"] == "internet_reachable"
    assert ["device", "wifi", "connect", "cafe-open", "ifname", "wlan0"] in observed


def test_connect_sta_network_updates_existing_managed_profile() -> None:
    observed: list[list[str]] = []
    outputs = {
        tuple(["-m", "multiline", "-f", "DEVICE,TYPE,STATE,CONNECTION", "device", "status"]): (
            "DEVICE: wlan1\nTYPE: wifi\nSTATE: connected\nCONNECTION: photovault-ap\n"
            "DEVICE: wlan0\nTYPE: wifi\nSTATE: connected\nCONNECTION: hotel-wifi\n"
        ),
        tuple(
            [
                "connection",
                "modify",
                "photovault-sta-hotel-wifi",
                "connection.interface-name",
                "wlan0",
                "connection.autoconnect",
                "yes",
                "802-11-wireless.ssid",
                "hotel-wifi",
                "802-11-wireless.mode",
                "infrastructure",
                "802-11-wireless-security.key-mgmt",
                "wpa-psk",
                "802-11-wireless-security.psk",
                "validpass11",
                "ipv4.method",
                "auto",
                "ipv6.method",
                "ignore",
            ]
        ): "",
        tuple(["connection", "up", "photovault-sta-hotel-wifi", "ifname", "wlan0"]): "",
        tuple(["-m", "multiline", "-f", "GENERAL.IP4-CONNECTIVITY", "device", "show", "wlan0"]): (
            "GENERAL.IP4-CONNECTIVITY: 4 (full)\n"
        ),
        tuple(["-m", "multiline", "-f", "STATE,CONNECTIVITY,WIFI", "general"]): (
            "STATE: connected\nCONNECTIVITY: full\nWIFI: enabled\n"
        ),
        tuple(
            ["-m", "multiline", "-f", "IN-USE,SSID,SIGNAL,SECURITY,CHAN,RATE", "device", "wifi", "list"]
        ): "IN-USE: *\nSSID: hotel-wifi\nSIGNAL: 70\nSECURITY: WPA2\nCHAN: 11\nRATE: 260 Mbit/s\n",
        tuple(["-t", "-f", "NAME", "connection", "show"]): (
            "photovault-ap\nhotel-wifi\nphotovault-sta-hotel-wifi\n"
        ),
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
    add_profile_command = [
        "connection",
        "add",
        "type",
        "wifi",
        "ifname",
        "wlan0",
        "con-name",
        "photovault-sta-hotel-wifi",
        "ssid",
        "hotel-wifi",
    ]
    assert payload["target_device"] == "wlan0"
    assert ["connection", "up", "photovault-sta-hotel-wifi", "ifname", "wlan0"] in observed
    assert add_profile_command not in observed


def test_connect_sta_network_uses_managed_profile_even_with_external_ssid_profile() -> None:
    observed: list[list[str]] = []
    outputs = {
        tuple(["-m", "multiline", "-f", "DEVICE,TYPE,STATE,CONNECTION", "device", "status"]): (
            "DEVICE: wlan1\nTYPE: wifi\nSTATE: connected\nCONNECTION: photovault-ap\n"
            "DEVICE: wlan0\nTYPE: wifi\nSTATE: connected\nCONNECTION: netplan-wlan0-hotel\n"
        ),
        tuple(
            [
                "connection",
                "add",
                "type",
                "wifi",
                "ifname",
                "wlan0",
                "con-name",
                "photovault-sta-hotel-wifi",
                "ssid",
                "hotel-wifi",
            ]
        ): "",
        tuple(
            [
                "connection",
                "modify",
                "photovault-sta-hotel-wifi",
                "connection.interface-name",
                "wlan0",
                "connection.autoconnect",
                "yes",
                "802-11-wireless.ssid",
                "hotel-wifi",
                "802-11-wireless.mode",
                "infrastructure",
                "802-11-wireless-security.key-mgmt",
                "wpa-psk",
                "802-11-wireless-security.psk",
                "validpass11",
                "ipv4.method",
                "auto",
                "ipv6.method",
                "ignore",
            ]
        ): "",
        tuple(["connection", "up", "photovault-sta-hotel-wifi", "ifname", "wlan0"]): "",
        tuple(["-m", "multiline", "-f", "GENERAL.IP4-CONNECTIVITY", "device", "show", "wlan0"]): (
            "GENERAL.IP4-CONNECTIVITY: 4 (full)\n"
        ),
        tuple(["-m", "multiline", "-f", "STATE,CONNECTIVITY,WIFI", "general"]): (
            "STATE: connected\nCONNECTIVITY: full\nWIFI: enabled\n"
        ),
        tuple(
            ["-m", "multiline", "-f", "IN-USE,SSID,SIGNAL,SECURITY,CHAN,RATE", "device", "wifi", "list"]
        ): "IN-USE: *\nSSID: hotel-wifi\nSIGNAL: 70\nSECURITY: WPA2\nCHAN: 11\nRATE: 260 Mbit/s\n",
        tuple(["-t", "-f", "NAME", "connection", "show"]): (
            "photovault-ap\nhotel-wifi\nphotovault-sta-hotel-wifi\n"
        ),
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
    assert ["connection", "up", "photovault-sta-hotel-wifi", "ifname", "wlan0"] in observed
    assert [
        "device",
        "wifi",
        "connect",
        "hotel-wifi",
        "ifname",
        "wlan0",
        "password",
        "validpass11",
        "name",
        "photovault-sta-hotel-wifi",
    ] not in observed


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


def test_connect_sta_network_surfaces_invalid_profile_error() -> None:
    def runner(args: list[str]) -> str:
        if args == ["-m", "multiline", "-f", "DEVICE,TYPE,STATE,CONNECTION", "device", "status"]:
            return (
                "DEVICE: wlan1\nTYPE: wifi\nSTATE: connected\nCONNECTION: photovault-ap\n"
                "DEVICE: wlan0\nTYPE: wifi\nSTATE: connected\nCONNECTION: hotel-wifi\n"
            )
        raise subprocess.CalledProcessError(
            2,
            ["nmcli", *args],
            stderr=(
                "Error: connection update failed: "
                "802-11-wireless-security.key-mgmt: property is missing"
            ),
        )

    adapter = NetworkManagerAdapter(command_runner=runner)
    with pytest.raises(NetworkManagerError) as exc:
        adapter.connect_sta_network(
            ssid="hotel-wifi",
            password="validpass11",
            ap_profile_name="photovault-ap",
        )
    assert exc.value.operator_error.code == "NM_WIFI_PROFILE_INVALID"


def test_start_portal_handoff_requires_captive_portal_state() -> None:
    outputs = {
        tuple(["-m", "multiline", "-f", "STATE,CONNECTIVITY,WIFI", "general"]): (
            "STATE: connected\nCONNECTIVITY: full\nWIFI: enabled\n"
        ),
        tuple(["-m", "multiline", "-f", "DEVICE,TYPE,STATE,CONNECTION", "device", "status"]): (
            "DEVICE: wlan1\nTYPE: wifi\nSTATE: connected\nCONNECTION: photovault-ap\n"
            "DEVICE: wlan0\nTYPE: wifi\nSTATE: connected\nCONNECTION: cpa-test\n"
            "DEVICE: eth0\nTYPE: ethernet\nSTATE: connected\nCONNECTION: Wired connection 1\n"
        ),
        tuple(["-m", "multiline", "-f", "GENERAL.IP4-CONNECTIVITY", "device", "show", "wlan0"]): (
            "GENERAL.IP4-CONNECTIVITY: 4 (full)\n"
        ),
        tuple(
            ["-m", "multiline", "-f", "IN-USE,SSID,SIGNAL,SECURITY,CHAN,RATE", "device", "wifi", "list"]
        ): "IN-USE: *\nSSID: cpa-test\nSIGNAL: 60\nSECURITY: --\nCHAN: 6\nRATE: 260 Mbit/s\n",
        tuple(["-t", "-f", "NAME", "connection", "show"]): (
            "photovault-ap\ncpa-test\nWired connection 1\n"
        ),
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

    adapter = NetworkManagerAdapter(command_runner=runner, connectivity_probe=lambda _device: "full")
    with pytest.raises(NetworkManagerError) as exc:
        adapter.start_portal_handoff(ap_profile_name="photovault-ap")
    assert exc.value.operator_error.code == "NM_PORTAL_HANDOFF_INVALID_STATE"


def test_start_portal_handoff_modifies_active_ethernet_connections() -> None:
    observed: list[list[str]] = []
    outputs = {
        tuple(["-m", "multiline", "-f", "STATE,CONNECTIVITY,WIFI", "general"]): (
            "STATE: connected\nCONNECTIVITY: full\nWIFI: enabled\n"
        ),
        tuple(["-m", "multiline", "-f", "DEVICE,TYPE,STATE,CONNECTION", "device", "status"]): (
            "DEVICE: wlan1\nTYPE: wifi\nSTATE: connected\nCONNECTION: photovault-ap\n"
            "DEVICE: wlan0\nTYPE: wifi\nSTATE: connected\nCONNECTION: cpa-test\n"
            "DEVICE: eth0\nTYPE: ethernet\nSTATE: connected\nCONNECTION: Wired connection 1\n"
        ),
        tuple(["-m", "multiline", "-f", "GENERAL.IP4-CONNECTIVITY", "device", "show", "wlan0"]): (
            "GENERAL.IP4-CONNECTIVITY: 4 (full)\n"
        ),
        tuple(
            ["-m", "multiline", "-f", "IN-USE,SSID,SIGNAL,SECURITY,CHAN,RATE", "device", "wifi", "list"]
        ): "IN-USE: *\nSSID: cpa-test\nSIGNAL: 60\nSECURITY: --\nCHAN: 6\nRATE: 260 Mbit/s\n",
        tuple(["-t", "-f", "NAME", "connection", "show"]): (
            "photovault-ap\ncpa-test\nWired connection 1\n"
        ),
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
        tuple(
            [
                "-m",
                "multiline",
                "-f",
                "connection.id,ipv4.never-default,ipv6.never-default",
                "connection",
                "show",
                "Wired connection 1",
            ]
        ): (
            "connection.id: Wired connection 1\n"
            "ipv4.never-default: no\n"
            "ipv6.never-default: no\n"
        ),
        tuple(
            [
                "connection",
                "modify",
                "Wired connection 1",
                "ipv4.never-default",
                "yes",
                "ipv6.never-default",
                "yes",
            ]
        ): "",
        tuple(["connection", "up", "Wired connection 1", "ifname", "eth0"]): "",
    }

    def runner(args: list[str]) -> str:
        observed.append(args)
        key = tuple(args)
        if key not in outputs:
            raise AssertionError(f"unexpected nmcli args: {args}")
        return outputs[key]

    adapter = NetworkManagerAdapter(command_runner=runner, connectivity_probe=lambda _device: "portal")
    payload = adapter.start_portal_handoff(ap_profile_name="photovault-ap")
    assert payload["modified_ethernet_connections"] == ["Wired connection 1"]
    assert payload["previous_eth_route_prefs"] == [
        {
            "connection_name": "Wired connection 1",
            "device_name": "eth0",
            "ipv4_never_default": "no",
            "ipv6_never_default": "no",
        }
    ]
    assert [
        "connection",
        "modify",
        "Wired connection 1",
        "ipv4.never-default",
        "yes",
        "ipv6.never-default",
        "yes",
    ] in observed
    assert ["connection", "up", "Wired connection 1", "ifname", "eth0"] in observed


def test_start_portal_handoff_requires_active_ethernet_connection() -> None:
    outputs = {
        tuple(["-m", "multiline", "-f", "STATE,CONNECTIVITY,WIFI", "general"]): (
            "STATE: connected\nCONNECTIVITY: full\nWIFI: enabled\n"
        ),
        tuple(["-m", "multiline", "-f", "DEVICE,TYPE,STATE,CONNECTION", "device", "status"]): (
            "DEVICE: wlan1\nTYPE: wifi\nSTATE: connected\nCONNECTION: photovault-ap\n"
            "DEVICE: wlan0\nTYPE: wifi\nSTATE: connected\nCONNECTION: cpa-test\n"
        ),
        tuple(["-m", "multiline", "-f", "GENERAL.IP4-CONNECTIVITY", "device", "show", "wlan0"]): (
            "GENERAL.IP4-CONNECTIVITY: 4 (full)\n"
        ),
        tuple(
            ["-m", "multiline", "-f", "IN-USE,SSID,SIGNAL,SECURITY,CHAN,RATE", "device", "wifi", "list"]
        ): "IN-USE: *\nSSID: cpa-test\nSIGNAL: 60\nSECURITY: --\nCHAN: 6\nRATE: 260 Mbit/s\n",
        tuple(["-t", "-f", "NAME", "connection", "show"]): (
            "photovault-ap\ncpa-test\n"
        ),
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

    adapter = NetworkManagerAdapter(command_runner=runner, connectivity_probe=lambda _device: "portal")
    with pytest.raises(NetworkManagerError) as exc:
        adapter.start_portal_handoff(ap_profile_name="photovault-ap")
    assert exc.value.operator_error.code == "NM_PORTAL_HANDOFF_NO_ETH"


def test_stop_portal_handoff_restores_previous_route_preferences() -> None:
    observed: list[list[str]] = []
    outputs = {
        tuple(
            [
                "connection",
                "modify",
                "Wired connection 1",
                "ipv4.never-default",
                "no",
                "ipv6.never-default",
                "no",
            ]
        ): "",
        tuple(["connection", "up", "Wired connection 1", "ifname", "eth0"]): "",
    }

    def runner(args: list[str]) -> str:
        observed.append(args)
        key = tuple(args)
        if key not in outputs:
            raise AssertionError(f"unexpected nmcli args: {args}")
        return outputs[key]

    adapter = NetworkManagerAdapter(command_runner=runner)
    payload = adapter.stop_portal_handoff(
        previous_eth_route_prefs=[
            {
                "connection_name": "Wired connection 1",
                "device_name": "eth0",
                "ipv4_never_default": "no",
                "ipv6_never_default": "no",
            }
        ]
    )
    assert payload["restored_ethernet_connections"] == ["Wired connection 1"]
    assert [
        "connection",
        "modify",
        "Wired connection 1",
        "ipv4.never-default",
        "no",
        "ipv6.never-default",
        "no",
    ] in observed
    assert ["connection", "up", "Wired connection 1", "ifname", "eth0"] in observed


def test_stop_portal_handoff_requires_previous_route_preferences() -> None:
    adapter = NetworkManagerAdapter(command_runner=lambda _args: "")
    with pytest.raises(NetworkManagerError) as exc:
        adapter.stop_portal_handoff(previous_eth_route_prefs=[])
    assert exc.value.operator_error.code == "NM_PORTAL_HANDOFF_RESTORE_INVALID"
