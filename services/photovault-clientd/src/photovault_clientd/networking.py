"""Deterministic NetworkManager adapter for photovault-clientd."""

from __future__ import annotations

import subprocess
from dataclasses import asdict, dataclass
from typing import Any, Callable

DEFAULT_AP_PROFILE_NAME = "photovault-ap"
DEFAULT_AP_SSID = "photovault-ap"
DEFAULT_AP_PASSWORD = "photovault123"


@dataclass(frozen=True)
class OperatorError:
    code: str
    message: str
    detail: str
    suggestion: str


@dataclass(frozen=True)
class NetworkGeneral:
    state: str
    connectivity: str
    wifi: str


@dataclass(frozen=True)
class NetworkDevice:
    device: str
    type: str
    state: str
    connection: str


@dataclass(frozen=True)
class WifiNetwork:
    in_use: str
    ssid: str
    signal: str
    security: str
    channel: str
    rate: str


@dataclass(frozen=True)
class AccessPointProfile:
    profile_name: str
    exists: bool
    active: bool
    ssid: str
    autoconnect: str
    mode: str
    key_mgmt: str


@dataclass(frozen=True)
class NetworkStatusSnapshot:
    general: NetworkGeneral
    devices: list[NetworkDevice]
    wifi_networks: list[WifiNetwork]
    ap_profile: AccessPointProfile
    sta_connected: bool
    ap_device_names: list[str]
    sta_device_names: list[str]
    sta_connection_names: list[str]
    local_ap_ready: bool
    upstream_internet_reachable: bool
    captive_portal_detected: bool
    next_operator_action: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class NetworkManagerError(RuntimeError):
    def __init__(self, operator_error: OperatorError) -> None:
        super().__init__(operator_error.message)
        self.operator_error = operator_error

    def to_payload(self) -> dict[str, str]:
        return asdict(self.operator_error)


def _run_nmcli(args: list[str]) -> str:
    completed = subprocess.run(
        ["nmcli", *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout


def _normalize_failure(action: str, exc: Exception) -> NetworkManagerError:
    if isinstance(exc, FileNotFoundError):
        return NetworkManagerError(
            OperatorError(
                code="NMCLI_MISSING",
                message=f"Failed to {action}: nmcli is not installed on this device.",
                detail=str(exc),
                suggestion="Install NetworkManager/nmcli and restart photovault-clientd.",
            )
        )
    if isinstance(exc, subprocess.CalledProcessError):
        stderr = (exc.stderr or "").strip()
        stdout = (exc.stdout or "").strip()
        detail = stderr or stdout or f"nmcli exited with status {exc.returncode}"
        detail_lower = detail.lower()
        if "not authorized" in detail.lower():
            return NetworkManagerError(
                OperatorError(
                    code="NM_PERMISSION_DENIED",
                    message=f"Failed to {action}: NetworkManager denied the photovault service user.",
                    detail=detail,
                    suggestion=(
                        "Configure a polkit rule allowing this service user to manage "
                        "NetworkManager Wi-Fi operations."
                    ),
                )
            )
        if "no network with ssid" in detail_lower:
            return NetworkManagerError(
                OperatorError(
                    code="NM_WIFI_NOT_FOUND",
                    message=f"Failed to {action}: requested Wi-Fi network was not found.",
                    detail=detail,
                    suggestion="Run Wi-Fi scan and verify the SSID before retrying.",
                )
            )
        if "secrets were required" in detail_lower or "invalid wifi password" in detail_lower:
            return NetworkManagerError(
                OperatorError(
                    code="NM_WIFI_AUTH_FAILED",
                    message=f"Failed to {action}: Wi-Fi authentication failed.",
                    detail=detail,
                    suggestion="Verify Wi-Fi password/security settings and retry.",
                )
            )
        return NetworkManagerError(
            OperatorError(
                code="NM_COMMAND_FAILED",
                message=f"Failed to {action}: nmcli command failed.",
                detail=detail,
                suggestion="Inspect NetworkManager logs and retry the requested operation.",
            )
        )
    if isinstance(exc, ValueError):
        return NetworkManagerError(
            OperatorError(
                code="NM_PARSE_ERROR",
                message=f"Failed to {action}: unable to parse NetworkManager output.",
                detail=str(exc),
                suggestion="Retry; if this persists, capture nmcli output and inspect parser assumptions.",
            )
        )
    return NetworkManagerError(
        OperatorError(
            code="NM_COMMAND_FAILED",
            message=f"Failed to {action}: unexpected networking error.",
            detail=str(exc),
            suggestion="Inspect daemon logs and retry the requested operation.",
        )
    )


def parse_nmcli_multiline(output: str) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    current: dict[str, str] = {}
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line:
            if current:
                records.append(current)
                current = {}
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        normalized_key = key.strip()
        if normalized_key in current and current:
            records.append(current)
            current = {}
        current[normalized_key] = value.strip()
    if current:
        records.append(current)
    return records


class NetworkManagerAdapter:
    def __init__(self, command_runner: Callable[[list[str]], str] = _run_nmcli) -> None:
        self._command_runner = command_runner

    def _run(self, args: list[str], *, action: str) -> str:
        try:
            return self._command_runner(args)
        except Exception as exc:  # pragma: no cover - covered via _normalize_failure tests
            raise _normalize_failure(action, exc) from exc

    def _network_general(self) -> NetworkGeneral:
        output = self._run(
            ["-m", "multiline", "-f", "STATE,CONNECTIVITY,WIFI", "general"],
            action="load NetworkManager status",
        )
        records = parse_nmcli_multiline(output)
        general = records[0] if records else {}
        return NetworkGeneral(
            state=general.get("STATE", "unknown"),
            connectivity=general.get("CONNECTIVITY", "unknown"),
            wifi=general.get("WIFI", "unknown"),
        )

    def _network_devices(self) -> list[NetworkDevice]:
        output = self._run(
            ["-m", "multiline", "-f", "DEVICE,TYPE,STATE,CONNECTION", "device", "status"],
            action="list NetworkManager devices",
        )
        rows = parse_nmcli_multiline(output)
        return [
            NetworkDevice(
                device=item.get("DEVICE", ""),
                type=item.get("TYPE", ""),
                state=item.get("STATE", ""),
                connection=item.get("CONNECTION", ""),
            )
            for item in rows
        ]

    def _visible_wifi_networks(self) -> list[WifiNetwork]:
        output = self._run(
            ["-m", "multiline", "-f", "IN-USE,SSID,SIGNAL,SECURITY,CHAN,RATE", "device", "wifi", "list"],
            action="list visible Wi-Fi networks",
        )
        rows = parse_nmcli_multiline(output)
        deduplicated: dict[tuple[str, str, str], WifiNetwork] = {}
        for item in rows:
            ssid = item.get("SSID", "").strip()
            if not ssid or ssid == "--":
                continue
            network = WifiNetwork(
                in_use=item.get("IN-USE", "").strip(),
                ssid=ssid,
                signal=item.get("SIGNAL", "").strip(),
                security=item.get("SECURITY", "").strip(),
                channel=item.get("CHAN", "").strip(),
                rate=item.get("RATE", "").strip(),
            )
            dedupe_key = (network.ssid, network.security, network.channel)
            existing = deduplicated.get(dedupe_key)
            if existing is None:
                deduplicated[dedupe_key] = network
                continue
            existing_signal = _safe_signal_value(existing.signal)
            incoming_signal = _safe_signal_value(network.signal)
            existing_in_use = existing.in_use == "*"
            incoming_in_use = network.in_use == "*"
            if incoming_in_use and not existing_in_use:
                deduplicated[dedupe_key] = network
                continue
            if incoming_in_use == existing_in_use and incoming_signal > existing_signal:
                deduplicated[dedupe_key] = network
        networks = list(deduplicated.values())
        networks.sort(
            key=lambda row: (
                1 if row.in_use == "*" else 0,
                _safe_signal_value(row.signal),
                row.ssid.lower(),
                row.channel,
            ),
            reverse=True,
        )
        return networks[:64]

    def _connection_names(self) -> set[str]:
        output = self._run(
            ["-t", "-f", "NAME", "connection", "show"],
            action="list NetworkManager connection profiles",
        )
        names = {line.strip() for line in output.splitlines() if line.strip()}
        return names

    def get_ap_profile(self, profile_name: str = DEFAULT_AP_PROFILE_NAME) -> AccessPointProfile:
        exists = profile_name in self._connection_names()
        if not exists:
            return AccessPointProfile(
                profile_name=profile_name,
                exists=False,
                active=False,
                ssid="",
                autoconnect="",
                mode="",
                key_mgmt="",
            )

        output = self._run(
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
                profile_name,
            ],
            action=f"load AP profile {profile_name}",
        )
        records = parse_nmcli_multiline(output)
        row = records[0] if records else {}
        general_state = row.get("GENERAL.STATE", "").lower()
        return AccessPointProfile(
            profile_name=profile_name,
            exists=True,
            active=("activated" in general_state),
            ssid=row.get("802-11-wireless.ssid", ""),
            autoconnect=row.get("connection.autoconnect", ""),
            mode=row.get("802-11-wireless.mode", ""),
            key_mgmt=row.get("802-11-wireless-security.key-mgmt", ""),
        )

    def ensure_ap_profile(self, *, profile_name: str, ssid: str, password: str) -> dict[str, Any]:
        action = f"ensure AP profile {profile_name}"
        names = self._connection_names()
        created = False
        if profile_name not in names:
            self._run(
                [
                    "connection",
                    "add",
                    "type",
                    "wifi",
                    "ifname",
                    "*",
                    "con-name",
                    profile_name,
                    "ssid",
                    ssid,
                ],
                action=action,
            )
            created = True

        self._run(
            [
                "connection",
                "modify",
                profile_name,
                "802-11-wireless.mode",
                "ap",
                "802-11-wireless.band",
                "bg",
                "connection.autoconnect",
                "yes",
                "802-11-wireless-security.key-mgmt",
                "wpa-psk",
                "802-11-wireless-security.psk",
                password,
                "ipv4.method",
                "shared",
                "ipv6.method",
                "ignore",
            ],
            action=action,
        )
        # Keep SSID reconciliation explicit and deterministic.
        self._run(["connection", "modify", profile_name, "802-11-wireless.ssid", ssid], action=action)

        profile = self.get_ap_profile(profile_name)
        return {
            "created": created,
            "profile_name": profile_name,
            "ap_profile": asdict(profile),
        }

    def trigger_wifi_scan(self) -> None:
        self._run(["device", "wifi", "rescan"], action="trigger Wi-Fi scan")

    def connect_sta_network(
        self,
        *,
        ssid: str,
        password: str | None,
        ap_profile_name: str = DEFAULT_AP_PROFILE_NAME,
    ) -> dict[str, Any]:
        normalized_ssid = ssid.strip()
        if not normalized_ssid:
            raise NetworkManagerError(
                OperatorError(
                    code="NM_STA_INVALID_INPUT",
                    message="Failed to connect upstream Wi-Fi: SSID is required.",
                    detail="empty SSID",
                    suggestion="Provide a non-empty SSID and retry.",
                )
            )

        wifi_devices = [
            device
            for device in self._network_devices()
            if device.type == "wifi" and device.device
        ]
        non_ap_devices = [
            device
            for device in wifi_devices
            if device.connection != ap_profile_name and device.state.lower() != "unavailable"
        ]
        if not non_ap_devices:
            raise NetworkManagerError(
                OperatorError(
                    code="NM_STA_DEVICE_UNAVAILABLE",
                    message=(
                        "Failed to connect upstream Wi-Fi: no non-AP Wi-Fi device is available for STA mode."
                    ),
                    detail=(
                        "All Wi-Fi devices are currently bound to AP or unavailable; "
                        "keeping AP active to preserve local appliance reachability."
                    ),
                    suggestion="Attach a second Wi-Fi adapter or free one device from AP usage, then retry.",
                )
            )

        target_device = non_ap_devices[0].device
        command = ["device", "wifi", "connect", normalized_ssid, "ifname", target_device]
        if password is not None and password.strip():
            command.extend(["password", password])
        self._run(command, action=f"connect upstream Wi-Fi SSID {normalized_ssid}")
        snapshot = self.status_snapshot(ap_profile_name)
        return {
            "ssid": normalized_ssid,
            "target_device": target_device,
            "snapshot": snapshot.to_dict(),
        }

    def status_snapshot(self, profile_name: str = DEFAULT_AP_PROFILE_NAME) -> NetworkStatusSnapshot:
        general = self._network_general()
        devices = self._network_devices()
        wifi_networks = self._visible_wifi_networks()
        ap_profile = self.get_ap_profile(profile_name)

        connected_wifi_devices = [device for device in devices if device.type == "wifi" and device.device]
        ap_device_names = [
            device.device
            for device in connected_wifi_devices
            if device.state.lower() == "connected" and device.connection == profile_name
        ]
        sta_devices = [
            device
            for device in connected_wifi_devices
            if device.state.lower() == "connected" and device.connection != profile_name
        ]
        sta_connected = len(sta_devices) > 0
        sta_device_names = [device.device for device in sta_devices]
        sta_connection_names: list[str] = []
        for device in sta_devices:
            if (
                device.connection
                and device.connection != "--"
                and device.connection not in sta_connection_names
            ):
                sta_connection_names.append(device.connection)

        local_ap_ready = ap_profile.exists and ap_profile.active and len(ap_device_names) > 0
        connectivity = general.connectivity.lower()
        upstream_internet_reachable = sta_connected and connectivity == "full"
        captive_portal_detected = sta_connected and connectivity in {"portal", "limited"}
        if not ap_profile.exists:
            next_action = "AP profile missing. Update AP config to apply baseline."
        elif general.wifi.lower() == "disabled":
            next_action = "Wi-Fi is disabled in NetworkManager. Enable Wi-Fi to restore AP/scan behavior."
        elif not local_ap_ready:
            next_action = "Local AP is not active. Re-apply AP config and verify AP-capable Wi-Fi hardware."
        elif upstream_internet_reachable:
            next_action = "Local AP is available and upstream Internet is reachable."
        elif captive_portal_detected:
            next_action = (
                "Local AP is available. Upstream Wi-Fi is linked but Internet is limited; "
                "complete captive portal login if required."
            )
        else:
            next_action = "Local AP is available, but upstream Wi-Fi is not connected. Join upstream Wi-Fi."

        return NetworkStatusSnapshot(
            general=general,
            devices=devices,
            wifi_networks=wifi_networks,
            ap_profile=ap_profile,
            sta_connected=sta_connected,
            ap_device_names=ap_device_names,
            sta_device_names=sta_device_names,
            sta_connection_names=sta_connection_names,
            local_ap_ready=local_ap_ready,
            upstream_internet_reachable=upstream_internet_reachable,
            captive_portal_detected=captive_portal_detected,
            next_operator_action=next_action,
        )


def _safe_signal_value(signal: str) -> int:
    raw = signal.strip()
    if not raw:
        return -1
    try:
        return int(raw)
    except ValueError:
        return -1
