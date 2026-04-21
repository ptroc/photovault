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
        return [
            WifiNetwork(
                in_use=item.get("IN-USE", ""),
                ssid=item.get("SSID", ""),
                signal=item.get("SIGNAL", ""),
                security=item.get("SECURITY", ""),
                channel=item.get("CHAN", ""),
                rate=item.get("RATE", ""),
            )
            for item in rows
            if item.get("SSID", "")
        ]

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

    def status_snapshot(self, profile_name: str = DEFAULT_AP_PROFILE_NAME) -> NetworkStatusSnapshot:
        general = self._network_general()
        devices = self._network_devices()
        wifi_networks = self._visible_wifi_networks()
        ap_profile = self.get_ap_profile(profile_name)

        sta_connected = False
        for device in devices:
            if device.type != "wifi":
                continue
            if device.state.lower() == "connected" and device.connection != profile_name:
                sta_connected = True
                break

        connectivity = general.connectivity.lower()
        if not ap_profile.exists:
            next_action = "AP profile missing. Update AP config to apply baseline."
        elif connectivity in {"full", "limited"} and sta_connected:
            next_action = "Upstream network is connected. Verify AP settings and continue operations."
        elif general.wifi.lower() == "disabled":
            next_action = "Wi-Fi is disabled in NetworkManager. Enable Wi-Fi to restore AP/scan behavior."
        else:
            next_action = (
                "No usable upstream network. Join this device AP and configure Wi-Fi from the Network page."
            )

        return NetworkStatusSnapshot(
            general=general,
            devices=devices,
            wifi_networks=wifi_networks,
            ap_profile=ap_profile,
            sta_connected=sta_connected,
            next_operator_action=next_action,
        )
