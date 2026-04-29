"""Shared types for the clientd NetworkManager adapter."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

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
    upstream_connectivity: str
    upstream_status: str
    upstream_no_usable_internet: bool
    upstream_internet_reachable: bool
    captive_portal_detected: bool
    next_operator_action: str
    portal_handoff_active: bool = False
    portal_handoff_started_at_utc: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class NetworkManagerError(RuntimeError):
    def __init__(self, operator_error: OperatorError) -> None:
        super().__init__(operator_error.message)
        self.operator_error = operator_error

    def to_payload(self) -> dict[str, str]:
        return asdict(self.operator_error)

