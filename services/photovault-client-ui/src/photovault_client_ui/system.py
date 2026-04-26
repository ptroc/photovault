"""OS/System dependency helpers (NetworkManager, systemd)."""
import json
import os
import sqlite3
import subprocess
from collections.abc import Callable
from typing import Any

from .constants import (
    DEFAULT_CLIENT_DB_PATH,
    DEFAULT_DAEMON_BASE_URL,
    DEFAULT_SERVER_API_URL,
    DEFAULT_STAGING_ROOT,
)


def _run_command(args: list[str]) -> str:
    completed = subprocess.run(
        args,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout


def _parse_nmcli_multiline(output: str) -> list[dict[str, str]]:
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


def _get_network_snapshot(command_runner: Callable[[list[str]], str] = _run_command) -> dict[str, Any]:
    general_output = command_runner(["nmcli", "-m", "multiline", "-f", "STATE,CONNECTIVITY,WIFI", "general"])
    devices_output = command_runner(
        ["nmcli", "-m", "multiline", "-f", "DEVICE,TYPE,STATE,CONNECTION", "device", "status"]
    )
    wifi_output = command_runner(
        ["nmcli", "-m", "multiline", "-f", "IN-USE,SSID,SIGNAL,SECURITY,CHAN,RATE", "device", "wifi", "list"]
    )

    general_records = _parse_nmcli_multiline(general_output)
    general = general_records[0] if general_records else {}
    devices = _parse_nmcli_multiline(devices_output)
    wifi_networks = _parse_nmcli_multiline(wifi_output)

    return {
        "general": {
            "state": general.get("STATE", "unknown"),
            "connectivity": general.get("CONNECTIVITY", "unknown"),
            "wifi": general.get("WIFI", "unknown"),
        },
        "devices": [
            {
                "device": item.get("DEVICE", ""),
                "type": item.get("TYPE", ""),
                "state": item.get("STATE", ""),
                "connection": item.get("CONNECTION", ""),
            }
            for item in devices
        ],
        "wifi_networks": [
            {
                "in_use": item.get("IN-USE", ""),
                "ssid": item.get("SSID", ""),
                "signal": item.get("SIGNAL", ""),
                "security": item.get("SECURITY", ""),
                "channel": item.get("CHAN", ""),
                "rate": item.get("RATE", ""),
            }
            for item in wifi_networks
            if item.get("SSID", "")
        ],
    }


def _scan_networks(command_runner: Callable[[list[str]], str] = _run_command) -> None:
    command_runner(["nmcli", "device", "wifi", "rescan"])


def _connect_network(
    ssid: str,
    password: str | None,
    command_runner: Callable[[list[str]], str] = _run_command,
) -> None:
    args = ["nmcli", "device", "wifi", "connect", ssid]
    if password:
        args.extend(["password", password])
    command_runner(args)


def _format_network_error(action: str, exc: subprocess.CalledProcessError | FileNotFoundError) -> str:
    if isinstance(exc, FileNotFoundError):
        return f"Failed to {action}: nmcli is not installed on this device."

    stderr = (exc.stderr or "").strip()
    stdout = (exc.stdout or "").strip()
    details = stderr or stdout
    if "not authorized" in details.lower():
        return (
            f"Failed to {action}: NetworkManager denied the photovault service user. "
            "This device needs a polkit rule that allows Wi-Fi management."
        )
    if details:
        return f"Failed to {action}: {details}"
    return f"Failed to {action}: nmcli exited with status {exc.returncode}."


def _systemd_service_state(
    service_name: str,
    command_runner: Callable[[list[str]], str] = _run_command,
) -> str:
    try:
        return command_runner(["systemctl", "is-active", service_name]).strip() or "unknown"
    except FileNotFoundError:
        return "systemctl unavailable"
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        stdout = (exc.stdout or "").strip()
        return stderr or stdout or f"exit {exc.returncode}"


def _get_dependency_snapshot(server_api_url: str = DEFAULT_SERVER_API_URL) -> list[dict[str, str]]:
    dependencies: list[dict[str, str]] = []

    if DEFAULT_CLIENT_DB_PATH.exists():
        sqlite_status = "ready"
        sqlite_detail = str(DEFAULT_CLIENT_DB_PATH)
        try:
            with sqlite3.connect(DEFAULT_CLIENT_DB_PATH) as conn:
                conn.execute("SELECT 1;").fetchone()
        except sqlite3.Error as exc:
            sqlite_status = "error"
            sqlite_detail = f"{DEFAULT_CLIENT_DB_PATH}: {exc}"
    else:
        sqlite_status = "missing"
        sqlite_detail = str(DEFAULT_CLIENT_DB_PATH)
    dependencies.append({"name": "SQLite", "status": sqlite_status, "detail": sqlite_detail})

    storage_status = "ready"
    if DEFAULT_STAGING_ROOT.exists():
        storage_detail = str(DEFAULT_STAGING_ROOT)
        if not DEFAULT_STAGING_ROOT.is_dir():
            storage_status = "error"
            storage_detail = f"{DEFAULT_STAGING_ROOT}: not a directory"
    else:
        parent = DEFAULT_STAGING_ROOT.parent
        if parent.exists():
            writable = parent.is_dir() and os.access(parent, os.W_OK)
            storage_status = "provisionable" if writable else "missing"
            storage_detail = f"{DEFAULT_STAGING_ROOT} (parent {parent})"
        else:
            storage_status = "missing"
            storage_detail = f"{DEFAULT_STAGING_ROOT} (parent missing)"
    dependencies.append({"name": "Storage", "status": storage_status, "detail": storage_detail})

    dependencies.append(
        {
            "name": "photovault-clientd.service",
            "status": _systemd_service_state("photovault-clientd.service"),
            "detail": f"local daemon API at {DEFAULT_DAEMON_BASE_URL}",
        }
    )
    dependencies.append(
        {
            "name": "NetworkManager.service",
            "status": _systemd_service_state("NetworkManager.service"),
            "detail": "network connectivity and Wi-Fi control",
        }
    )
    dependencies.append(
        {
            "name": "photovault-api.service",
            "status": _systemd_service_state("photovault-api.service"),
            "detail": f"server upload and verify API at {server_api_url}",
        }
    )

    return dependencies


def _get_interface_addresses(
    command_runner: Callable[[list[str]], str] = _run_command,
) -> list[dict[str, Any]]:
    """Return per-interface IP addresses using `ip -j addr`.

    Falls back to an empty list if the command fails or is unavailable.
    Each entry: {"interface": str, "addresses": [str], "connection": str | None}.
    """
    # -- gather IPs via `ip -j addr` -------------------------------------------
    interfaces: dict[str, list[str]] = {}
    try:
        raw = command_runner(["ip", "-j", "addr"])
        parsed = json.loads(raw)
        for iface_data in parsed:
            ifname = str(iface_data.get("ifname", "")).strip()
            if not ifname or ifname == "lo":
                continue
            addrs: list[str] = []
            for addr_info in iface_data.get("addr_info", []):
                local = str(addr_info.get("local", "")).strip()
                prefix = addr_info.get("prefixlen", "")
                family = str(addr_info.get("family", "")).strip()
                if local and family in {"inet", "inet6"}:
                    addrs.append(f"{local}/{prefix}")
            interfaces[ifname] = addrs
    except (FileNotFoundError, subprocess.CalledProcessError, json.JSONDecodeError, ValueError):
        return []

    # -- overlay the active nmcli connection name per device -------------------
    connection_map: dict[str, str] = {}
    try:
        nmcli_out = command_runner(
            ["nmcli", "-m", "multiline", "-f", "DEVICE,CONNECTION", "device", "status"]
        )
        for record in _parse_nmcli_multiline(nmcli_out):
            dev = str(record.get("DEVICE", "")).strip()
            conn = str(record.get("CONNECTION", "")).strip()
            if dev and conn and conn != "--":
                connection_map[dev] = conn
    except (FileNotFoundError, subprocess.CalledProcessError):
        pass

    return [
        {
            "interface": ifname,
            "addresses": addrs,
            "connection": connection_map.get(ifname),
        }
        for ifname, addrs in interfaces.items()
    ]
