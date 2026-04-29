"""nmcli execution, parsing, and connectivity helpers."""

from __future__ import annotations

import subprocess

from .networking_types import NetworkManagerError, OperatorError


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
        if "802-11-wireless-security.key-mgmt: property is missing" in detail_lower:
            return NetworkManagerError(
                OperatorError(
                    code="NM_WIFI_PROFILE_INVALID",
                    message=f"Failed to {action}: existing Wi-Fi profile is missing security settings.",
                    detail=detail,
                    suggestion="Retry connect with password to let photovault refresh the STA profile.",
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


def _safe_signal_value(signal: str) -> int:
    raw = signal.strip()
    if not raw:
        return -1
    try:
        return int(raw)
    except ValueError:
        return -1


def _classify_upstream_status(*, sta_connected: bool, connectivity: str) -> str:
    normalized = connectivity.strip().lower()
    if not sta_connected:
        return "disconnected"
    if normalized == "full":
        return "internet_reachable"
    if normalized == "portal":
        return "captive_portal_likely"
    return "no_usable_internet"


def _normalize_connectivity_value(raw: str) -> str:
    value = raw.strip().lower()
    if not value:
        return "unknown"
    if "(" in value and ")" in value:
        start = value.rfind("(")
        end = value.find(")", start + 1)
        if start != -1 and end != -1 and end > start + 1:
            return value[start + 1 : end].strip()
    return value


def _select_sta_connectivity(states: list[str], *, fallback: str) -> str:
    if not states:
        return fallback
    if "full" in states:
        return "full"
    if "portal" in states:
        return "portal"
    if "limited" in states:
        return "limited"
    if "none" in states:
        return "none"
    if "unknown" in states:
        return "unknown"
    return states[0]


def _apply_sta_probe_override(*, connectivity: str, probe_state: str) -> str:
    normalized_connectivity = connectivity.strip().lower()
    normalized_probe = probe_state.strip().lower()
    if normalized_probe in {"portal", "limited", "none"}:
        return normalized_probe
    if normalized_connectivity == "unknown" and normalized_probe in {"full"}:
        return normalized_probe
    return normalized_connectivity


def _probe_sta_interface_connectivity(device_name: str) -> str:
    probe_target = "http://connectivity-check.ubuntu.com/"
    try:
        completed = subprocess.run(
            [
                "curl",
                "-sS",
                "-I",
                "--max-time",
                "4",
                "--interface",
                device_name,
                probe_target,
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return "unknown"
    except subprocess.CalledProcessError:
        return "unknown"

    status_code = _extract_http_status_code(completed.stdout)
    if status_code in {301, 302, 303, 307, 308, 511}:
        return "portal"
    if status_code in {200, 204}:
        return "full"
    if status_code in {400, 401, 403, 407}:
        return "limited"
    if status_code is None:
        return "unknown"
    return "none"


def _extract_http_status_code(headers_output: str) -> int | None:
    for raw_line in headers_output.splitlines():
        line = raw_line.strip()
        if not line.lower().startswith("http/"):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        status = parts[1].strip()
        if status.isdigit():
            return int(status)
    return None


def _normalize_never_default_value(raw: str) -> str:
    normalized = raw.strip().lower()
    if normalized in {"yes", "true", "1"}:
        return "yes"
    return "no"


def _sta_profile_name(ssid: str) -> str:
    normalized = "".join(
        character if character.isalnum() or character in {"-", "_", "."} else "_"
        for character in ssid.strip()
    )
    trimmed = normalized.strip("._-")
    if not trimmed:
        trimmed = "ssid"
    return f"photovault-sta-{trimmed}"[:96]

