"""Scoped block-device operations for manual operator-controlled media mounting."""

from __future__ import annotations

import json
import re
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

DEFAULT_BLOCKDEV_HELPER = "/usr/local/sbin/photovault-blockdev-helper"
_DISK_PATH_RE = re.compile(r"^/dev/sd[a-z]+$")
_PARTITION_PATH_RE = re.compile(r"^/dev/sd[a-z]+[0-9]+$")


@dataclass(frozen=True)
class OperatorError:
    code: str
    message: str
    detail: str
    suggestion: str


class BlockDeviceError(RuntimeError):
    def __init__(self, operator_error: OperatorError) -> None:
        super().__init__(operator_error.message)
        self.operator_error = operator_error

    def to_payload(self) -> dict[str, str]:
        return asdict(self.operator_error)


def derive_mount_path(device_path: str) -> str:
    normalized = device_path.strip()
    if not _PARTITION_PATH_RE.fullmatch(normalized):
        raise ValueError(f"unsupported partition device path: {device_path}")
    suffix = normalized.removeprefix("/dev/sd")
    disk = suffix[0]
    partnum = suffix[1:]
    return f"/mnt/sd{disk}_{partnum}"


def _normalize_mountpoints(node: dict[str, Any]) -> list[str]:
    results: list[str] = []
    mountpoint = str(node.get("mountpoint") or "").strip()
    if mountpoint:
        results.append(mountpoint)
    mountpoints = node.get("mountpoints")
    if isinstance(mountpoints, list):
        for item in mountpoints:
            value = str(item or "").strip()
            if value:
                results.append(value)
    elif isinstance(mountpoints, str):
        for raw in mountpoints.splitlines():
            value = raw.strip()
            if value:
                results.append(value)
    deduped: list[str] = []
    seen: set[str] = set()
    for mount in results:
        if mount in seen:
            continue
        seen.add(mount)
        deduped.append(mount)
    return deduped


def _normalize_failure(action: str, exc: Exception) -> BlockDeviceError:
    if isinstance(exc, FileNotFoundError):
        return BlockDeviceError(
            OperatorError(
                code="BLOCK_DEVICE_HELPER_MISSING",
                message=f"Failed to {action}: block-device helper is not installed.",
                detail=str(exc),
                suggestion="Install the photovault block-device helper and retry.",
            )
        )
    if isinstance(exc, subprocess.CalledProcessError):
        stderr = (exc.stderr or "").strip()
        stdout = (exc.stdout or "").strip()
        detail = stderr or stdout or f"command exited with status {exc.returncode}"
        lowered = detail.lower()
        if "not allowed" in lowered or "permission denied" in lowered or "sudo" in lowered:
            return BlockDeviceError(
                OperatorError(
                    code="BLOCK_DEVICE_PERMISSION_DENIED",
                    message=f"Failed to {action}: photovault service user is not authorized.",
                    detail=detail,
                    suggestion=(
                        "Configure sudo helper permissions for photovault block-device operations "
                        "and retry."
                    ),
                )
            )
        return BlockDeviceError(
            OperatorError(
                code="BLOCK_DEVICE_COMMAND_FAILED",
                message=f"Failed to {action}: block-device command failed.",
                detail=detail,
                suggestion="Inspect daemon logs and helper output, then retry.",
            )
        )
    if isinstance(exc, json.JSONDecodeError):
        return BlockDeviceError(
            OperatorError(
                code="BLOCK_DEVICE_PARSE_ERROR",
                message=f"Failed to {action}: helper returned invalid JSON.",
                detail=str(exc),
                suggestion="Inspect helper output and parser assumptions.",
            )
        )
    if isinstance(exc, ValueError):
        return BlockDeviceError(
            OperatorError(
                code="BLOCK_DEVICE_INVALID_INPUT",
                message=f"Failed to {action}: invalid block-device request.",
                detail=str(exc),
                suggestion="Use a supported external partition path such as /dev/sda1.",
            )
        )
    return BlockDeviceError(
        OperatorError(
            code="BLOCK_DEVICE_COMMAND_FAILED",
            message=f"Failed to {action}: unexpected block-device error.",
            detail=str(exc),
            suggestion="Inspect daemon logs and retry.",
        )
    )


class BlockDeviceAdapter:
    def __init__(
        self,
        command_runner: Callable[[list[str]], str] | None = None,
        helper_path: str = DEFAULT_BLOCKDEV_HELPER,
    ) -> None:
        self._helper_path = helper_path
        if command_runner is None:
            self._command_runner = self._run_helper
        else:
            self._command_runner = command_runner

    def _run_helper(self, args: list[str]) -> str:
        completed = subprocess.run(
            ["sudo", "-n", self._helper_path, *args],
            check=True,
            capture_output=True,
            text=True,
        )
        return completed.stdout

    def _run_json(self, args: list[str], *, action: str) -> dict[str, Any]:
        try:
            output = self._command_runner(args)
            payload = json.loads(output)
        except Exception as exc:  # pragma: no cover - normalization covered in tests
            raise _normalize_failure(action, exc) from exc
        if not isinstance(payload, dict):
            raise BlockDeviceError(
                OperatorError(
                    code="BLOCK_DEVICE_PARSE_ERROR",
                    message=f"Failed to {action}: helper returned unexpected payload shape.",
                    detail=f"payload type={type(payload).__name__}",
                    suggestion="Inspect helper output and parser assumptions.",
                )
            )
        return payload

    def list_external_devices(self) -> list[dict[str, Any]]:
        payload = self._run_json(["list"], action="list block devices")
        block_devices = payload.get("blockdevices")
        if not isinstance(block_devices, list):
            return []

        external_disks: list[dict[str, Any]] = []
        for disk in block_devices:
            if not isinstance(disk, dict):
                continue
            path = str(disk.get("path") or "").strip()
            if not _DISK_PATH_RE.fullmatch(path):
                continue
            if str(disk.get("type") or "") != "disk":
                continue
            removable = str(disk.get("rm") or "0") == "1"
            transport = str(disk.get("tran") or "").strip().lower()
            if not removable and transport != "usb":
                continue

            partitions: list[dict[str, Any]] = []
            children = disk.get("children")
            if isinstance(children, list):
                for child in children:
                    if not isinstance(child, dict):
                        continue
                    part_path = str(child.get("path") or "").strip()
                    if str(child.get("type") or "") != "part":
                        continue
                    if not _PARTITION_PATH_RE.fullmatch(part_path):
                        continue
                    target_mount_path = derive_mount_path(part_path)
                    mountpoints = _normalize_mountpoints(child)
                    mount_active = target_mount_path in mountpoints
                    partitions.append(
                        {
                            "name": str(child.get("name") or ""),
                            "path": part_path,
                            "size_bytes": int(child.get("size") or 0),
                            "filesystem_type": str(child.get("fstype") or "").strip() or None,
                            "filesystem_label": str(child.get("label") or "").strip() or None,
                            "filesystem_uuid": str(child.get("uuid") or "").strip() or None,
                            "current_mountpoints": mountpoints,
                            "target_mount_path": target_mount_path,
                            "mount_active": mount_active,
                            "can_mount": not mount_active,
                            "can_unmount": mount_active,
                        }
                    )

            external_disks.append(
                {
                    "name": str(disk.get("name") or ""),
                    "path": path,
                    "size_bytes": int(disk.get("size") or 0),
                    "transport": transport or None,
                    "removable": removable,
                    "vendor": str(disk.get("vendor") or "").strip() or None,
                    "model": str(disk.get("model") or "").strip() or None,
                    "partitions": partitions,
                }
            )

        external_disks.sort(key=lambda item: str(item["path"]))
        return external_disks

    def mount_partition(self, device_path: str) -> dict[str, Any]:
        normalized = device_path.strip()
        if not _PARTITION_PATH_RE.fullmatch(normalized):
            raise _normalize_failure("mount block device", ValueError(normalized))
        target_mount_path = derive_mount_path(normalized)
        result = self._run_json(
            ["mount", normalized, target_mount_path],
            action=f"mount block device {normalized}",
        )
        return {
            "device_path": normalized,
            "mount_path": target_mount_path,
            "result": result,
        }

    def unmount_partition(self, device_path: str) -> dict[str, Any]:
        normalized = device_path.strip()
        if not _PARTITION_PATH_RE.fullmatch(normalized):
            raise _normalize_failure("unmount block device", ValueError(normalized))
        target_mount_path = derive_mount_path(normalized)
        result = self._run_json(
            ["unmount", normalized, target_mount_path],
            action=f"unmount block device {normalized}",
        )
        return {
            "device_path": normalized,
            "mount_path": target_mount_path,
            "result": result,
        }


def is_mountpoint_blocked_by_sources(mount_path: str, source_paths: list[str]) -> bool:
    try:
        mount_root = Path(mount_path).resolve()
    except OSError:
        return False
    for source_path in source_paths:
        normalized = source_path.strip()
        if not normalized:
            continue
        try:
            source = Path(normalized).resolve()
        except OSError:
            continue
        if source == mount_root or mount_root in source.parents:
            return True
    return False
