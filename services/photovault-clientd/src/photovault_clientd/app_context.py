"""Shared application context and request models for photovault-clientd."""

import json
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel, Field

from photovault_clientd.block_devices import BlockDeviceAdapter
from photovault_clientd.db import (
    append_daemon_event,
    fetch_network_ap_config,
    fetch_network_portal_handoff_state,
    get_daemon_state_safe,
    open_db,
    set_network_ap_apply_result,
    upsert_network_ap_config,
    upsert_network_portal_handoff_state,
)
from photovault_clientd.events import EventLevel
from photovault_clientd.networking import (
    DEFAULT_AP_PASSWORD,
    DEFAULT_AP_PROFILE_NAME,
    DEFAULT_AP_SSID,
    NetworkManagerAdapter,
    NetworkManagerError,
)
from photovault_clientd.state_machine import ClientState


class IngestJobCreateRequest(BaseModel):
    media_label: str = Field(min_length=1, max_length=255)
    source_paths: list[str] = Field(min_length=1)


class IngestStageNextRequest(BaseModel):
    job_id: int
    staging_root: str = Field(min_length=1)


class APConfigUpdateRequest(BaseModel):
    ssid: str
    password: str


class STAConnectRequest(BaseModel):
    ssid: str
    password: str | None = None


class BlockDeviceActionRequest(BaseModel):
    device_path: str = Field(min_length=1, max_length=512)


@dataclass(slots=True)
class AppContext:
    db_path: Path
    staging_root: Path
    resolved_server_base_url: str
    resolved_client_id: str
    resolved_client_display_name: str
    resolved_bootstrap_token: str | None
    retain_staged_files: bool
    auto_progress_max_steps: int
    resolved_heartbeat_interval_seconds: int
    progression_lock: threading.Lock
    resolved_network_manager: NetworkManagerAdapter
    resolved_block_device_adapter: BlockDeviceAdapter

    def open_db(self):
        return open_db(self.db_path)

    def load_or_init_ap_config(self, conn, now_utc: str) -> dict[str, object]:
        existing = fetch_network_ap_config(conn)
        if existing is not None:
            return existing
        upsert_network_ap_config(
            conn,
            profile_name=DEFAULT_AP_PROFILE_NAME,
            ssid=DEFAULT_AP_SSID,
            password_plaintext=DEFAULT_AP_PASSWORD,
            now_utc=now_utc,
        )
        conn.commit()
        created = fetch_network_ap_config(conn)
        if created is None:
            raise RuntimeError("failed to initialize network_ap_config singleton row")
        return created

    def ap_config_view(self, conn) -> dict[str, object]:
        row = fetch_network_ap_config(conn)
        if row is None:
            return {
                "profile_name": DEFAULT_AP_PROFILE_NAME,
                "ssid": DEFAULT_AP_SSID,
                "password_set": False,
                "updated_at_utc": "",
                "last_applied_at_utc": None,
                "last_apply_error": "AP config not initialized",
            }
        return {
            "profile_name": str(row["profile_name"]),
            "ssid": str(row["ssid"]),
            "password_set": bool(row["password_plaintext"]),
            "updated_at_utc": str(row["updated_at_utc"]),
            "last_applied_at_utc": row["last_applied_at_utc"],
            "last_apply_error": row["last_apply_error"],
        }

    def load_or_init_portal_handoff_state(self, conn, now_utc: str) -> dict[str, object]:
        existing = fetch_network_portal_handoff_state(conn)
        if existing is not None:
            return existing
        upsert_network_portal_handoff_state(
            conn,
            active=False,
            started_at_utc=None,
            previous_eth_route_prefs_json=json.dumps([]),
            updated_at_utc=now_utc,
        )
        conn.commit()
        created = fetch_network_portal_handoff_state(conn)
        if created is None:
            raise RuntimeError("failed to initialize network_portal_handoff_state singleton row")
        return created

    def parse_portal_handoff_route_prefs(self, raw_json: object) -> list[dict[str, str]]:
        if raw_json is None:
            return []
        if not isinstance(raw_json, str):
            raise ValueError("portal handoff route preferences must be serialized JSON text")
        if not raw_json.strip():
            return []
        payload = json.loads(raw_json)
        if not isinstance(payload, list):
            raise ValueError("portal handoff route preferences must be a JSON list")

        normalized: list[dict[str, str]] = []
        for item in payload:
            if not isinstance(item, dict):
                raise ValueError("portal handoff route preference entries must be JSON objects")
            connection_name = str(item.get("connection_name", "")).strip()
            if not connection_name:
                raise ValueError("portal handoff route preference entry is missing connection_name")
            normalized.append(
                {
                    "connection_name": connection_name,
                    "device_name": str(item.get("device_name", "")).strip(),
                    "ipv4_never_default": str(item.get("ipv4_never_default", "no")).strip().lower()
                    or "no",
                    "ipv6_never_default": str(item.get("ipv6_never_default", "no")).strip().lower()
                    or "no",
                }
            )
        return normalized

    def portal_handoff_state_view(self, conn, now_utc: str) -> dict[str, object]:
        row = self.load_or_init_portal_handoff_state(conn, now_utc)
        return {
            "active": bool(row["active"]),
            "started_at_utc": row["started_at_utc"],
            "previous_eth_route_prefs_json": str(row["previous_eth_route_prefs_json"]),
        }

    def snapshot_with_portal_handoff_state(
        self,
        *,
        conn,
        snapshot_payload: dict[str, object],
        now_utc: str,
    ) -> dict[str, object]:
        handoff_state = self.portal_handoff_state_view(conn, now_utc)
        snapshot_payload["portal_handoff_active"] = bool(handoff_state["active"])
        snapshot_payload["portal_handoff_started_at_utc"] = handoff_state["started_at_utc"]
        return snapshot_payload

    def clear_portal_handoff_state(self, conn, now_utc: str) -> None:
        upsert_network_portal_handoff_state(
            conn,
            active=False,
            started_at_utc=None,
            previous_eth_route_prefs_json=json.dumps([]),
            updated_at_utc=now_utc,
        )

    def restore_portal_handoff_if_active(self, conn, now_utc: str) -> None:
        handoff_state = self.load_or_init_portal_handoff_state(conn, now_utc)
        if not bool(handoff_state["active"]):
            return

        try:
            previous_route_prefs = self.parse_portal_handoff_route_prefs(
                handoff_state["previous_eth_route_prefs_json"]
            )
            restore_result = self.resolved_network_manager.stop_portal_handoff(
                previous_eth_route_prefs=previous_route_prefs
            )
            self.clear_portal_handoff_state(conn, now_utc)
            append_daemon_event(
                conn,
                level=EventLevel.INFO,
                category="NETWORK_PORTAL_HANDOFF_RESTORED_ON_STARTUP",
                message=(
                    "restored portal handoff route preferences on startup for connections: "
                    f"{', '.join(restore_result.get('restored_ethernet_connections', [])) or 'none'}"
                ),
                created_at_utc=now_utc,
                from_state=get_daemon_state_safe(conn),
                to_state=get_daemon_state_safe(conn),
            )
            conn.commit()
        except (NetworkManagerError, ValueError, json.JSONDecodeError) as exc:
            append_daemon_event(
                conn,
                level=EventLevel.ERROR,
                category="NETWORK_PORTAL_HANDOFF_RESTORE_FAILED",
                message=f"failed to restore portal handoff state on startup: {exc}",
                created_at_utc=now_utc,
                from_state=get_daemon_state_safe(conn),
                to_state=ClientState.ERROR_DAEMON,
            )
            conn.commit()
            raise RuntimeError("failed to restore portal handoff state on startup") from exc

    def ensure_ap_baseline(self, conn, now_utc: str) -> dict[str, object]:
        ap_config = self.load_or_init_ap_config(conn, now_utc)
        result = self.resolved_network_manager.ensure_ap_profile(
            profile_name=str(ap_config["profile_name"]),
            ssid=str(ap_config["ssid"]),
            password=str(ap_config["password_plaintext"]),
        )
        set_network_ap_apply_result(
            conn,
            last_applied_at_utc=now_utc,
            last_apply_error=None,
        )
        conn.commit()
        return result

    def manual_tick_busy_noop_response(self) -> dict[str, object]:
        now = datetime.now(UTC).isoformat()
        conn = self.open_db()
        try:
            current_state = get_daemon_state_safe(conn)
            append_daemon_event(
                conn,
                level=EventLevel.INFO,
                category="TICK_NOOP",
                message=(
                    "manual daemon tick skipped because another progression cycle is active; "
                    "wait for auto progression and refresh status"
                ),
                created_at_utc=now,
                from_state=current_state,
                to_state=current_state,
            )
            conn.commit()
            return {
                "handled": True,
                "progressed": False,
                "errored": False,
                "already_progressing": True,
                "no_op": True,
                "state": current_state.value,
                "next_state": current_state.value,
            }
        finally:
            conn.close()
