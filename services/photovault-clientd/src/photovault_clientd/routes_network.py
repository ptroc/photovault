"""Network route registration for photovault-clientd."""

import json
from datetime import UTC, datetime

from fastapi import FastAPI, HTTPException

from photovault_clientd.db import (
    set_network_ap_apply_result,
    upsert_network_ap_config,
    upsert_network_portal_handoff_state,
)
from photovault_clientd.networking import NetworkManagerError

from .app_context import APConfigUpdateRequest, AppContext, STAConnectRequest


def _validate_ap_config_payload(*, ssid: str, password: str) -> None:
    normalized_ssid = ssid.strip()
    if not normalized_ssid:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "AP_CONFIG_INVALID",
                "message": "AP SSID is required.",
                "suggestion": "Provide a non-empty SSID and retry AP configuration.",
            },
        )
    if len(password) < 8 or len(password) > 63:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "AP_CONFIG_INVALID",
                "message": "AP password must be between 8 and 63 characters.",
                "suggestion": "Use a WPA-PSK password in the valid range and retry.",
            },
        )


def _validate_sta_connect_payload(*, ssid: str, password: str | None) -> None:
    normalized_ssid = ssid.strip()
    if not normalized_ssid:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "STA_CONNECT_INVALID",
                "message": "Upstream Wi-Fi SSID is required.",
                "suggestion": "Provide a non-empty SSID and retry connecting upstream Wi-Fi.",
            },
        )
    if password is None:
        return
    normalized_password = password.strip()
    if normalized_password and (len(normalized_password) < 8 or len(normalized_password) > 63):
        raise HTTPException(
            status_code=422,
            detail={
                "code": "STA_CONNECT_INVALID",
                "message": "Wi-Fi password must be between 8 and 63 characters when provided.",
                "suggestion": (
                    "Use a valid WPA-PSK password, or leave password empty for open/known networks."
                ),
            },
        )


def register_network_routes(app: FastAPI, context: AppContext) -> None:
    @app.get("/network/status")
    def network_status() -> dict[str, object]:
        conn = context.open_db()
        try:
            now = datetime.now(UTC).isoformat()
            ap_config = context.load_or_init_ap_config(conn, now)
            snapshot = context.resolved_network_manager.status_snapshot(str(ap_config["profile_name"]))
            snapshot_payload = context.snapshot_with_portal_handoff_state(
                conn=conn,
                snapshot_payload=snapshot.to_dict(),
                now_utc=now,
            )
            return {
                "snapshot": snapshot_payload,
                "ap_config": context.ap_config_view(conn),
            }
        except NetworkManagerError as exc:
            raise HTTPException(status_code=503, detail=exc.to_payload()) from exc
        finally:
            conn.close()

    @app.get("/network/ap-config")
    def network_ap_config() -> dict[str, object]:
        conn = context.open_db()
        try:
            context.load_or_init_ap_config(conn, datetime.now(UTC).isoformat())
            return context.ap_config_view(conn)
        finally:
            conn.close()

    @app.put("/network/ap-config")
    def update_network_ap_config(request: APConfigUpdateRequest) -> dict[str, object]:
        now = datetime.now(UTC).isoformat()
        _validate_ap_config_payload(ssid=request.ssid, password=request.password)
        conn = context.open_db()
        try:
            existing = context.load_or_init_ap_config(conn, now)
            upsert_network_ap_config(
                conn,
                profile_name=str(existing["profile_name"]),
                ssid=request.ssid.strip(),
                password_plaintext=request.password,
                now_utc=now,
            )
            conn.commit()
            try:
                apply_result = context.resolved_network_manager.ensure_ap_profile(
                    profile_name=str(existing["profile_name"]),
                    ssid=request.ssid.strip(),
                    password=request.password,
                )
                set_network_ap_apply_result(
                    conn,
                    last_applied_at_utc=now,
                    last_apply_error=None,
                )
                conn.commit()
                return {
                    "ap_config": context.ap_config_view(conn),
                    "apply_result": apply_result,
                }
            except NetworkManagerError as exc:
                set_network_ap_apply_result(
                    conn,
                    last_applied_at_utc=None,
                    last_apply_error=exc.operator_error.message,
                )
                conn.commit()
                raise HTTPException(status_code=503, detail=exc.to_payload()) from exc
        finally:
            conn.close()

    @app.post("/network/wifi-scan")
    def network_wifi_scan() -> dict[str, object]:
        conn = context.open_db()
        try:
            context.load_or_init_ap_config(conn, datetime.now(UTC).isoformat())
            context.resolved_network_manager.trigger_wifi_scan()
            return {"triggered": True}
        except NetworkManagerError as exc:
            raise HTTPException(status_code=503, detail=exc.to_payload()) from exc
        finally:
            conn.close()

    @app.post("/network/sta-connect")
    def network_sta_connect(request: STAConnectRequest) -> dict[str, object]:
        now = datetime.now(UTC).isoformat()
        _validate_sta_connect_payload(ssid=request.ssid, password=request.password)
        conn = context.open_db()
        try:
            ap_config = context.load_or_init_ap_config(conn, now)
            result = context.resolved_network_manager.connect_sta_network(
                ssid=request.ssid.strip(),
                password=request.password,
                ap_profile_name=str(ap_config["profile_name"]),
            )
            result["snapshot"] = context.snapshot_with_portal_handoff_state(
                conn=conn,
                snapshot_payload=dict(result["snapshot"]),
                now_utc=now,
            )
            return result
        except NetworkManagerError as exc:
            raise HTTPException(status_code=503, detail=exc.to_payload()) from exc
        finally:
            conn.close()

    @app.post("/network/upstream-recheck")
    def network_upstream_recheck() -> dict[str, object]:
        now = datetime.now(UTC).isoformat()
        conn = context.open_db()
        try:
            ap_config = context.load_or_init_ap_config(conn, now)
            result = context.resolved_network_manager.recheck_upstream_status(
                str(ap_config["profile_name"])
            )
            result["snapshot"] = context.snapshot_with_portal_handoff_state(
                conn=conn,
                snapshot_payload=dict(result["snapshot"]),
                now_utc=now,
            )
            return result
        except NetworkManagerError as exc:
            raise HTTPException(status_code=503, detail=exc.to_payload()) from exc
        finally:
            conn.close()

    @app.post("/network/portal-handoff/start")
    def network_portal_handoff_start() -> dict[str, object]:
        now = datetime.now(UTC).isoformat()
        conn = context.open_db()
        try:
            ap_config = context.load_or_init_ap_config(conn, now)
            current_state = context.load_or_init_portal_handoff_state(conn, now)
            if bool(current_state["active"]):
                raise HTTPException(
                    status_code=409,
                    detail={
                        "code": "PORTAL_HANDOFF_INVALID_STATE",
                        "message": "Portal handoff is already active.",
                        "suggestion": "Stop portal handoff before starting it again.",
                    },
                )
            result = context.resolved_network_manager.start_portal_handoff(
                ap_profile_name=str(ap_config["profile_name"])
            )
            previous_route_prefs = result.get("previous_eth_route_prefs", [])
            upsert_network_portal_handoff_state(
                conn,
                active=True,
                started_at_utc=now,
                previous_eth_route_prefs_json=json.dumps(previous_route_prefs),
                updated_at_utc=now,
            )
            conn.commit()
            snapshot = context.resolved_network_manager.status_snapshot(str(ap_config["profile_name"]))
            snapshot_payload = context.snapshot_with_portal_handoff_state(
                conn=conn,
                snapshot_payload=snapshot.to_dict(),
                now_utc=now,
            )
            return {
                "started": True,
                "modified_ethernet_connections": result.get("modified_ethernet_connections", []),
                "snapshot": snapshot_payload,
            }
        except NetworkManagerError as exc:
            raise HTTPException(status_code=503, detail=exc.to_payload()) from exc
        finally:
            conn.close()

    @app.post("/network/portal-handoff/stop")
    def network_portal_handoff_stop() -> dict[str, object]:
        now = datetime.now(UTC).isoformat()
        conn = context.open_db()
        try:
            ap_config = context.load_or_init_ap_config(conn, now)
            current_state = context.load_or_init_portal_handoff_state(conn, now)
            if not bool(current_state["active"]):
                raise HTTPException(
                    status_code=409,
                    detail={
                        "code": "PORTAL_HANDOFF_INVALID_STATE",
                        "message": "Portal handoff is not active.",
                        "suggestion": "Start portal handoff first, then stop it when login is complete.",
                    },
                )
            previous_route_prefs = context.parse_portal_handoff_route_prefs(
                current_state["previous_eth_route_prefs_json"]
            )
            result = context.resolved_network_manager.stop_portal_handoff(
                previous_eth_route_prefs=previous_route_prefs
            )
            context.clear_portal_handoff_state(conn, now)
            conn.commit()
            snapshot = context.resolved_network_manager.status_snapshot(str(ap_config["profile_name"]))
            snapshot_payload = context.snapshot_with_portal_handoff_state(
                conn=conn,
                snapshot_payload=snapshot.to_dict(),
                now_utc=now,
            )
            return {
                "stopped": True,
                "restored_ethernet_connections": result.get("restored_ethernet_connections", []),
                "snapshot": snapshot_payload,
            }
        except ValueError as exc:
            raise HTTPException(
                status_code=503,
                detail={
                    "code": "PORTAL_HANDOFF_RESTORE_INVALID",
                    "message": "Failed to stop portal handoff: stored route preference state is invalid.",
                    "detail": str(exc),
                    "suggestion": "Inspect daemon logs and reset portal handoff state before retry.",
                },
            ) from exc
        except NetworkManagerError as exc:
            raise HTTPException(status_code=503, detail=exc.to_payload()) from exc
        finally:
            conn.close()
