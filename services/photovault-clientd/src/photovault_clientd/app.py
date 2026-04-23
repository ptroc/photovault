"""Local control-plane API exposed by photovault-clientd."""

import asyncio
import os
import socket
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from photovault_clientd.block_devices import (
    BlockDeviceAdapter,
    BlockDeviceError,
    is_mountpoint_blocked_by_sources,
)
from photovault_clientd.block_devices import (
    derive_mount_path as derive_block_device_mount_path,
)
from photovault_clientd.db import (
    LATEST_SCHEMA_VERSION,
    append_daemon_event,
    bootstrap_recovery,
    consume_bootstrap_queue,
    count_hash_pending_files,
    count_pending_copy_files,
    count_staged_files,
    create_ingest_job,
    fetch_ingest_job_detail,
    fetch_network_ap_config,
    fetch_next_copy_candidate,
    fetch_recent_daemon_events,
    fetch_server_auth_state,
    fetch_server_heartbeat_state,
    get_daemon_state,
    get_daemon_state_safe,
    get_schema_version,
    ingest_job_exists,
    insert_discovered_files,
    list_ingest_job_summaries,
    list_non_terminal_source_paths,
    mark_file_copy_retry,
    mark_file_staged,
    open_db,
    run_state_invariant_checks,
    set_daemon_state,
    set_job_status,
    set_network_ap_apply_result,
    transition_daemon_state,
    upsert_network_ap_config,
)
from photovault_clientd.engine import (
    DEFAULT_AUTO_PROGRESS_MAX_STEPS,
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_RETAIN_STAGED_FILES,
    DEFAULT_SERVER_BASE_URL,
    run_auto_progress_dispatch,
    run_daemon_tick,
    run_error_file_requeue,
    run_recovery_dispatch,
)
from photovault_clientd.events import EventCategory, EventLevel, classify_copy_error
from photovault_clientd.ingest_policy import (
    build_disallowed_file_reason,
    enumerate_directory_media_files,
    is_allowed_media_file,
)
from photovault_clientd.m0_checks import run_m0_foundation_checks
from photovault_clientd.networking import (
    DEFAULT_AP_PASSWORD,
    DEFAULT_AP_PROFILE_NAME,
    DEFAULT_AP_SSID,
    NetworkManagerAdapter,
    NetworkManagerError,
)
from photovault_clientd.state_machine import ClientState
from photovault_clientd.storage import build_staged_path, copy_with_fsync

DEFAULT_DB_PATH = Path("/var/lib/photovault-clientd/state.sqlite3")
DEFAULT_STAGING_ROOT = Path("/var/lib/photovault-clientd/staging")
DEFAULT_AUTO_PROGRESS_INTERVAL_SECONDS = 2.0
DEFAULT_CLIENT_ID = socket.gethostname().strip() or "photovault-client"
DEFAULT_CLIENT_DISPLAY_NAME = DEFAULT_CLIENT_ID


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


def _format_path_os_error(exc: OSError) -> str:
    return exc.strerror or exc.__class__.__name__


def _discover_source_files(
    source_paths: list[str],
) -> tuple[list[str], list[dict[str, str]], list[dict[str, str]], int]:
    discovered: list[str] = []
    invalid_sources: list[dict[str, str]] = []
    filtered_sources: list[dict[str, str]] = []
    filtered_count = 0
    for raw_source_path in source_paths:
        source_path = raw_source_path.strip()
        source = Path(source_path)
        if not source.is_absolute():
            invalid_sources.append(
                {
                    "source_path": source_path,
                    "reason": "Source path must be absolute.",
                }
            )
            continue
        try:
            if source.is_file():
                if not is_allowed_media_file(source):
                    invalid_sources.append(
                        {
                            "source_path": source_path,
                            "reason": build_disallowed_file_reason(source),
                        }
                    )
                    continue
                discovered.append(str(source))
                continue
            if source.is_dir():
                directory_result = enumerate_directory_media_files(source)
                if directory_result.filtered_count > 0:
                    filtered_count += directory_result.filtered_count
                    filtered_sources.extend(directory_result.to_examples())
                if not directory_result.discovered_files:
                    invalid_sources.append(
                        {
                            "source_path": source_path,
                            "reason": (
                                "Directory does not contain ingestable media files after applying "
                                "the v1 exclusion and extension policy."
                            ),
                        }
                    )
                    continue
                discovered.extend(directory_result.discovered_files)
                continue
            if not source.exists():
                invalid_sources.append(
                    {
                        "source_path": source_path,
                        "reason": "Path does not exist.",
                    }
                )
                continue
            invalid_sources.append(
                {
                    "source_path": source_path,
                    "reason": "Path must be a regular file or directory.",
                }
            )
        except OSError as exc:
            invalid_sources.append(
                {
                    "source_path": source_path,
                    "reason": _format_path_os_error(exc),
                }
            )
    return discovered, invalid_sources, filtered_sources[:10], filtered_count


def _next_state_for_stage_phase(pending_copy: int, hash_pending: int) -> ClientState:
    if pending_copy > 0:
        return ClientState.STAGING_COPY
    if hash_pending > 0:
        return ClientState.HASHING
    return ClientState.IDLE


def _http_status_for_block_device_error(exc: BlockDeviceError) -> int:
    code = exc.operator_error.code
    if code == "BLOCK_DEVICE_INVALID_INPUT":
        return 422
    return 503


def _create_ingest_job_from_sources(
    conn,
    *,
    media_label: str,
    source_paths: list[str],
    now_utc: str,
) -> dict[str, object]:
    current_state = get_daemon_state(conn)
    if current_state != ClientState.IDLE:
        raise HTTPException(status_code=409, detail=f"daemon must be IDLE, got {current_state}")

    discovered_source_paths, invalid_sources, filtered_sources, filtered_count = _discover_source_files(
        source_paths
    )
    if invalid_sources:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "INGEST_SOURCE_PATH_INVALID",
                "message": "One or more source paths could not be used for ingest discovery.",
                "invalid_sources": invalid_sources,
                "suggestion": "Fix the listed paths, then retry ingest creation.",
            },
        )

    transition_daemon_state(conn, ClientState.DISCOVERING, now_utc, reason="ingest job created", commit=False)
    job_id = create_ingest_job(conn, media_label, now_utc)
    discovered_count = insert_discovered_files(conn, job_id, discovered_source_paths, now_utc)
    set_job_status(conn, job_id, ClientState.STAGING_COPY.value, now_utc)
    transition_daemon_state(
        conn,
        ClientState.STAGING_COPY,
        now_utc,
        reason="discovery completed; entering staging copy",
        commit=False,
    )
    return {
        "job_id": job_id,
        "discovered_count": discovered_count,
        "filtered_count": filtered_count,
        "filtered_sources": filtered_sources,
        "state": ClientState.STAGING_COPY.value,
    }


def create_app(
    db_path: Path = DEFAULT_DB_PATH,
    staging_root: Path = DEFAULT_STAGING_ROOT,
    server_base_url: str = DEFAULT_SERVER_BASE_URL,
    client_id: str = DEFAULT_CLIENT_ID,
    client_display_name: str = DEFAULT_CLIENT_DISPLAY_NAME,
    bootstrap_token: str | None = None,
    retain_staged_files: bool = DEFAULT_RETAIN_STAGED_FILES,
    auto_progress_interval_seconds: float = DEFAULT_AUTO_PROGRESS_INTERVAL_SECONDS,
    auto_progress_max_steps: int = DEFAULT_AUTO_PROGRESS_MAX_STEPS,
    heartbeat_interval_seconds: int = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    network_manager: NetworkManagerAdapter | None = None,
    block_device_adapter: BlockDeviceAdapter | None = None,
) -> FastAPI:
    resolved_server_base_url = os.getenv("PHOTOVAULT_SERVER_BASE_URL", server_base_url).strip()
    resolved_client_id = os.getenv("PHOTOVAULT_CLIENT_ID", client_id).strip() or DEFAULT_CLIENT_ID
    resolved_client_display_name = (
        os.getenv("PHOTOVAULT_CLIENT_DISPLAY_NAME", client_display_name).strip() or resolved_client_id
    )
    resolved_bootstrap_token = (
        bootstrap_token
        if bootstrap_token is not None
        else os.getenv("PHOTOVAULT_CLIENT_BOOTSTRAP_TOKEN")
    )
    resolved_heartbeat_interval_seconds = max(
        1,
        int(os.getenv("PHOTOVAULT_CLIENT_HEARTBEAT_INTERVAL_SECONDS", str(heartbeat_interval_seconds))),
    )

    resolved_network_manager = network_manager or NetworkManagerAdapter()
    resolved_block_device_adapter = block_device_adapter or BlockDeviceAdapter()
    progression_lock = threading.Lock()

    def _load_or_init_ap_config(conn, now_utc: str) -> dict[str, object]:
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

    def _ap_config_view(conn) -> dict[str, object]:
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

    def _ensure_ap_baseline(conn, now_utc: str) -> dict[str, object]:
        ap_config = _load_or_init_ap_config(conn, now_utc)
        result = resolved_network_manager.ensure_ap_profile(
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

    def _manual_tick_busy_noop_response() -> dict[str, object]:
        now = datetime.now(UTC).isoformat()
        conn_busy = open_db(db_path)
        try:
            current_state = get_daemon_state_safe(conn_busy)
            append_daemon_event(
                conn_busy,
                level=EventLevel.INFO,
                category=EventCategory.TICK_NOOP,
                message=(
                    "manual daemon tick skipped because another progression cycle is active; "
                    "wait for auto progression and refresh status"
                ),
                created_at_utc=now,
                from_state=current_state,
                to_state=current_state,
            )
            conn_busy.commit()
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
            conn_busy.close()

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        conn = open_db(db_path)
        now = datetime.now(UTC).isoformat()
        try:
            invariant_issues = run_state_invariant_checks(conn)
            if invariant_issues:
                for issue in invariant_issues:
                    append_daemon_event(
                        conn,
                        level=EventLevel.ERROR,
                        category=EventCategory.INVARIANT_VIOLATION,
                        message=issue,
                        created_at_utc=now,
                        from_state=get_daemon_state_safe(conn),
                        to_state=ClientState.ERROR_DAEMON,
                    )
                conn.commit()
                raise RuntimeError("state invariant checks failed at startup")

            transition_daemon_state(conn, ClientState.BOOTSTRAP, now, reason="daemon startup")
            bootstrap_recovery(conn, now)
            resume_state = consume_bootstrap_queue(conn, now)
            transition_daemon_state(
                conn,
                resume_state,
                now,
                reason="bootstrap recovery complete",
            )
            run_recovery_dispatch(
                conn,
                staging_root,
                server_base_url=resolved_server_base_url,
                client_id=resolved_client_id,
                client_display_name=resolved_client_display_name,
                bootstrap_token=resolved_bootstrap_token,
                retain_staged_files=retain_staged_files,
                heartbeat_interval_seconds=resolved_heartbeat_interval_seconds,
            )
            try:
                ensure_result = _ensure_ap_baseline(conn, now)
                append_daemon_event(
                    conn,
                    level=EventLevel.INFO,
                    category="NETWORK_AP_BASELINE_ENSURED",
                    message=(
                        f"startup AP baseline ensured: profile={ensure_result['profile_name']}, "
                        f"created={ensure_result['created']}"
                    ),
                    created_at_utc=now,
                    from_state=get_daemon_state_safe(conn),
                    to_state=get_daemon_state_safe(conn),
                )
                conn.commit()
            except NetworkManagerError as exc:
                set_network_ap_apply_result(
                    conn,
                    last_applied_at_utc=None,
                    last_apply_error=exc.operator_error.message,
                )
                append_daemon_event(
                    conn,
                    level=EventLevel.ERROR,
                    category="NETWORK_AP_BASELINE_FAILED",
                    message=f"{exc.operator_error.message} ({exc.operator_error.code})",
                    created_at_utc=now,
                    from_state=get_daemon_state_safe(conn),
                    to_state=get_daemon_state_safe(conn),
                )
                conn.commit()
        except Exception:
            append_daemon_event(
                conn,
                level=EventLevel.ERROR,
                category=EventCategory.BOOTSTRAP_FAILURE,
                message="daemon bootstrap failed; switching to ERROR_DAEMON",
                created_at_utc=now,
                from_state=get_daemon_state_safe(conn),
                to_state=ClientState.ERROR_DAEMON,
            )
            set_daemon_state(conn, ClientState.ERROR_DAEMON, now)
            conn.close()
            raise
        conn.close()

        stop_event = asyncio.Event()

        async def _run_auto_progress_loop() -> None:
            while not stop_event.is_set():
                await asyncio.sleep(max(auto_progress_interval_seconds, 0.1))
                if not progression_lock.acquire(blocking=False):
                    continue
                conn_loop = open_db(db_path)
                cycle_now = datetime.now(UTC).isoformat()
                try:
                    outcome = run_auto_progress_dispatch(
                        conn_loop,
                        staging_root,
                        server_base_url=resolved_server_base_url,
                        client_id=resolved_client_id,
                        client_display_name=resolved_client_display_name,
                        bootstrap_token=resolved_bootstrap_token,
                        retain_staged_files=retain_staged_files,
                        heartbeat_interval_seconds=resolved_heartbeat_interval_seconds,
                        max_steps=auto_progress_max_steps,
                    )
                    if outcome["progressed_steps"] > 0:
                        append_daemon_event(
                            conn_loop,
                            level=EventLevel.INFO,
                            category=EventCategory.AUTO_PROGRESS_APPLIED,
                            message=(
                                f"auto progression cycle: initial_state={outcome['initial_state']}, "
                                f"final_state={outcome['final_state']}, steps={outcome['steps']}, "
                                f"progressed={outcome['progressed_steps']}, "
                                f"stop_reason={outcome['stop_reason']}, errored={outcome['errored']}"
                            ),
                            created_at_utc=cycle_now,
                            from_state=get_daemon_state_safe(conn_loop),
                            to_state=get_daemon_state_safe(conn_loop),
                        )
                        conn_loop.commit()
                except Exception as exc:
                    append_daemon_event(
                        conn_loop,
                        level=EventLevel.ERROR,
                        category=EventCategory.AUTO_PROGRESS_FAILURE,
                        message=f"auto progression loop failed: {exc}",
                        created_at_utc=cycle_now,
                        from_state=get_daemon_state_safe(conn_loop),
                        to_state=get_daemon_state_safe(conn_loop),
                    )
                    conn_loop.commit()
                finally:
                    conn_loop.close()
                    progression_lock.release()

        auto_progress_task = asyncio.create_task(_run_auto_progress_loop())
        try:
            yield
        finally:
            stop_event.set()
            auto_progress_task.cancel()
            try:
                await auto_progress_task
            except asyncio.CancelledError:
                pass

    app = FastAPI(title="photovault-clientd", version="0.1.0", lifespan=lifespan)

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/schema")
    def schema() -> dict[str, int]:
        conn = open_db(db_path)
        version = get_schema_version(conn)
        conn.close()
        return {"schema_version": version, "latest_schema_version": LATEST_SCHEMA_VERSION}

    @app.get("/diagnostics/invariants")
    def diagnostics_invariants() -> dict[str, object]:
        conn = open_db(db_path)
        issues = run_state_invariant_checks(conn)
        conn.close()
        return {"ok": len(issues) == 0, "issue_count": len(issues), "issues": issues}

    @app.get("/diagnostics/m0")
    def diagnostics_m0() -> dict[str, object]:
        conn = open_db(db_path)
        checks = run_m0_foundation_checks(conn)
        conn.close()
        checks["ok"] = (
            checks["resume_map_complete"] and checks["resume_map_terminal_clean"] and checks["invariants_ok"]
        )
        return checks

    @app.get("/state")
    def daemon_state() -> dict[str, object]:
        conn = open_db(db_path)
        row = conn.execute("SELECT current_state, updated_at_utc FROM daemon_state WHERE id = 1;").fetchone()
        auth_state = fetch_server_auth_state(conn)
        heartbeat_state = fetch_server_heartbeat_state(conn)
        conn.close()
        if row is None:
            return {
                "current_state": ClientState.ERROR_DAEMON.value,
                "updated_at_utc": "",
                "server_auth": auth_state,
                "server_heartbeat": heartbeat_state,
            }
        return {
            "current_state": row[0],
            "updated_at_utc": row[1],
            "server_auth": auth_state,
            "server_heartbeat": heartbeat_state,
        }

    @app.get("/network/status")
    def network_status() -> dict[str, object]:
        conn = open_db(db_path)
        try:
            ap_config = _load_or_init_ap_config(conn, datetime.now(UTC).isoformat())
            snapshot = resolved_network_manager.status_snapshot(str(ap_config["profile_name"]))
            return {
                "snapshot": snapshot.to_dict(),
                "ap_config": _ap_config_view(conn),
            }
        except NetworkManagerError as exc:
            raise HTTPException(status_code=503, detail=exc.to_payload()) from exc
        finally:
            conn.close()

    @app.get("/network/ap-config")
    def network_ap_config() -> dict[str, object]:
        conn = open_db(db_path)
        try:
            _load_or_init_ap_config(conn, datetime.now(UTC).isoformat())
            return _ap_config_view(conn)
        finally:
            conn.close()

    @app.put("/network/ap-config")
    def update_network_ap_config(request: APConfigUpdateRequest) -> dict[str, object]:
        now = datetime.now(UTC).isoformat()
        _validate_ap_config_payload(ssid=request.ssid, password=request.password)
        conn = open_db(db_path)
        try:
            existing = _load_or_init_ap_config(conn, now)
            upsert_network_ap_config(
                conn,
                profile_name=str(existing["profile_name"]),
                ssid=request.ssid.strip(),
                password_plaintext=request.password,
                now_utc=now,
            )
            conn.commit()
            try:
                apply_result = resolved_network_manager.ensure_ap_profile(
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
                    "ap_config": _ap_config_view(conn),
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
        conn = open_db(db_path)
        try:
            _load_or_init_ap_config(conn, datetime.now(UTC).isoformat())
            resolved_network_manager.trigger_wifi_scan()
            return {"triggered": True}
        except NetworkManagerError as exc:
            raise HTTPException(status_code=503, detail=exc.to_payload()) from exc
        finally:
            conn.close()

    @app.post("/network/sta-connect")
    def network_sta_connect(request: STAConnectRequest) -> dict[str, object]:
        now = datetime.now(UTC).isoformat()
        _validate_sta_connect_payload(ssid=request.ssid, password=request.password)
        conn = open_db(db_path)
        try:
            ap_config = _load_or_init_ap_config(conn, now)
            result = resolved_network_manager.connect_sta_network(
                ssid=request.ssid.strip(),
                password=request.password,
                ap_profile_name=str(ap_config["profile_name"]),
            )
            return result
        except NetworkManagerError as exc:
            raise HTTPException(status_code=503, detail=exc.to_payload()) from exc
        finally:
            conn.close()

    @app.post("/network/upstream-recheck")
    def network_upstream_recheck() -> dict[str, object]:
        conn = open_db(db_path)
        try:
            ap_config = _load_or_init_ap_config(conn, datetime.now(UTC).isoformat())
            return resolved_network_manager.recheck_upstream_status(
                str(ap_config["profile_name"])
            )
        except NetworkManagerError as exc:
            raise HTTPException(status_code=503, detail=exc.to_payload()) from exc
        finally:
            conn.close()

    @app.get("/bootstrap/recovery")
    def recovery_queue() -> dict[str, object]:
        conn = open_db(db_path)
        pending_rows = conn.execute(
            """
            SELECT file_id, target_state, enqueued_at_utc
            FROM bootstrap_queue
            WHERE processed_at_utc IS NULL
            ORDER BY file_id ASC;
            """
        ).fetchall()
        processed_count_row = conn.execute(
            "SELECT COUNT(1) FROM bootstrap_queue WHERE processed_at_utc IS NOT NULL;"
        ).fetchone()
        conn.close()
        return {
            "pending_count": len(pending_rows),
            "processed_count": int(processed_count_row[0]) if processed_count_row else 0,
            "items": [
                {"file_id": row[0], "target_state": row[1], "enqueued_at_utc": row[2]}
                for row in pending_rows
            ],
        }

    @app.get("/events")
    def daemon_events(limit: int = Query(default=50, ge=1, le=500)) -> dict[str, object]:
        conn = open_db(db_path)
        events = fetch_recent_daemon_events(conn, limit=limit)
        conn.close()
        return {"count": len(events), "events": events}

    @app.get("/block-devices")
    def block_devices() -> dict[str, object]:
        try:
            devices = resolved_block_device_adapter.list_external_devices()
        except BlockDeviceError as exc:
            raise HTTPException(
                status_code=_http_status_for_block_device_error(exc),
                detail=exc.to_payload(),
            ) from exc
        return {"count": len(devices), "devices": devices}

    @app.post("/block-devices/mount")
    def block_devices_mount(request: BlockDeviceActionRequest) -> dict[str, object]:
        now = datetime.now(UTC).isoformat()
        conn = open_db(db_path)
        try:
            try:
                outcome = resolved_block_device_adapter.mount_partition(request.device_path)
            except BlockDeviceError as exc:
                raise HTTPException(
                    status_code=_http_status_for_block_device_error(exc),
                    detail=exc.to_payload(),
                ) from exc

            append_daemon_event(
                conn,
                level=EventLevel.INFO,
                category="BLOCK_DEVICE_MOUNTED",
                message=(
                    f"operator mounted block device {outcome['device_path']} "
                    f"at {outcome['mount_path']} (read-only)"
                ),
                created_at_utc=now,
                from_state=get_daemon_state_safe(conn),
                to_state=get_daemon_state_safe(conn),
            )
            conn.commit()
            return outcome
        finally:
            conn.close()

    @app.post("/block-devices/unmount")
    def block_devices_unmount(request: BlockDeviceActionRequest) -> dict[str, object]:
        now = datetime.now(UTC).isoformat()
        conn = open_db(db_path)
        try:
            try:
                mount_path = derive_block_device_mount_path(request.device_path)
            except ValueError as exc:
                raise HTTPException(
                    status_code=422,
                    detail={
                        "code": "BLOCK_DEVICE_INVALID_INPUT",
                        "message": f"Invalid partition device path: {request.device_path}",
                        "suggestion": "Use a partition path such as /dev/sda1.",
                        "detail": str(exc),
                    },
                ) from exc

            active_sources = list_non_terminal_source_paths(conn)
            if is_mountpoint_blocked_by_sources(mount_path, active_sources):
                raise HTTPException(
                    status_code=409,
                    detail={
                        "code": "BLOCK_DEVICE_BUSY",
                        "message": (
                            f"Cannot unmount {mount_path}: ingest work still references this source path."
                        ),
                        "suggestion": (
                            "Wait for active ingest work to finish before unmounting this partition."
                        ),
                    },
                )

            try:
                outcome = resolved_block_device_adapter.unmount_partition(request.device_path)
            except BlockDeviceError as exc:
                raise HTTPException(
                    status_code=_http_status_for_block_device_error(exc),
                    detail=exc.to_payload(),
                ) from exc

            append_daemon_event(
                conn,
                level=EventLevel.INFO,
                category="BLOCK_DEVICE_UNMOUNTED",
                message=(
                    "operator unmounted block device "
                    f"{outcome['device_path']} from {outcome['mount_path']}"
                ),
                created_at_utc=now,
                from_state=get_daemon_state_safe(conn),
                to_state=get_daemon_state_safe(conn),
            )
            conn.commit()
            return outcome
        finally:
            conn.close()

    @app.get("/ingest/jobs")
    def ingest_jobs() -> dict[str, object]:
        conn = open_db(db_path)
        jobs = list_ingest_job_summaries(conn)
        conn.close()
        return {"count": len(jobs), "jobs": jobs}

    @app.get("/ingest/jobs/{job_id}")
    def ingest_job_detail(job_id: int) -> dict[str, object]:
        conn = open_db(db_path)
        detail = fetch_ingest_job_detail(conn, job_id)
        conn.close()
        if detail is None:
            raise HTTPException(status_code=404, detail=f"job_id {job_id} not found")
        return detail

    @app.post("/daemon/tick")
    def daemon_tick() -> dict[str, object]:
        if not progression_lock.acquire(blocking=False):
            return _manual_tick_busy_noop_response()

        conn = open_db(db_path)
        try:
            outcome = run_daemon_tick(
                conn,
                staging_root,
                server_base_url=resolved_server_base_url,
                client_id=resolved_client_id,
                client_display_name=resolved_client_display_name,
                bootstrap_token=resolved_bootstrap_token,
                retain_staged_files=retain_staged_files,
                heartbeat_interval_seconds=resolved_heartbeat_interval_seconds,
            )
            return outcome
        finally:
            conn.close()
            progression_lock.release()

    @app.post("/ingest/jobs")
    def create_ingest(request: IngestJobCreateRequest) -> dict[str, object]:
        conn = open_db(db_path)
        now = datetime.now(UTC).isoformat()
        try:
            outcome = _create_ingest_job_from_sources(
                conn,
                media_label=request.media_label,
                source_paths=request.source_paths,
                now_utc=now,
            )
            conn.commit()
            return outcome
        finally:
            conn.close()

    @app.post("/ingest/staging/next")
    def stage_next(request: IngestStageNextRequest) -> dict[str, object]:
        conn = open_db(db_path)
        now = datetime.now(UTC).isoformat()
        current_state = get_daemon_state(conn)
        if current_state != ClientState.STAGING_COPY:
            conn.close()
            raise HTTPException(status_code=409, detail=f"daemon must be STAGING_COPY, got {current_state}")
        if not ingest_job_exists(conn, request.job_id):
            conn.close()
            raise HTTPException(status_code=404, detail=f"job_id {request.job_id} not found")
        row = fetch_next_copy_candidate(conn, request.job_id)
        if row is None:
            pending_copy = count_pending_copy_files(conn, request.job_id)
            staged = count_staged_files(conn, request.job_id)
            hash_pending = count_hash_pending_files(conn, request.job_id)
            next_state = _next_state_for_stage_phase(pending_copy, hash_pending)
            set_job_status(conn, request.job_id, next_state.value, now)
            transition_daemon_state(
                conn,
                next_state,
                now,
                reason="staging loop idle tick",
                commit=False,
            )
            conn.commit()
            conn.close()
            return {
                "job_id": request.job_id,
                "copied_file": None,
                "pending_copy": pending_copy,
                "staged": staged,
                "hash_pending": hash_pending,
                "next_state": next_state.value,
            }

        file_id, source_path = row
        staged_path = build_staged_path(Path(request.staging_root), request.job_id, file_id, source_path)
        try:
            copied_size = copy_with_fsync(source_path, staged_path)
            mark_file_staged(conn, file_id, str(staged_path), copied_size, now)
            pending_copy = count_pending_copy_files(conn, request.job_id)
            staged = count_staged_files(conn, request.job_id)
            hash_pending = count_hash_pending_files(conn, request.job_id)
            next_state = _next_state_for_stage_phase(pending_copy, hash_pending)
            set_job_status(conn, request.job_id, next_state.value, now)
            transition_daemon_state(
                conn,
                next_state,
                now,
                reason=f"file copied to staging (file_id={file_id})",
                commit=False,
            )
            conn.commit()
        except OSError as exc:
            mark_file_copy_retry(conn, file_id, str(exc), now)
            append_daemon_event(
                conn,
                level=EventLevel.ERROR,
                category=EventCategory.COPY_RETRY_SCHEDULED,
                message=f"{classify_copy_error(exc).value}: file_id={file_id}, error={exc}",
                created_at_utc=now,
                from_state=ClientState.STAGING_COPY,
                to_state=ClientState.STAGING_COPY,
            )
            pending_copy = count_pending_copy_files(conn, request.job_id)
            staged = count_staged_files(conn, request.job_id)
            hash_pending = count_hash_pending_files(conn, request.job_id)
            next_state = _next_state_for_stage_phase(pending_copy, hash_pending)
            set_job_status(conn, request.job_id, next_state.value, now)
            transition_daemon_state(
                conn,
                next_state,
                now,
                reason=f"copy failed; retry scheduled (file_id={file_id})",
                commit=False,
            )
            conn.commit()
            conn.close()
            return {
                "job_id": request.job_id,
                "copied_file": None,
                "error": str(exc),
                "retry_scheduled": True,
                "pending_copy": pending_copy,
                "staged": staged,
                "hash_pending": hash_pending,
                "next_state": next_state.value,
            }

        conn.close()
        return {
            "job_id": request.job_id,
            "copied_file": {
                "file_id": file_id,
                "source_path": source_path,
                "staged_path": str(staged_path),
                "size_bytes": copied_size,
            },
            "pending_copy": pending_copy,
            "staged": staged,
            "hash_pending": hash_pending,
            "next_state": next_state.value,
        }

    @app.post("/ingest/files/{file_id}/retry-upload")
    def retry_error_file_upload(file_id: int) -> dict[str, object]:
        conn = open_db(db_path)
        current_state = get_daemon_state(conn)
        if current_state != ClientState.ERROR_FILE:
            conn.close()
            raise HTTPException(
                status_code=409,
                detail=f"daemon must be ERROR_FILE for upload requeue, got {current_state}",
            )

        outcome = run_error_file_requeue(conn, file_id=file_id)
        conn.close()
        if not outcome.get("handled"):
            raise HTTPException(status_code=404, detail=f"file_id {file_id} not in ERROR_FILE")
        return outcome

    return app
