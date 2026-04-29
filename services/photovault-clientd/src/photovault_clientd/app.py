"""Local control-plane API exposed by photovault-clientd."""

import asyncio
import logging
import os
import secrets
import socket
import threading
import time
import traceback
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from photovault_clientd.app_context import AppContext
from photovault_clientd.block_devices import BlockDeviceAdapter
from photovault_clientd.db import (
    LATEST_SCHEMA_VERSION,
    append_daemon_event,
    bootstrap_recovery,
    consume_bootstrap_queue,
    get_daemon_state_safe,
    get_schema_version,
    run_state_invariant_checks,
    set_daemon_state,
    set_network_ap_apply_result,
    transition_daemon_state,
)
from photovault_clientd.engine import (
    DEFAULT_AUTO_PROGRESS_MAX_STEPS,
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_RETAIN_STAGED_FILES,
    DEFAULT_SERVER_BASE_URL,
    run_auto_progress_dispatch,
    run_recovery_dispatch,
)
from photovault_clientd.events import EventCategory, EventLevel
from photovault_clientd.m0_checks import run_m0_foundation_checks
from photovault_clientd.networking import NetworkManagerAdapter, NetworkManagerError
from photovault_clientd.routes_ingest import register_ingest_routes
from photovault_clientd.routes_network import register_network_routes
from photovault_clientd.state_machine import ClientState

DEFAULT_DB_PATH = Path("/var/lib/photovault-clientd/state.sqlite3")
DEFAULT_STAGING_ROOT = Path("/var/lib/photovault-clientd/staging")
DEFAULT_AUTO_PROGRESS_INTERVAL_SECONDS = 2.0
DEFAULT_CLIENT_ID = socket.gethostname().strip() or "photovault-client"
DEFAULT_CLIENT_DISPLAY_NAME = DEFAULT_CLIENT_ID
APP_LOGGER = logging.getLogger("photovault-clientd.app")


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

    context = AppContext(
        db_path=db_path,
        staging_root=staging_root,
        resolved_server_base_url=resolved_server_base_url,
        resolved_client_id=resolved_client_id,
        resolved_client_display_name=resolved_client_display_name,
        resolved_bootstrap_token=resolved_bootstrap_token,
        retain_staged_files=retain_staged_files,
        auto_progress_max_steps=auto_progress_max_steps,
        resolved_heartbeat_interval_seconds=resolved_heartbeat_interval_seconds,
        progression_lock=threading.Lock(),
        resolved_network_manager=network_manager or NetworkManagerAdapter(),
        resolved_block_device_adapter=block_device_adapter or BlockDeviceAdapter(),
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        conn = context.open_db()
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
            context.restore_portal_handoff_if_active(conn, now)
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
                context.staging_root,
                server_base_url=context.resolved_server_base_url,
                client_id=context.resolved_client_id,
                client_display_name=context.resolved_client_display_name,
                bootstrap_token=context.resolved_bootstrap_token,
                retain_staged_files=context.retain_staged_files,
                heartbeat_interval_seconds=context.resolved_heartbeat_interval_seconds,
            )
            try:
                ensure_result = context.ensure_ap_baseline(conn, now)
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
                if not context.progression_lock.acquire(blocking=False):
                    continue
                conn_loop = context.open_db()
                cycle_now = datetime.now(UTC).isoformat()
                try:
                    outcome = run_auto_progress_dispatch(
                        conn_loop,
                        context.staging_root,
                        server_base_url=context.resolved_server_base_url,
                        client_id=context.resolved_client_id,
                        client_display_name=context.resolved_client_display_name,
                        bootstrap_token=context.resolved_bootstrap_token,
                        retain_staged_files=context.retain_staged_files,
                        heartbeat_interval_seconds=context.resolved_heartbeat_interval_seconds,
                        max_steps=context.auto_progress_max_steps,
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
                    context.progression_lock.release()

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

    def _extract_error_message(detail: object) -> str:
        if isinstance(detail, str):
            return detail
        if isinstance(detail, dict):
            message = detail.get("message")
            if isinstance(message, str) and message.strip():
                return message
        return str(detail)

    def _request_id_from_request(request: Request) -> str:
        request_id = getattr(request.state, "request_id", None)
        if isinstance(request_id, str) and request_id.strip():
            return request_id
        return secrets.token_hex(8)

    @app.middleware("http")
    async def log_http_requests(request: Request, call_next):
        request_id = secrets.token_hex(8)
        request.state.request_id = request_id
        started_at_utc = datetime.now(UTC).isoformat()
        started_monotonic = time.perf_counter()
        method = request.method
        path = request.url.path
        try:
            response = await call_next(request)
        except Exception as exc:
            duration_ms = (time.perf_counter() - started_monotonic) * 1000.0
            APP_LOGGER.exception(
                "request timestamp=%s method=%s path=%s status_code=%s duration_ms=%.2f request_id=%s",
                started_at_utc,
                method,
                path,
                500,
                duration_ms,
                request_id,
                exc_info=exc,
            )
            raise
        duration_ms = (time.perf_counter() - started_monotonic) * 1000.0
        APP_LOGGER.info(
            "request timestamp=%s method=%s path=%s status_code=%s duration_ms=%.2f request_id=%s",
            started_at_utc,
            method,
            path,
            response.status_code,
            duration_ms,
            request_id,
        )
        response.headers["x-request-id"] = request_id
        return response

    @app.exception_handler(HTTPException)
    async def handle_http_exception(request: Request, exc: HTTPException):
        request_id = _request_id_from_request(request)
        headers = dict(exc.headers) if exc.headers is not None else {}
        headers["x-request-id"] = request_id
        if exc.status_code < 500:
            return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail}, headers=headers)

        timestamp_utc = datetime.now(UTC).isoformat()
        traceback_lines = traceback.format_exception(type(exc), exc, exc.__traceback__)
        enriched_detail: dict[str, object] = {
            "request_id": request_id,
            "timestamp_utc": timestamp_utc,
            "message": _extract_error_message(exc.detail),
            "traceback": traceback_lines,
        }
        if isinstance(exc.detail, dict):
            enriched_detail = {**exc.detail, **enriched_detail}
        else:
            enriched_detail["error_detail"] = exc.detail

        APP_LOGGER.error(
            "http_5xx timestamp=%s method=%s path=%s status_code=%s request_id=%s message=%s",
            timestamp_utc,
            request.method,
            request.url.path,
            exc.status_code,
            request_id,
            enriched_detail["message"],
            exc_info=exc,
        )
        return JSONResponse(status_code=exc.status_code, content={"detail": enriched_detail}, headers=headers)

    @app.exception_handler(Exception)
    async def handle_unexpected_exception(request: Request, exc: Exception):
        request_id = _request_id_from_request(request)
        timestamp_utc = datetime.now(UTC).isoformat()
        traceback_lines = traceback.format_exception(type(exc), exc, exc.__traceback__)
        detail = {
            "request_id": request_id,
            "timestamp_utc": timestamp_utc,
            "message": str(exc) or exc.__class__.__name__,
            "traceback": traceback_lines,
            "exception_type": exc.__class__.__name__,
        }
        APP_LOGGER.exception(
            "unhandled_exception timestamp=%s method=%s path=%s status_code=%s request_id=%s message=%s",
            timestamp_utc,
            request.method,
            request.url.path,
            500,
            request_id,
            detail["message"],
            exc_info=exc,
        )
        return JSONResponse(status_code=500, content={"detail": detail}, headers={"x-request-id": request_id})

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/schema")
    def schema() -> dict[str, int]:
        conn = context.open_db()
        version = get_schema_version(conn)
        conn.close()
        return {"schema_version": version, "latest_schema_version": LATEST_SCHEMA_VERSION}

    @app.get("/diagnostics/invariants")
    def diagnostics_invariants() -> dict[str, object]:
        conn = context.open_db()
        issues = run_state_invariant_checks(conn)
        conn.close()
        return {"ok": len(issues) == 0, "issue_count": len(issues), "issues": issues}

    @app.get("/diagnostics/m0")
    def diagnostics_m0() -> dict[str, object]:
        conn = context.open_db()
        checks = run_m0_foundation_checks(conn)
        conn.close()
        checks["ok"] = (
            checks["resume_map_complete"] and checks["resume_map_terminal_clean"] and checks["invariants_ok"]
        )
        return checks

    register_network_routes(app, context)
    register_ingest_routes(app, context, APP_LOGGER)
    return app
