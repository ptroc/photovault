"""State, ingest, and block-device route registration for photovault-clientd."""

import logging
from datetime import UTC, datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request

from photovault_clientd.block_devices import (
    BlockDeviceError,
    is_mountpoint_blocked_by_sources,
)
from photovault_clientd.block_devices import (
    derive_mount_path as derive_block_device_mount_path,
)
from photovault_clientd.db import (
    append_daemon_event,
    count_hash_pending_files,
    count_pending_copy_files,
    count_staged_files,
    create_ingest_job,
    fetch_ingest_job_detail,
    fetch_next_copy_candidate,
    fetch_recent_daemon_events,
    fetch_server_auth_state,
    fetch_server_heartbeat_state,
    get_daemon_state,
    get_daemon_state_safe,
    ingest_job_exists,
    insert_discovered_files,
    list_ingest_job_summaries,
    list_non_terminal_source_paths,
    mark_file_copy_retry,
    mark_file_staged,
    set_job_status,
    transition_daemon_state,
)
from photovault_clientd.engine import run_daemon_tick, run_error_file_requeue
from photovault_clientd.events import EventCategory, EventLevel, classify_copy_error
from photovault_clientd.ingest_policy import (
    build_disallowed_file_reason,
    enumerate_directory_media_files,
    is_allowed_media_file,
)
from photovault_clientd.state_machine import ClientState
from photovault_clientd.storage import build_staged_path, copy_with_fsync

from .app_context import (
    AppContext,
    BlockDeviceActionRequest,
    IngestJobCreateRequest,
    IngestStageNextRequest,
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
    if exc.operator_error.code == "BLOCK_DEVICE_INVALID_INPUT":
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


def register_ingest_routes(app: FastAPI, context: AppContext, app_logger: logging.Logger) -> None:
    @app.get("/state")
    def daemon_state() -> dict[str, object]:
        conn = context.open_db()
        row = conn.execute("SELECT current_state, updated_at_utc FROM daemon_state WHERE id = 1;").fetchone()
        auth_state = fetch_server_auth_state(conn)
        heartbeat_state = fetch_server_heartbeat_state(conn)
        conn.close()
        if row is None:
            return {
                "current_state": ClientState.ERROR_DAEMON.value,
                "updated_at_utc": "",
                "server_base_url": context.resolved_server_base_url,
                "server_auth": auth_state,
                "server_heartbeat": heartbeat_state,
            }
        return {
            "current_state": row[0],
            "updated_at_utc": row[1],
            "server_base_url": context.resolved_server_base_url,
            "server_auth": auth_state,
            "server_heartbeat": heartbeat_state,
        }

    @app.get("/bootstrap/recovery")
    def recovery_queue() -> dict[str, object]:
        conn = context.open_db()
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
        conn = context.open_db()
        events = fetch_recent_daemon_events(conn, limit=limit)
        conn.close()
        return {"count": len(events), "events": events}

    @app.get("/block-devices")
    def block_devices() -> dict[str, object]:
        try:
            devices = context.resolved_block_device_adapter.list_external_devices()
        except BlockDeviceError as exc:
            raise HTTPException(
                status_code=_http_status_for_block_device_error(exc),
                detail=exc.to_payload(),
            ) from exc
        return {"count": len(devices), "devices": devices}

    @app.post("/block-devices/mount")
    def block_devices_mount(request: BlockDeviceActionRequest) -> dict[str, object]:
        now = datetime.now(UTC).isoformat()
        conn = context.open_db()
        try:
            try:
                outcome = context.resolved_block_device_adapter.mount_partition(request.device_path)
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
        conn = context.open_db()
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
                outcome = context.resolved_block_device_adapter.unmount_partition(request.device_path)
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
        conn = context.open_db()
        jobs = list_ingest_job_summaries(conn)
        conn.close()
        return {"count": len(jobs), "jobs": jobs}

    @app.get("/ingest/jobs/{job_id}")
    def ingest_job_detail(job_id: int) -> dict[str, object]:
        conn = context.open_db()
        detail = fetch_ingest_job_detail(conn, job_id)
        conn.close()
        if detail is None:
            raise HTTPException(status_code=404, detail=f"job_id {job_id} not found")
        return detail

    @app.post("/daemon/tick")
    def daemon_tick() -> dict[str, object]:
        if not context.progression_lock.acquire(blocking=False):
            return context.manual_tick_busy_noop_response()

        conn = context.open_db()
        try:
            return run_daemon_tick(
                conn,
                context.staging_root,
                server_base_url=context.resolved_server_base_url,
                client_id=context.resolved_client_id,
                client_display_name=context.resolved_client_display_name,
                bootstrap_token=context.resolved_bootstrap_token,
                retain_staged_files=context.retain_staged_files,
                heartbeat_interval_seconds=context.resolved_heartbeat_interval_seconds,
            )
        finally:
            conn.close()
            context.progression_lock.release()

    @app.post("/ingest/jobs")
    def create_ingest(payload: IngestJobCreateRequest, http_request: Request) -> dict[str, object]:
        conn = context.open_db()
        now = datetime.now(UTC).isoformat()
        try:
            outcome = _create_ingest_job_from_sources(
                conn,
                media_label=payload.media_label,
                source_paths=payload.source_paths,
                now_utc=now,
            )
            conn.commit()
            app_logger.info(
                (
                    "ingest_job_created timestamp=%s job_id=%s media_label=%s "
                    "discovered_count=%s filtered_count=%s request_id=%s"
                ),
                now,
                outcome.get("job_id"),
                payload.media_label,
                outcome.get("discovered_count"),
                outcome.get("filtered_count"),
                getattr(http_request.state, "request_id", ""),
            )
            return outcome
        finally:
            conn.close()

    @app.post("/ingest/staging/next")
    def stage_next(request: IngestStageNextRequest) -> dict[str, object]:
        conn = context.open_db()
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
        conn = context.open_db()
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
