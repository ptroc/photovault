"""Local control-plane API exposed by photovault-clientd."""

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

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
    fetch_next_copy_candidate,
    fetch_recent_daemon_events,
    get_daemon_state,
    get_daemon_state_safe,
    get_schema_version,
    ingest_job_exists,
    insert_discovered_files,
    list_ingest_job_summaries,
    mark_file_copy_retry,
    mark_file_staged,
    open_db,
    run_state_invariant_checks,
    set_daemon_state,
    set_job_status,
    transition_daemon_state,
)
from photovault_clientd.engine import (
    DEFAULT_RETAIN_STAGED_FILES,
    DEFAULT_SERVER_BASE_URL,
    run_daemon_tick,
    run_error_file_requeue,
    run_recovery_dispatch,
)
from photovault_clientd.events import EventCategory, EventLevel, classify_copy_error
from photovault_clientd.m0_checks import run_m0_foundation_checks
from photovault_clientd.state_machine import ClientState
from photovault_clientd.storage import build_staged_path, copy_with_fsync

DEFAULT_DB_PATH = Path("/var/lib/photovault-clientd/state.sqlite3")
DEFAULT_STAGING_ROOT = Path("/var/lib/photovault-clientd/staging")


class IngestJobCreateRequest(BaseModel):
    media_label: str = Field(min_length=1, max_length=255)
    source_paths: list[str] = Field(min_length=1)


class IngestStageNextRequest(BaseModel):
    job_id: int
    staging_root: str = Field(min_length=1)


def _format_path_os_error(exc: OSError) -> str:
    return exc.strerror or exc.__class__.__name__


def _enumerate_directory_files(path: Path) -> list[str]:
    discovered: list[str] = []
    walk_errors: list[OSError] = []

    def _on_walk_error(exc: OSError) -> None:
        walk_errors.append(exc)

    for root, dirnames, filenames in os.walk(path, onerror=_on_walk_error):
        if walk_errors:
            break
        dirnames.sort()
        filenames.sort()
        for filename in filenames:
            file_path = Path(root) / filename
            try:
                if file_path.is_file():
                    discovered.append(str(file_path))
            except OSError as exc:
                raise OSError(f"failed to stat {file_path}: {_format_path_os_error(exc)}") from exc

    if walk_errors:
        raise OSError(
            f"failed to read directory {path}: {_format_path_os_error(walk_errors[0])}"
        ) from walk_errors[0]
    return discovered


def _discover_source_files(
    source_paths: list[str],
) -> tuple[list[str], list[dict[str, str]]]:
    discovered: list[str] = []
    invalid_sources: list[dict[str, str]] = []
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
                discovered.append(str(source))
                continue
            if source.is_dir():
                directory_files = _enumerate_directory_files(source)
                if not directory_files:
                    invalid_sources.append(
                        {
                            "source_path": source_path,
                            "reason": "Directory does not contain readable files.",
                        }
                    )
                    continue
                discovered.extend(directory_files)
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
    return discovered, invalid_sources


def _next_state_for_stage_phase(pending_copy: int, hash_pending: int) -> ClientState:
    if pending_copy > 0:
        return ClientState.STAGING_COPY
    if hash_pending > 0:
        return ClientState.HASHING
    return ClientState.IDLE


def create_app(
    db_path: Path = DEFAULT_DB_PATH,
    staging_root: Path = DEFAULT_STAGING_ROOT,
    server_base_url: str = DEFAULT_SERVER_BASE_URL,
    retain_staged_files: bool = DEFAULT_RETAIN_STAGED_FILES,
) -> FastAPI:
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
                server_base_url=server_base_url,
                retain_staged_files=retain_staged_files,
            )
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
        yield

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
    def daemon_state() -> dict[str, str]:
        conn = open_db(db_path)
        row = conn.execute("SELECT current_state, updated_at_utc FROM daemon_state WHERE id = 1;").fetchone()
        conn.close()
        if row is None:
            return {"current_state": ClientState.ERROR_DAEMON.value, "updated_at_utc": ""}
        return {"current_state": row[0], "updated_at_utc": row[1]}

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
        conn = open_db(db_path)
        outcome = run_daemon_tick(
            conn,
            staging_root,
            server_base_url=server_base_url,
            retain_staged_files=retain_staged_files,
        )
        conn.close()
        return outcome

    @app.post("/ingest/jobs")
    def create_ingest(request: IngestJobCreateRequest) -> dict[str, object]:
        conn = open_db(db_path)
        now = datetime.now(UTC).isoformat()
        current_state = get_daemon_state(conn)
        if current_state != ClientState.IDLE:
            conn.close()
            raise HTTPException(status_code=409, detail=f"daemon must be IDLE, got {current_state}")

        discovered_source_paths, invalid_sources = _discover_source_files(request.source_paths)
        if invalid_sources:
            conn.close()
            raise HTTPException(
                status_code=422,
                detail={
                    "code": "INGEST_SOURCE_PATH_INVALID",
                    "message": "One or more source paths could not be used for ingest discovery.",
                    "invalid_sources": invalid_sources,
                    "suggestion": "Fix the listed paths, then retry ingest creation.",
                },
            )

        transition_daemon_state(conn, ClientState.DISCOVERING, now, reason="ingest job created", commit=False)
        job_id = create_ingest_job(conn, request.media_label, now)
        discovered_count = insert_discovered_files(conn, job_id, discovered_source_paths, now)
        set_job_status(conn, job_id, ClientState.STAGING_COPY.value, now)
        transition_daemon_state(
            conn,
            ClientState.STAGING_COPY,
            now,
            reason="discovery completed; entering staging copy",
            commit=False,
        )
        conn.commit()
        conn.close()
        return {
            "job_id": job_id,
            "discovered_count": discovered_count,
            "state": ClientState.STAGING_COPY.value,
        }

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
