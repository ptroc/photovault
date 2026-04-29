"""Heartbeat summary and sender helpers for clientd engine state."""

import json
from datetime import UTC, datetime, timedelta
from urllib.error import HTTPError, URLError

from photovault_clientd.db import (
    CLIENT_ENROLLMENT_PENDING,
    CLIENT_ENROLLMENT_REVOKED,
    TERMINAL_FILE_STATUSES,
    fetch_ingest_job_detail,
    fetch_next_job_with_status,
    fetch_next_ready_to_upload_file,
    fetch_next_uploaded_file,
    fetch_recent_daemon_events,
    fetch_server_auth_state,
    fetch_server_heartbeat_state,
    fetch_wait_network_retry_candidates,
    get_daemon_state,
    upsert_server_heartbeat_state,
)
from photovault_clientd.events import EventLevel
from photovault_clientd.state_machine import ClientState, FileStatus

from .http_helpers import (
    _build_client_auth_headers,
    _extract_http_error_detail,
    _post_client_heartbeat,
    _update_auth_state_from_privileged_http_error,
)
from .upload_common import AUTH_BLOCKED_DETAILS

DEFAULT_RETRY_BACKOFF_MAX_SECONDS = 30
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 15


def _heartbeat_workload_status(*, state: ClientState | None, auth_block_reason: str | None) -> str:
    if auth_block_reason:
        return "blocked"
    if state in {
        ClientState.ERROR_DAEMON,
        ClientState.ERROR_FILE,
        ClientState.ERROR_JOB,
        ClientState.PAUSED_STORAGE,
    }:
        return "blocked"
    if state == ClientState.IDLE:
        return "idle"
    if state == ClientState.WAIT_NETWORK:
        return "waiting"
    return "working"


def _heartbeat_auth_block_reason(conn) -> str | None:
    auth_state = fetch_server_auth_state(conn)
    if auth_state is None:
        return None
    enrollment_status = str(auth_state.get("enrollment_status") or "")
    if enrollment_status == CLIENT_ENROLLMENT_PENDING:
        return "CLIENT_PENDING_APPROVAL"
    if enrollment_status == CLIENT_ENROLLMENT_REVOKED:
        return "CLIENT_REVOKED"
    last_error = str(auth_state.get("last_error") or "")
    if last_error in AUTH_BLOCKED_DETAILS:
        return last_error
    return None


def _heartbeat_active_job_summary(conn) -> dict[str, object] | None:
    current_state = get_daemon_state(conn)
    job_id: int | None = None
    if current_state in {
        ClientState.STAGING_COPY,
        ClientState.HASHING,
        ClientState.DEDUP_SESSION_SHA,
        ClientState.DEDUP_LOCAL_SHA,
        ClientState.QUEUE_UPLOAD,
        ClientState.UPLOAD_PREPARE,
        ClientState.UPLOAD_FILE,
        ClientState.SERVER_VERIFY,
        ClientState.REUPLOAD_OR_QUARANTINE,
        ClientState.POST_UPLOAD_VERIFY,
        ClientState.CLEANUP_STAGING,
        ClientState.JOB_COMPLETE_REMOTE,
        ClientState.JOB_COMPLETE_LOCAL,
        ClientState.WAIT_NETWORK,
    }:
        job_id = fetch_next_job_with_status(conn, current_state)
    if job_id is None:
        candidate = fetch_next_ready_to_upload_file(conn)
        if candidate is not None:
            job_id = int(candidate["job_id"])
        else:
            uploaded = fetch_next_uploaded_file(conn)
            if uploaded is not None:
                job_id = int(uploaded["job_id"])
    if job_id is None:
        return None

    detail = fetch_ingest_job_detail(conn, job_id)
    if detail is None:
        return None
    status_counts = detail.get("status_counts") or {}
    total_files = sum(int(count) for count in status_counts.values())
    terminal_files = sum(
        int(status_counts.get(terminal_status, 0))
        for terminal_status in TERMINAL_FILE_STATUSES
    )
    non_terminal_files = max(0, total_files - terminal_files)
    retrying = int(status_counts.get(FileStatus.NEEDS_RETRY_COPY.value, 0)) + int(
        status_counts.get(FileStatus.NEEDS_RETRY_HASH.value, 0)
    )
    error_files = int(status_counts.get(FileStatus.ERROR_FILE.value, 0))
    blocking_reason: str | None = None
    if current_state in {
        ClientState.WAIT_NETWORK,
        ClientState.PAUSED_STORAGE,
        ClientState.ERROR_DAEMON,
        ClientState.ERROR_FILE,
        ClientState.ERROR_JOB,
    }:
        blocking_reason = current_state.value
    elif error_files > 0:
        blocking_reason = "ERROR_FILE_PRESENT"
    return {
        "job_id": int(detail["job_id"]),
        "media_label": detail.get("media_label"),
        "job_status": str(detail.get("status") or "unknown"),
        "ready_to_upload": int(status_counts.get(FileStatus.READY_TO_UPLOAD.value, 0)),
        "uploaded": int(status_counts.get(FileStatus.UPLOADED.value, 0)),
        "retrying": retrying,
        "total_files": total_files,
        "non_terminal_files": non_terminal_files,
        "error_files": error_files,
        "blocking_reason": blocking_reason,
    }


def _retry_backoff_seconds(retry_count: int) -> int:
    if retry_count <= 0:
        return 0
    return min(2 ** (retry_count - 1), DEFAULT_RETRY_BACKOFF_MAX_SECONDS)


def _retry_due_time(updated_at_utc: str, retry_count: int) -> datetime:
    updated_at = datetime.fromisoformat(updated_at_utc)
    return updated_at + timedelta(seconds=_retry_backoff_seconds(retry_count))


def _heartbeat_retry_backoff_summary(conn) -> dict[str, object] | None:
    candidates = fetch_wait_network_retry_candidates(conn)
    if not candidates:
        return None
    retried_rows = [row for row in candidates if int(row["retry_count"]) > 0]
    if not retried_rows:
        return None

    next_retry_at: datetime | None = None
    most_recent_error: tuple[datetime, str] | None = None
    for row in retried_rows:
        retry_due_at = _retry_due_time(str(row["updated_at_utc"]), int(row["retry_count"]))
        if next_retry_at is None or retry_due_at < next_retry_at:
            next_retry_at = retry_due_at
        last_error = str(row.get("last_error") or "").strip()
        if not last_error:
            continue
        try:
            updated_at = datetime.fromisoformat(str(row["updated_at_utc"]))
        except ValueError:
            continue
        if most_recent_error is None or updated_at > most_recent_error[0]:
            most_recent_error = (updated_at, last_error)
    return {
        "pending_count": len(retried_rows),
        "next_retry_at_utc": next_retry_at.isoformat() if next_retry_at is not None else None,
        "reason": most_recent_error[1] if most_recent_error is not None else None,
    }


def _heartbeat_recent_error(conn) -> dict[str, object] | None:
    for event in fetch_recent_daemon_events(conn, limit=25):
        if str(event.get("level")) != EventLevel.ERROR.value:
            continue
        message = str(event.get("message") or "").strip()
        if not message:
            continue
        return {
            "category": str(event.get("category") or "ERROR"),
            "message": message[:512],
            "created_at_utc": str(event.get("created_at_utc") or ""),
        }
    return None


def _build_client_heartbeat_payload(conn, *, now_utc: str) -> dict[str, object]:
    state = get_daemon_state(conn)
    auth_block_reason = _heartbeat_auth_block_reason(conn)
    return {
        "last_seen_at_utc": now_utc,
        "daemon_state": state.value if state is not None else ClientState.ERROR_DAEMON.value,
        "workload_status": _heartbeat_workload_status(state=state, auth_block_reason=auth_block_reason),
        "active_job": _heartbeat_active_job_summary(conn),
        "retry_backoff": _heartbeat_retry_backoff_summary(conn),
        "auth_block_reason": auth_block_reason,
        "recent_error": _heartbeat_recent_error(conn),
    }


def run_client_heartbeat_tick(
    conn,
    *,
    server_base_url: str,
    client_id: str,
    client_display_name: str,
    bootstrap_token: str | None,
    heartbeat_interval_seconds: int = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
) -> dict[str, object]:
    now = datetime.now(UTC)
    now_utc = now.isoformat()
    interval_seconds = max(1, heartbeat_interval_seconds)
    current = fetch_server_heartbeat_state(conn)

    if current is None:
        next_due = (now + timedelta(seconds=interval_seconds)).isoformat()
        upsert_server_heartbeat_state(
            conn,
            heartbeat_interval_seconds=interval_seconds,
            last_attempt_at_utc=None,
            last_success_at_utc=None,
            last_error=None,
            last_status="initialized",
            next_due_at_utc=next_due,
            updated_at_utc=now_utc,
        )
        conn.commit()
        return {
            "handled": True,
            "sent": False,
            "reason": "initialized",
            "next_due_at_utc": next_due,
        }

    next_due_raw = current.get("next_due_at_utc")
    if isinstance(next_due_raw, str) and next_due_raw:
        try:
            next_due_at = datetime.fromisoformat(next_due_raw)
        except ValueError:
            next_due_at = None
        if next_due_at is not None and now < next_due_at:
            return {
                "handled": True,
                "sent": False,
                "reason": "not_due",
                "next_due_at_utc": next_due_at.isoformat(),
            }

    auth_headers, auth_block_reason = _build_client_auth_headers(
        conn,
        server_base_url=server_base_url,
        client_id=client_id,
        display_name=client_display_name,
        bootstrap_token=bootstrap_token,
        now_utc=now_utc,
    )
    next_due_at_utc = (now + timedelta(seconds=interval_seconds)).isoformat()
    if auth_headers is None:
        blocked_reason = auth_block_reason or "CLIENT_AUTH_REQUIRED"
        upsert_server_heartbeat_state(
            conn,
            heartbeat_interval_seconds=interval_seconds,
            last_attempt_at_utc=now_utc,
            last_success_at_utc=current.get("last_success_at_utc"),
            last_error=blocked_reason,
            last_status="auth_blocked",
            next_due_at_utc=next_due_at_utc,
            updated_at_utc=now_utc,
        )
        conn.commit()
        return {
            "handled": True,
            "sent": False,
            "reason": "auth_blocked",
            "auth_reason": blocked_reason,
            "next_due_at_utc": next_due_at_utc,
        }

    payload = _build_client_heartbeat_payload(conn, now_utc=now_utc)
    try:
        _post_client_heartbeat(server_base_url=server_base_url, headers=auth_headers, payload=payload)
    except HTTPError as exc:
        auth_detail = _update_auth_state_from_privileged_http_error(conn, now_utc=now_utc, exc=exc)
        detail = auth_detail or _extract_http_error_detail(exc)
        upsert_server_heartbeat_state(
            conn,
            heartbeat_interval_seconds=interval_seconds,
            last_attempt_at_utc=now_utc,
            last_success_at_utc=current.get("last_success_at_utc"),
            last_error=detail,
            last_status="error",
            next_due_at_utc=next_due_at_utc,
            updated_at_utc=now_utc,
        )
        conn.commit()
        return {
            "handled": True,
            "sent": False,
            "reason": "error",
            "error": detail,
            "next_due_at_utc": next_due_at_utc,
        }
    except (URLError, TimeoutError, ValueError, OSError, json.JSONDecodeError) as exc:
        upsert_server_heartbeat_state(
            conn,
            heartbeat_interval_seconds=interval_seconds,
            last_attempt_at_utc=now_utc,
            last_success_at_utc=current.get("last_success_at_utc"),
            last_error=str(exc),
            last_status="error",
            next_due_at_utc=next_due_at_utc,
            updated_at_utc=now_utc,
        )
        conn.commit()
        return {
            "handled": True,
            "sent": False,
            "reason": "error",
            "error": str(exc),
            "next_due_at_utc": next_due_at_utc,
        }

    upsert_server_heartbeat_state(
        conn,
        heartbeat_interval_seconds=interval_seconds,
        last_attempt_at_utc=now_utc,
        last_success_at_utc=now_utc,
        last_error=None,
        last_status="sent",
        next_due_at_utc=next_due_at_utc,
        updated_at_utc=now_utc,
    )
    conn.commit()
    return {
        "handled": True,
        "sent": True,
        "reason": "sent",
        "next_due_at_utc": next_due_at_utc,
    }

