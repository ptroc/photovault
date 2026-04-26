"""Single-thread daemon tick and recovery helpers for photovault-clientd."""

import json
import subprocess
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from photovault_clientd.db import (
    CLIENT_ENROLLMENT_APPROVED,
    CLIENT_ENROLLMENT_PENDING,
    CLIENT_ENROLLMENT_REVOKED,
    TERMINAL_FILE_STATUSES,
    append_daemon_event,
    clear_ready_to_upload_error,
    count_hash_pending_files_global,
    count_hashed_files,
    count_hashed_files_global,
    count_job_files_by_statuses,
    count_non_terminal_files_for_job,
    count_pending_copy_files_global,
    count_ready_to_upload_files,
    count_ready_to_upload_files_global,
    count_staged_files_global,
    count_uploaded_files_global,
    fetch_cleanup_remote_terminal_files,
    fetch_hashed_files_for_job,
    fetch_ingest_job_detail,
    fetch_next_copy_candidate,
    fetch_next_hash_candidate,
    fetch_next_hashing_job_with_pending_hash,
    fetch_next_job_with_status,
    fetch_next_ready_to_upload_file,
    fetch_next_staging_job_with_pending_copy,
    fetch_next_uploaded_file,
    fetch_ready_to_upload_files_global,
    fetch_recent_daemon_events,
    fetch_reupload_target_file,
    fetch_server_auth_state,
    fetch_server_heartbeat_state,
    fetch_wait_network_retry_candidates,
    get_daemon_state,
    local_sha_exists,
    mark_file_copy_retry,
    mark_file_duplicate_global,
    mark_file_hash_retry,
    mark_file_hashed,
    mark_file_staged,
    mark_file_uploaded,
    mark_file_verified_remote,
    mark_files_duplicate_local,
    mark_files_duplicate_session,
    mark_files_ready_to_upload,
    mark_files_upload_retry,
    mark_ready_to_upload_error,
    mark_ready_to_upload_retry,
    mark_uploaded_for_reupload,
    mark_uploaded_retry,
    register_local_sha,
    replace_copy_candidate_with_discovered_files,
    requeue_error_file_for_upload,
    set_job_status,
    transition_daemon_state,
    upsert_server_auth_state,
    upsert_server_heartbeat_state,
)
from photovault_clientd.events import EventCategory, EventLevel, classify_copy_error, classify_hash_error
from photovault_clientd.hashing import compute_sha256
from photovault_clientd.ingest_policy import enumerate_directory_media_files
from photovault_clientd.networking import parse_nmcli_multiline
from photovault_clientd.state_machine import ClientState, FileStatus
from photovault_clientd.storage import build_staged_path, copy_with_fsync

DEFAULT_SERVER_BASE_URL = "http://127.0.0.1:9301"
DEFAULT_HANDSHAKE_TIMEOUT_SECONDS = 5.0
DEFAULT_RETAIN_STAGED_FILES = True
DEFAULT_MAX_UPLOAD_RETRIES = 3
DEFAULT_RETRY_BACKOFF_MAX_SECONDS = 30
DEFAULT_AUTO_PROGRESS_MAX_STEPS = 32
DEFAULT_ENROLL_TIMEOUT_SECONDS = 5.0
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 15

_CLIENT_REQUEST_AUTH_HEADERS: dict[str, str] = {}

AUTO_PROGRESS_SAFE_STATES = {
    ClientState.IDLE,
    ClientState.STAGING_COPY,
    ClientState.HASHING,
    ClientState.DEDUP_SESSION_SHA,
    ClientState.DEDUP_LOCAL_SHA,
    ClientState.QUEUE_UPLOAD,
    ClientState.WAIT_NETWORK,
    ClientState.UPLOAD_PREPARE,
    ClientState.UPLOAD_FILE,
    ClientState.SERVER_VERIFY,
    ClientState.REUPLOAD_OR_QUARANTINE,
    ClientState.POST_UPLOAD_VERIFY,
    ClientState.CLEANUP_STAGING,
    ClientState.JOB_COMPLETE_REMOTE,
    ClientState.JOB_COMPLETE_LOCAL,
}

_AUTH_BLOCKED_DETAILS = {
    "CLIENT_PENDING_APPROVAL",
    "CLIENT_REVOKED",
    "CLIENT_AUTH_REQUIRED",
    "CLIENT_AUTH_INVALID",
}


def _copy_phase_next_state(pending_copy: int, hash_pending: int) -> ClientState:
    if pending_copy > 0:
        return ClientState.STAGING_COPY
    if hash_pending > 0:
        return ClientState.HASHING
    return ClientState.IDLE


def _hash_phase_next_state(pending_copy: int, hash_pending: int, hashed_ready: int) -> ClientState:
    if pending_copy > 0:
        return ClientState.STAGING_COPY
    if hash_pending > 0:
        return ClientState.HASHING
    if hashed_ready > 0:
        return ClientState.DEDUP_SESSION_SHA
    return ClientState.IDLE


def _hashed_groups(rows: list[dict[str, object]]) -> dict[str, list[int]]:
    grouped: dict[str, list[int]] = defaultdict(list)
    for row in rows:
        grouped[str(row["sha256_hex"])].append(int(row["file_id"]))
    return grouped


def _next_online_state(conn) -> ClientState:
    if count_uploaded_files_global(conn) > 0:
        return ClientState.SERVER_VERIFY
    if fetch_next_job_with_status(conn, ClientState.POST_UPLOAD_VERIFY) is not None:
        return ClientState.POST_UPLOAD_VERIFY
    if fetch_next_job_with_status(conn, ClientState.CLEANUP_STAGING) is not None:
        return ClientState.CLEANUP_STAGING
    if fetch_next_job_with_status(conn, ClientState.JOB_COMPLETE_REMOTE) is not None:
        return ClientState.JOB_COMPLETE_REMOTE
    if count_ready_to_upload_files_global(conn) > 0:
        return ClientState.UPLOAD_PREPARE
    if fetch_next_job_with_status(conn, ClientState.JOB_COMPLETE_LOCAL) is not None:
        return ClientState.JOB_COMPLETE_LOCAL
    return ClientState.WAIT_NETWORK


def _set_client_request_auth_headers(headers: dict[str, str] | None) -> None:
    global _CLIENT_REQUEST_AUTH_HEADERS
    _CLIENT_REQUEST_AUTH_HEADERS = dict(headers or {})


def _extract_http_error_detail(exc: HTTPError) -> str:
    try:
        body = exc.read().decode("utf-8")
    except Exception:
        body = ""
    if not body:
        return f"HTTP {exc.code}"
    try:
        decoded = json.loads(body)
    except json.JSONDecodeError:
        return body
    detail = decoded.get("detail")
    if isinstance(detail, str):
        return detail
    return body


def _build_client_auth_headers(
    conn,
    *,
    server_base_url: str,
    client_id: str,
    display_name: str,
    bootstrap_token: str | None,
    now_utc: str,
) -> tuple[dict[str, str] | None, str | None]:
    auth_state = fetch_server_auth_state(conn)
    effective_client_id = client_id
    effective_display_name = display_name
    if auth_state is not None:
        existing_client_id = str(auth_state.get("client_id", "")).strip()
        existing_display_name = str(auth_state.get("display_name", "")).strip()
        if existing_client_id:
            effective_client_id = existing_client_id
        if existing_display_name:
            effective_display_name = existing_display_name

    if (
        auth_state is not None
        and str(auth_state.get("enrollment_status")) == CLIENT_ENROLLMENT_APPROVED
        and isinstance(auth_state.get("auth_token"), str)
        and str(auth_state.get("auth_token"))
    ):
        return {
            "x-photovault-client-id": str(auth_state["client_id"]),
            "x-photovault-client-token": str(auth_state["auth_token"]),
        }, None

    if not bootstrap_token:
        if auth_state is not None and str(auth_state.get("enrollment_status")) == CLIENT_ENROLLMENT_PENDING:
            return None, "CLIENT_PENDING_APPROVAL"
        if auth_state is not None and str(auth_state.get("enrollment_status")) == CLIENT_ENROLLMENT_REVOKED:
            return None, "CLIENT_REVOKED"
        return {}, None

    request = Request(
        url=f"{server_base_url.rstrip('/')}/v1/client/enroll/bootstrap",
        data=json.dumps(
            {
                "client_id": effective_client_id,
                "display_name": effective_display_name,
                "bootstrap_token": bootstrap_token,
            }
        ).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=DEFAULT_ENROLL_TIMEOUT_SECONDS) as response:
            body = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        detail = _extract_http_error_detail(exc)
        upsert_server_auth_state(
            conn,
            client_id=effective_client_id,
            display_name=effective_display_name,
            enrollment_status=CLIENT_ENROLLMENT_PENDING,
            auth_token=None,
            server_first_seen_at_utc=None,
            server_last_enrolled_at_utc=None,
            approved_at_utc=None,
            revoked_at_utc=None,
            last_enrollment_attempt_at_utc=now_utc,
            last_enrollment_result_at_utc=now_utc,
            last_error=detail,
            updated_at_utc=now_utc,
        )
        conn.commit()
        return None, detail
    except (URLError, TimeoutError, ValueError, OSError, json.JSONDecodeError) as exc:
        upsert_server_auth_state(
            conn,
            client_id=effective_client_id,
            display_name=effective_display_name,
            enrollment_status=CLIENT_ENROLLMENT_PENDING,
            auth_token=None,
            server_first_seen_at_utc=None,
            server_last_enrolled_at_utc=None,
            approved_at_utc=None,
            revoked_at_utc=None,
            last_enrollment_attempt_at_utc=now_utc,
            last_enrollment_result_at_utc=now_utc,
            last_error=str(exc),
            updated_at_utc=now_utc,
        )
        conn.commit()
        return None, str(exc)

    enrollment_status = str(body.get("enrollment_status") or CLIENT_ENROLLMENT_PENDING)
    auth_token = body.get("auth_token")
    approved_at_utc = now_utc if enrollment_status == CLIENT_ENROLLMENT_APPROVED else None
    revoked_at_utc = now_utc if enrollment_status == CLIENT_ENROLLMENT_REVOKED else None
    upsert_server_auth_state(
        conn,
        client_id=str(body.get("client_id") or client_id),
        display_name=str(body.get("display_name") or display_name),
        enrollment_status=enrollment_status,
        auth_token=str(auth_token) if isinstance(auth_token, str) and auth_token else None,
        server_first_seen_at_utc=(
            str(body.get("first_seen_at_utc")) if body.get("first_seen_at_utc") is not None else None
        ),
        server_last_enrolled_at_utc=(
            str(body.get("last_enrolled_at_utc"))
            if body.get("last_enrolled_at_utc") is not None
            else None
        ),
        approved_at_utc=approved_at_utc,
        revoked_at_utc=revoked_at_utc,
        last_enrollment_attempt_at_utc=now_utc,
        last_enrollment_result_at_utc=now_utc,
        last_error=None,
        updated_at_utc=now_utc,
    )
    conn.commit()
    if enrollment_status == CLIENT_ENROLLMENT_APPROVED and isinstance(auth_token, str) and auth_token:
        return {
            "x-photovault-client-id": str(body.get("client_id") or effective_client_id),
            "x-photovault-client-token": auth_token,
        }, None
    if enrollment_status == CLIENT_ENROLLMENT_REVOKED:
        return None, "CLIENT_REVOKED"
    return None, "CLIENT_PENDING_APPROVAL"


def _update_auth_state_from_privileged_http_error(
    conn,
    *,
    now_utc: str,
    exc: HTTPError,
) -> str | None:
    detail = _extract_http_error_detail(exc)
    auth_state = fetch_server_auth_state(conn)
    if auth_state is None:
        return None
    if detail == "CLIENT_PENDING_APPROVAL":
        upsert_server_auth_state(
            conn,
            client_id=str(auth_state["client_id"]),
            display_name=str(auth_state["display_name"]),
            enrollment_status=CLIENT_ENROLLMENT_PENDING,
            auth_token=None,
            server_first_seen_at_utc=auth_state.get("server_first_seen_at_utc"),
            server_last_enrolled_at_utc=auth_state.get("server_last_enrolled_at_utc"),
            approved_at_utc=None,
            revoked_at_utc=auth_state.get("revoked_at_utc"),
            last_enrollment_attempt_at_utc=auth_state.get("last_enrollment_attempt_at_utc"),
            last_enrollment_result_at_utc=auth_state.get("last_enrollment_result_at_utc"),
            last_error=detail,
            updated_at_utc=now_utc,
        )
        conn.commit()
        return detail
    if detail == "CLIENT_REVOKED":
        upsert_server_auth_state(
            conn,
            client_id=str(auth_state["client_id"]),
            display_name=str(auth_state["display_name"]),
            enrollment_status=CLIENT_ENROLLMENT_REVOKED,
            auth_token=auth_state.get("auth_token"),
            server_first_seen_at_utc=auth_state.get("server_first_seen_at_utc"),
            server_last_enrolled_at_utc=auth_state.get("server_last_enrolled_at_utc"),
            approved_at_utc=auth_state.get("approved_at_utc"),
            revoked_at_utc=now_utc,
            last_enrollment_attempt_at_utc=auth_state.get("last_enrollment_attempt_at_utc"),
            last_enrollment_result_at_utc=auth_state.get("last_enrollment_result_at_utc"),
            last_error=detail,
            updated_at_utc=now_utc,
        )
        conn.commit()
        return detail
    if detail in {"CLIENT_AUTH_REQUIRED", "CLIENT_AUTH_INVALID"}:
        upsert_server_auth_state(
            conn,
            client_id=str(auth_state["client_id"]),
            display_name=str(auth_state["display_name"]),
            enrollment_status=str(auth_state["enrollment_status"]),
            auth_token=auth_state.get("auth_token"),
            server_first_seen_at_utc=auth_state.get("server_first_seen_at_utc"),
            server_last_enrolled_at_utc=auth_state.get("server_last_enrolled_at_utc"),
            approved_at_utc=auth_state.get("approved_at_utc"),
            revoked_at_utc=auth_state.get("revoked_at_utc"),
            last_enrollment_attempt_at_utc=auth_state.get("last_enrollment_attempt_at_utc"),
            last_enrollment_result_at_utc=auth_state.get("last_enrollment_result_at_utc"),
            last_error=detail,
            updated_at_utc=now_utc,
        )
        conn.commit()
        return detail
    return None


def _post_metadata_handshake(
    *,
    server_base_url: str,
    files: list[dict[str, object]],
    timeout_seconds: float = DEFAULT_HANDSHAKE_TIMEOUT_SECONDS,
) -> dict[int, str]:
    payload = {
        "files": [
            {
                "client_file_id": int(item["file_id"]),
                "sha256_hex": str(item["sha256_hex"]),
                "size_bytes": int(item["size_bytes"]),
            }
            for item in files
        ]
    }
    request = Request(
        url=f"{server_base_url.rstrip('/')}/v1/upload/metadata-handshake",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", **_CLIENT_REQUEST_AUTH_HEADERS},
        method="POST",
    )
    with urlopen(request, timeout=timeout_seconds) as response:
        body = json.loads(response.read().decode("utf-8"))

    raw_results = body.get("results")
    if not isinstance(raw_results, list):
        raise ValueError("handshake response missing results list")

    results: dict[int, str] = {}
    for item in raw_results:
        if not isinstance(item, dict):
            raise ValueError("handshake result item must be an object")
        file_id = item.get("client_file_id")
        decision = item.get("decision")
        if not isinstance(file_id, int):
            raise ValueError("handshake result missing numeric client_file_id")
        if decision not in {"ALREADY_EXISTS", "UPLOAD_REQUIRED"}:
            raise ValueError(f"handshake result has invalid decision for file_id={file_id}")
        results[file_id] = decision
    return results


def _upload_file_content(
    *,
    server_base_url: str,
    sha256_hex: str,
    size_bytes: int,
    content: bytes,
    job_name: str | None = None,
    original_filename: str | None = None,
    timeout_seconds: float = DEFAULT_HANDSHAKE_TIMEOUT_SECONDS,
) -> str:
    headers = {"x-size-bytes": str(size_bytes)}
    headers.update(_CLIENT_REQUEST_AUTH_HEADERS)
    if job_name is not None:
        headers["x-job-name"] = job_name
    if original_filename is not None:
        headers["x-original-filename"] = original_filename

    request = Request(
        url=f"{server_base_url.rstrip('/')}/v1/upload/content/{sha256_hex}",
        data=content,
        headers=headers,
        method="PUT",
    )
    with urlopen(request, timeout=timeout_seconds) as response:
        body = json.loads(response.read().decode("utf-8"))

    status = body.get("status")
    if status not in {"STORED_TEMP", "ALREADY_EXISTS"}:
        raise ValueError("upload response has invalid status")
    return str(status)


def _post_server_verify(
    *,
    server_base_url: str,
    sha256_hex: str,
    size_bytes: int,
    timeout_seconds: float = DEFAULT_HANDSHAKE_TIMEOUT_SECONDS,
) -> str:
    request = Request(
        url=f"{server_base_url.rstrip('/')}/v1/upload/verify",
        data=json.dumps({"sha256_hex": sha256_hex, "size_bytes": size_bytes}).encode("utf-8"),
        headers={"Content-Type": "application/json", **_CLIENT_REQUEST_AUTH_HEADERS},
        method="POST",
    )
    with urlopen(request, timeout=timeout_seconds) as response:
        body = json.loads(response.read().decode("utf-8"))

    status = body.get("status")
    if status not in {"VERIFIED", "ALREADY_EXISTS", "VERIFY_FAILED"}:
        raise ValueError("verify response has invalid status")
    return str(status)


def _post_client_heartbeat(
    *,
    server_base_url: str,
    headers: dict[str, str],
    payload: dict[str, object],
    timeout_seconds: float = DEFAULT_HANDSHAKE_TIMEOUT_SECONDS,
) -> dict[str, object]:
    request = Request(
        url=f"{server_base_url.rstrip('/')}/v1/client/heartbeat",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", **headers},
        method="POST",
    )
    with urlopen(request, timeout=timeout_seconds) as response:
        body = json.loads(response.read().decode("utf-8"))
    if not isinstance(body, dict):
        raise ValueError("heartbeat response must be a JSON object")
    return body


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
    if last_error in _AUTH_BLOCKED_DETAILS:
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
        if fetch_next_ready_to_upload_file(conn) is not None:
            candidate = fetch_next_ready_to_upload_file(conn)
            if candidate is not None:
                job_id = int(candidate["job_id"])
        elif fetch_next_uploaded_file(conn) is not None:
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
    payload: dict[str, object] = {
        "last_seen_at_utc": now_utc,
        "daemon_state": state.value if state is not None else ClientState.ERROR_DAEMON.value,
        "workload_status": _heartbeat_workload_status(state=state, auth_block_reason=auth_block_reason),
        "active_job": _heartbeat_active_job_summary(conn),
        "retry_backoff": _heartbeat_retry_backoff_summary(conn),
        "auth_block_reason": auth_block_reason,
        "recent_error": _heartbeat_recent_error(conn),
    }
    return payload


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


def _retry_backoff_seconds(retry_count: int) -> int:
    if retry_count <= 0:
        return 0
    return min(2 ** (retry_count - 1), DEFAULT_RETRY_BACKOFF_MAX_SECONDS)


def _retry_due_time(updated_at_utc: str, retry_count: int) -> datetime:
    updated_at = datetime.fromisoformat(updated_at_utc)
    return updated_at + timedelta(seconds=_retry_backoff_seconds(retry_count))


def _network_is_online() -> bool:
    """Return True when NetworkManager reports connected state."""
    try:
        completed = subprocess.run(
            ["nmcli", "-m", "multiline", "-f", "STATE,CONNECTIVITY", "general"],
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        # v1 contract: networking must be gated through NetworkManager availability.
        return False
    except subprocess.CalledProcessError:
        return False

    records = parse_nmcli_multiline(completed.stdout)
    row = records[0] if records else {}
    state = str(row.get("STATE", "")).strip().lower()
    connectivity = str(row.get("CONNECTIVITY", "")).strip().lower()

    if state != "connected":
        return False
    return connectivity in {"full", "limited", "portal", "unknown"}


def run_daemon_tick(
    conn,
    staging_root: Path,
    *,
    server_base_url: str = DEFAULT_SERVER_BASE_URL,
    client_id: str,
    client_display_name: str,
    bootstrap_token: str | None = None,
    retain_staged_files: bool = DEFAULT_RETAIN_STAGED_FILES,
    max_upload_retries: int = DEFAULT_MAX_UPLOAD_RETRIES,
    heartbeat_interval_seconds: int = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
) -> dict[str, object]:

    from .ingest import run_staging_copy_tick, run_hashing_tick, run_session_dedup_tick, run_local_dedup_tick
    from .upload import run_queue_upload_tick, run_wait_network_tick, run_upload_prepare_tick, run_upload_file_tick, run_server_verify_tick, run_reupload_or_quarantine_tick, run_post_upload_verify_tick
    from .cleanup import run_cleanup_staging_tick, run_job_complete_local_tick, run_job_complete_remote_tick, run_error_file_requeue
    """Run one daemon tick for the current state."""
    run_client_heartbeat_tick(
        conn,
        server_base_url=server_base_url,
        client_id=client_id,
        client_display_name=client_display_name,
        bootstrap_token=bootstrap_token,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
    )

    state = get_daemon_state(conn)
    if state == ClientState.STAGING_COPY:
        return run_staging_copy_tick(conn, staging_root)
    if state == ClientState.HASHING:
        return run_hashing_tick(conn)
    if state == ClientState.DEDUP_SESSION_SHA:
        return run_session_dedup_tick(conn)
    if state == ClientState.DEDUP_LOCAL_SHA:
        return run_local_dedup_tick(conn)
    if state == ClientState.QUEUE_UPLOAD:
        return run_queue_upload_tick(conn)
    if state == ClientState.WAIT_NETWORK:
        return run_wait_network_tick(
            conn,
            server_base_url=server_base_url,
            client_id=client_id,
            client_display_name=client_display_name,
            bootstrap_token=bootstrap_token,
        )
    if state == ClientState.UPLOAD_PREPARE:
        return run_upload_prepare_tick(conn, server_base_url=server_base_url)
    if state == ClientState.UPLOAD_FILE:
        return run_upload_file_tick(conn, server_base_url=server_base_url)
    if state == ClientState.SERVER_VERIFY:
        return run_server_verify_tick(conn, server_base_url=server_base_url)
    if state == ClientState.REUPLOAD_OR_QUARANTINE:
        return run_reupload_or_quarantine_tick(conn, max_upload_retries=max_upload_retries)
    if state == ClientState.POST_UPLOAD_VERIFY:
        return run_post_upload_verify_tick(conn)
    if state == ClientState.CLEANUP_STAGING:
        return run_cleanup_staging_tick(conn, retain_staged_files=retain_staged_files)
    if state == ClientState.JOB_COMPLETE_REMOTE:
        return run_job_complete_remote_tick(conn)
    if state == ClientState.JOB_COMPLETE_LOCAL:
        return run_job_complete_local_tick(conn)
    if state == ClientState.IDLE:
        return {
            "handled": True,
            "progressed": False,
            "errored": False,
            "next_state": ClientState.IDLE.value,
        }

    now = datetime.now(UTC).isoformat()
    append_daemon_event(
        conn,
        level=EventLevel.INFO,
        category=EventCategory.TICK_NOOP,
        message=f"no tick handler for state={state}",
        created_at_utc=now,
        from_state=state,
        to_state=state,
    )
    conn.commit()
    return {
        "handled": False,
        "progressed": False,
        "errored": False,
        "state": state.value if state else None,
    }


def run_recovery_dispatch(
    conn,
    staging_root: Path,
    *,
    server_base_url: str = DEFAULT_SERVER_BASE_URL,
    client_id: str,
    client_display_name: str,
    bootstrap_token: str | None = None,
    retain_staged_files: bool = DEFAULT_RETAIN_STAGED_FILES,
    max_upload_retries: int = DEFAULT_MAX_UPLOAD_RETRIES,
    heartbeat_interval_seconds: int = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    max_steps: int = 1000,
) -> dict[str, object]:

    from .ingest import run_staging_copy_tick, run_hashing_tick, run_session_dedup_tick, run_local_dedup_tick
    from .upload import run_queue_upload_tick, run_wait_network_tick, run_upload_prepare_tick, run_upload_file_tick, run_server_verify_tick, run_reupload_or_quarantine_tick, run_post_upload_verify_tick
    from .cleanup import run_cleanup_staging_tick, run_job_complete_local_tick, run_job_complete_remote_tick, run_error_file_requeue
    """Drain implemented recovery phase work after bootstrap selection."""
    steps = 0
    progressed_steps = 0
    errored = False
    implemented_states = {
        ClientState.STAGING_COPY,
        ClientState.HASHING,
        ClientState.DEDUP_SESSION_SHA,
        ClientState.DEDUP_LOCAL_SHA,
        ClientState.QUEUE_UPLOAD,
        ClientState.JOB_COMPLETE_LOCAL,
    }

    while steps < max_steps:
        state = get_daemon_state(conn)
        if state not in implemented_states:
            now = datetime.now(UTC).isoformat()
            append_daemon_event(
                conn,
                level=EventLevel.INFO,
                category=EventCategory.RECOVERY_BOUNDARY_UNIMPLEMENTED,
                message=f"recovery dispatch stopped at boundary state={state}",
                created_at_utc=now,
                from_state=state,
                to_state=state,
            )
            conn.commit()
            break

        outcome = run_daemon_tick(
            conn,
            staging_root,
            server_base_url=server_base_url,
            client_id=client_id,
            client_display_name=client_display_name,
            bootstrap_token=bootstrap_token,
            retain_staged_files=retain_staged_files,
            max_upload_retries=max_upload_retries,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
        )
        steps += 1
        if outcome.get("progressed"):
            progressed_steps += 1

        if outcome.get("errored"):
            now = datetime.now(UTC).isoformat()
            append_daemon_event(
                conn,
                level=EventLevel.ERROR,
                category=EventCategory.RECOVERY_STOPPED_ERROR,
                message=f"recovery dispatch stopped on error in state={state}",
                created_at_utc=now,
                from_state=state,
                to_state=state,
            )
            conn.commit()
            errored = True
            break

        if not outcome.get("progressed"):
            break

    if steps >= max_steps:
        now = datetime.now(UTC).isoformat()
        append_daemon_event(
            conn,
            level=EventLevel.WARN,
            category=EventCategory.RECOVERY_DISPATCH_LIMIT,
            message=f"recovery dispatch hit max_steps={max_steps}",
            created_at_utc=now,
            from_state=get_daemon_state(conn),
            to_state=get_daemon_state(conn),
        )
        conn.commit()

    return {
        "steps": steps,
        "progressed_steps": progressed_steps,
        "errored": errored,
        "final_state": get_daemon_state(conn).value if get_daemon_state(conn) else None,
    }


def run_auto_progress_dispatch(
    conn,
    staging_root: Path,
    *,
    server_base_url: str = DEFAULT_SERVER_BASE_URL,
    client_id: str,
    client_display_name: str,
    bootstrap_token: str | None = None,
    retain_staged_files: bool = DEFAULT_RETAIN_STAGED_FILES,
    max_upload_retries: int = DEFAULT_MAX_UPLOAD_RETRIES,
    heartbeat_interval_seconds: int = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    max_steps: int = DEFAULT_AUTO_PROGRESS_MAX_STEPS,
) -> dict[str, object]:

    from .ingest import run_staging_copy_tick, run_hashing_tick, run_session_dedup_tick, run_local_dedup_tick
    from .upload import run_queue_upload_tick, run_wait_network_tick, run_upload_prepare_tick, run_upload_file_tick, run_server_verify_tick, run_reupload_or_quarantine_tick, run_post_upload_verify_tick
    from .cleanup import run_cleanup_staging_tick, run_job_complete_local_tick, run_job_complete_remote_tick, run_error_file_requeue
    """Bounded auto-drain for deterministic online/upload and completion states."""
    steps = 0
    progressed_steps = 0
    errored = False
    initial_state = get_daemon_state(conn)
    stop_reason = "boundary_state"

    while steps < max_steps:
        state = get_daemon_state(conn)
        if state not in AUTO_PROGRESS_SAFE_STATES:
            break

        outcome = run_daemon_tick(
            conn,
            staging_root,
            server_base_url=server_base_url,
            client_id=client_id,
            client_display_name=client_display_name,
            bootstrap_token=bootstrap_token,
            retain_staged_files=retain_staged_files,
            max_upload_retries=max_upload_retries,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
        )
        steps += 1
        if outcome.get("progressed"):
            progressed_steps += 1

        if outcome.get("errored"):
            stop_reason = "error"
            errored = True
            break

        if not outcome.get("progressed"):
            stop_reason = "no_progress"
            break

    if steps >= max_steps:
        stop_reason = "max_steps"

    final_state = get_daemon_state(conn)
    return {
        "steps": steps,
        "progressed_steps": progressed_steps,
        "errored": errored,
        "initial_state": initial_state.value if initial_state else None,
        "final_state": final_state.value if final_state else None,
        "stop_reason": stop_reason,
    }

__all__ = [k for k in list(globals().keys()) if not k.startswith("__")]
