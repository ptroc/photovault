from . import core
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


def run_queue_upload_tick(conn) -> dict[str, object]:
    """Mark unique locally-ingested files as READY_TO_UPLOAD."""
    now = datetime.now(UTC).isoformat()
    job_id = fetch_next_job_with_status(conn, ClientState.QUEUE_UPLOAD)
    if job_id is None:
        ready_count = count_ready_to_upload_files_global(conn)
        next_state = ClientState.WAIT_NETWORK if ready_count > 0 else ClientState.IDLE
        transition_daemon_state(
            conn,
            next_state,
            now,
            reason="queue upload tick found no jobs",
        )
        return {
            "handled": True,
            "progressed": False,
            "errored": False,
            "job_id": None,
            "ready_to_upload": ready_count,
            "next_state": next_state.value,
        }

    hashed_rows = fetch_hashed_files_for_job(conn, job_id)
    file_ids = [int(row["file_id"]) for row in hashed_rows]
    for row in hashed_rows:
        register_local_sha(conn, str(row["sha256_hex"]), int(row["file_id"]), job_id, now)
    queued_count = mark_files_ready_to_upload(conn, file_ids, now)
    ready_count = count_ready_to_upload_files(conn, job_id)
    next_state = ClientState.WAIT_NETWORK if ready_count > 0 else ClientState.JOB_COMPLETE_LOCAL
    set_job_status(conn, job_id, next_state.value, now)
    transition_daemon_state(
        conn,
        next_state,
        now,
        reason=f"queue upload completed for job_id={job_id}",
        commit=False,
    )
    append_daemon_event(
        conn,
        level=EventLevel.INFO,
        category=EventCategory.QUEUE_UPLOAD_PREPARED,
        message=f"job_id={job_id}, ready_to_upload={queued_count}",
        created_at_utc=now,
        from_state=ClientState.QUEUE_UPLOAD,
        to_state=next_state,
    )
    conn.commit()
    return {
        "handled": True,
        "progressed": True,
        "errored": False,
        "job_id": job_id,
        "ready_to_upload": queued_count,
        "next_state": next_state.value,
    }


def run_wait_network_tick(
    conn,
    *,
    server_base_url: str = DEFAULT_SERVER_BASE_URL,
    client_id: str,
    client_display_name: str,
    bootstrap_token: str | None,
) -> dict[str, object]:
    """Advance from WAIT_NETWORK when retry backoff has elapsed."""
    now = datetime.now(UTC).isoformat()
    now_dt = datetime.fromisoformat(now)
    network_online = core._network_is_online()
    candidates = fetch_wait_network_retry_candidates(conn)
    if not candidates:
        core._set_client_request_auth_headers(None)
        transition_daemon_state(
            conn,
            ClientState.WAIT_NETWORK,
            now,
            reason="wait network tick found no upload work",
        )
        return {
            "handled": True,
            "progressed": False,
            "errored": False,
            "ready_to_upload": 0,
            "uploaded": 0,
            "network_online": network_online,
            "next_retry_at_utc": None,
            "next_state": ClientState.WAIT_NETWORK.value,
        }

    due_ready_count = 0
    due_uploaded_count = 0
    next_retry_at: datetime | None = None
    for row in candidates:
        retry_count = int(row["retry_count"])
        updated_at_utc = str(row["updated_at_utc"])
        due_at = core._retry_due_time(updated_at_utc, retry_count)
        if due_at <= now_dt:
            if row["status"] == FileStatus.UPLOADED.value:
                due_uploaded_count += 1
            else:
                due_ready_count += 1
            continue
        if next_retry_at is None or due_at < next_retry_at:
            next_retry_at = due_at

    if due_ready_count <= 0 and due_uploaded_count <= 0:
        core._set_client_request_auth_headers(None)
        transition_daemon_state(
            conn,
            ClientState.WAIT_NETWORK,
            now,
            reason="wait network backoff still active",
        )
        return {
            "handled": True,
            "progressed": False,
            "errored": False,
            "ready_to_upload": 0,
            "uploaded": 0,
            "network_online": network_online,
            "next_retry_at_utc": next_retry_at.isoformat() if next_retry_at else None,
            "next_state": ClientState.WAIT_NETWORK.value,
        }

    if not network_online:
        core._set_client_request_auth_headers(None)
        transition_daemon_state(
            conn,
            ClientState.WAIT_NETWORK,
            now,
            reason=(
                "wait network detected offline state with due retries "
                f"(ready={due_ready_count}, uploaded={due_uploaded_count})"
            ),
        )
        return {
            "handled": True,
            "progressed": False,
            "errored": False,
            "ready_to_upload": due_ready_count,
            "uploaded": due_uploaded_count,
            "network_online": network_online,
            "next_retry_at_utc": next_retry_at.isoformat() if next_retry_at else None,
            "next_state": ClientState.WAIT_NETWORK.value,
        }

    auth_headers, auth_block_reason = core._build_client_auth_headers(
        conn,
        server_base_url=server_base_url,
        client_id=client_id,
        display_name=client_display_name,
        bootstrap_token=bootstrap_token,
        now_utc=now,
    )
    if auth_headers is None:
        core._set_client_request_auth_headers(None)
        transition_daemon_state(
            conn,
            ClientState.WAIT_NETWORK,
            now,
            reason=f"wait network blocked by client auth state: {auth_block_reason}",
            commit=False,
        )
        append_daemon_event(
            conn,
            level=EventLevel.WARN,
            category="CLIENT_AUTH_BLOCKED",
            message=f"client auth blocked privileged upload work: {auth_block_reason}",
            created_at_utc=now,
            from_state=ClientState.WAIT_NETWORK,
            to_state=ClientState.WAIT_NETWORK,
        )
        conn.commit()
        return {
            "handled": True,
            "progressed": False,
            "errored": False,
            "ready_to_upload": due_ready_count,
            "uploaded": due_uploaded_count,
            "network_online": network_online,
            "auth_blocked": True,
            "auth_reason": auth_block_reason,
            "next_retry_at_utc": next_retry_at.isoformat() if next_retry_at else None,
            "next_state": ClientState.WAIT_NETWORK.value,
        }

    core._set_client_request_auth_headers(auth_headers)
    transition_daemon_state(
        conn,
        ClientState.UPLOAD_PREPARE,
        now,
        reason=(
            "wait network gate opened for "
            f"ready={due_ready_count}, uploaded={due_uploaded_count} due retries"
        ),
    )
    return {
        "handled": True,
        "progressed": True,
        "errored": False,
        "ready_to_upload": due_ready_count,
        "uploaded": due_uploaded_count,
        "network_online": network_online,
        "next_retry_at_utc": next_retry_at.isoformat() if next_retry_at else None,
        "next_state": ClientState.UPLOAD_PREPARE.value,
    }


def run_upload_prepare_tick(conn, *, server_base_url: str = DEFAULT_SERVER_BASE_URL) -> dict[str, object]:
    """Classify READY_TO_UPLOAD files via server metadata handshake."""
    now = datetime.now(UTC).isoformat()
    ready_rows = fetch_ready_to_upload_files_global(conn)
    if not ready_rows:
        next_state = (
            ClientState.SERVER_VERIFY
            if count_uploaded_files_global(conn) > 0
            else ClientState.WAIT_NETWORK
        )
        transition_daemon_state(
            conn,
            next_state,
            now,
            reason="upload prepare tick found no ready uploads",
        )
        return {
            "handled": True,
            "progressed": False,
            "errored": False,
            "ready_to_upload": 0,
            "next_state": next_state.value,
        }

    invalid_rows = [
        row
        for row in ready_rows
        if not isinstance(row.get("sha256_hex"), str)
        or len(str(row.get("sha256_hex"))) != 64
        or row.get("size_bytes") is None
    ]
    if invalid_rows:
        invalid_ids = [int(row["file_id"]) for row in invalid_rows]
        mark_files_upload_retry(
            conn,
            invalid_ids,
            "metadata handshake blocked: missing sha256_hex or size_bytes",
            now,
        )
        for row in invalid_rows:
            set_job_status(conn, int(row["job_id"]), ClientState.WAIT_NETWORK.value, now)
        transition_daemon_state(
            conn,
            ClientState.WAIT_NETWORK,
            now,
            reason="upload prepare blocked by invalid local metadata",
            commit=False,
        )
        append_daemon_event(
            conn,
            level=EventLevel.ERROR,
            category=EventCategory.HANDSHAKE_INVALID_RESPONSE,
            message=f"invalid local metadata for file_ids={invalid_ids}",
            created_at_utc=now,
            from_state=ClientState.UPLOAD_PREPARE,
            to_state=ClientState.WAIT_NETWORK,
        )
        conn.commit()
        return {
            "handled": True,
            "progressed": False,
            "errored": True,
            "retry_scheduled": len(invalid_ids),
            "next_state": ClientState.WAIT_NETWORK.value,
        }

    file_ids = [int(row["file_id"]) for row in ready_rows]
    try:
        decisions = core._post_metadata_handshake(server_base_url=server_base_url, files=ready_rows)
    except HTTPError as exc:
        auth_detail = core._update_auth_state_from_privileged_http_error(conn, now_utc=now, exc=exc)
        if auth_detail in _AUTH_BLOCKED_DETAILS:
            for row in ready_rows:
                set_job_status(conn, int(row["job_id"]), ClientState.WAIT_NETWORK.value, now)
            transition_daemon_state(
                conn,
                ClientState.WAIT_NETWORK,
                now,
                reason=f"upload prepare blocked by client auth: {auth_detail}",
                commit=False,
            )
            append_daemon_event(
                conn,
                level=EventLevel.WARN,
                category="CLIENT_AUTH_BLOCKED",
                message=f"metadata handshake blocked by server auth status: {auth_detail}",
                created_at_utc=now,
                from_state=ClientState.UPLOAD_PREPARE,
                to_state=ClientState.WAIT_NETWORK,
            )
            conn.commit()
            return {
                "handled": True,
                "progressed": False,
                "errored": False,
                "auth_blocked": True,
                "auth_reason": auth_detail,
                "next_state": ClientState.WAIT_NETWORK.value,
            }
        retried = mark_files_upload_retry(conn, file_ids, str(exc), now)
        for row in ready_rows:
            set_job_status(conn, int(row["job_id"]), ClientState.WAIT_NETWORK.value, now)
        transition_daemon_state(
            conn,
            ClientState.WAIT_NETWORK,
            now,
            reason=f"upload prepare handshake failed; retry scheduled for {retried} file(s)",
            commit=False,
        )
        append_daemon_event(
            conn,
            level=EventLevel.ERROR,
            category=EventCategory.HANDSHAKE_RETRY_SCHEDULED,
            message=f"metadata handshake failed: {exc}",
            created_at_utc=now,
            from_state=ClientState.UPLOAD_PREPARE,
            to_state=ClientState.WAIT_NETWORK,
        )
        conn.commit()
        return {
            "handled": True,
            "progressed": False,
            "errored": True,
            "retry_scheduled": retried,
            "next_state": ClientState.WAIT_NETWORK.value,
        }
    except (URLError, TimeoutError, ValueError, OSError, json.JSONDecodeError) as exc:
        retried = mark_files_upload_retry(conn, file_ids, str(exc), now)
        for row in ready_rows:
            set_job_status(conn, int(row["job_id"]), ClientState.WAIT_NETWORK.value, now)
        transition_daemon_state(
            conn,
            ClientState.WAIT_NETWORK,
            now,
            reason=f"upload prepare handshake failed; retry scheduled for {retried} file(s)",
            commit=False,
        )
        append_daemon_event(
            conn,
            level=EventLevel.ERROR,
            category=EventCategory.HANDSHAKE_RETRY_SCHEDULED,
            message=f"metadata handshake failed: {exc}",
            created_at_utc=now,
            from_state=ClientState.UPLOAD_PREPARE,
            to_state=ClientState.WAIT_NETWORK,
        )
        conn.commit()
        return {
            "handled": True,
            "progressed": False,
            "errored": True,
            "retry_scheduled": retried,
            "next_state": ClientState.WAIT_NETWORK.value,
        }

    missing_ids = [file_id for file_id in file_ids if file_id not in decisions]
    if missing_ids:
        retried = mark_files_upload_retry(
            conn,
            file_ids,
            f"metadata handshake missing decisions for file_ids={missing_ids}",
            now,
        )
        for row in ready_rows:
            set_job_status(conn, int(row["job_id"]), ClientState.WAIT_NETWORK.value, now)
        transition_daemon_state(
            conn,
            ClientState.WAIT_NETWORK,
            now,
            reason="upload prepare handshake response incomplete",
            commit=False,
        )
        append_daemon_event(
            conn,
            level=EventLevel.ERROR,
            category=EventCategory.HANDSHAKE_INVALID_RESPONSE,
            message=f"metadata handshake missing file_ids={missing_ids}",
            created_at_utc=now,
            from_state=ClientState.UPLOAD_PREPARE,
            to_state=ClientState.WAIT_NETWORK,
        )
        conn.commit()
        return {
            "handled": True,
            "progressed": False,
            "errored": True,
            "retry_scheduled": retried,
            "next_state": ClientState.WAIT_NETWORK.value,
        }

    already_exists_count = 0
    upload_required_count = 0
    already_exists_jobs: set[int] = set()
    upload_required_jobs: set[int] = set()
    for row in ready_rows:
        file_id = int(row["file_id"])
        job_id = int(row["job_id"])
        decision = decisions[file_id]
        if decision == "ALREADY_EXISTS":
            mark_file_duplicate_global(conn, file_id, now)
            already_exists_count += 1
            already_exists_jobs.add(job_id)
            continue
        clear_ready_to_upload_error(conn, file_id, now)
        upload_required_count += 1
        upload_required_jobs.add(job_id)

    for job_id in upload_required_jobs:
        set_job_status(conn, job_id, ClientState.UPLOAD_FILE.value, now)
    for job_id in already_exists_jobs:
        if job_id not in upload_required_jobs:
            set_job_status(conn, job_id, ClientState.POST_UPLOAD_VERIFY.value, now)

    if upload_required_count > 0:
        next_state = ClientState.UPLOAD_FILE
    elif already_exists_count > 0 or count_uploaded_files_global(conn) > 0:
        next_state = ClientState.SERVER_VERIFY
    else:
        next_state = ClientState.WAIT_NETWORK
    transition_daemon_state(
        conn,
        next_state,
        now,
        reason=(
            "upload prepare handshake classified "
            f"{already_exists_count} already-present and {upload_required_count} upload-required files"
        ),
        commit=False,
    )
    append_daemon_event(
        conn,
        level=EventLevel.INFO,
        category=EventCategory.HANDSHAKE_CLASSIFIED,
        message=(
            f"already_exists={already_exists_count}, upload_required={upload_required_count}, "
            f"total={len(ready_rows)}"
        ),
        created_at_utc=now,
        from_state=ClientState.UPLOAD_PREPARE,
        to_state=next_state,
    )
    conn.commit()
    return {
        "handled": True,
        "progressed": True,
        "errored": False,
        "already_exists": already_exists_count,
        "upload_required": upload_required_count,
        "next_state": next_state.value,
    }


def run_upload_file_tick(conn, *, server_base_url: str = DEFAULT_SERVER_BASE_URL) -> dict[str, object]:
    """Upload one READY_TO_UPLOAD file as a non-resumable full transfer."""
    now = datetime.now(UTC).isoformat()
    candidate = fetch_next_ready_to_upload_file(conn)
    if candidate is None:
        next_state = (
            ClientState.SERVER_VERIFY
            if count_uploaded_files_global(conn) > 0
            else ClientState.WAIT_NETWORK
        )
        transition_daemon_state(
            conn,
            next_state,
            now,
            reason="upload file tick found no ready files",
        )
        return {
            "handled": True,
            "progressed": False,
            "errored": False,
            "next_state": next_state.value,
        }

    file_id = int(candidate["file_id"])
    job_id = int(candidate["job_id"])
    staged_path = candidate.get("staged_path")
    sha256_hex = candidate.get("sha256_hex")
    size_bytes = candidate.get("size_bytes")
    source_path = candidate.get("source_path")
    job_name = candidate.get("job_name")
    if not isinstance(staged_path, str) or not staged_path:
        mark_ready_to_upload_retry(conn, file_id, "missing staged_path for upload", now)
        set_job_status(conn, job_id, ClientState.WAIT_NETWORK.value, now)
        transition_daemon_state(
            conn,
            ClientState.WAIT_NETWORK,
            now,
            reason=f"upload file blocked for file_id={file_id}: missing staged_path",
            commit=False,
        )
        append_daemon_event(
            conn,
            level=EventLevel.ERROR,
            category=EventCategory.UPLOAD_RETRY_SCHEDULED,
            message=f"missing staged_path for file_id={file_id}",
            created_at_utc=now,
            from_state=ClientState.UPLOAD_FILE,
            to_state=ClientState.WAIT_NETWORK,
        )
        conn.commit()
        return {
            "handled": True,
            "progressed": False,
            "errored": True,
            "file_id": file_id,
            "next_state": ClientState.WAIT_NETWORK.value,
        }
    if not isinstance(sha256_hex, str) or len(sha256_hex) != 64 or not isinstance(size_bytes, int):
        mark_ready_to_upload_retry(conn, file_id, "invalid upload metadata for file", now)
        set_job_status(conn, job_id, ClientState.WAIT_NETWORK.value, now)
        transition_daemon_state(
            conn,
            ClientState.WAIT_NETWORK,
            now,
            reason=f"upload file blocked for file_id={file_id}: invalid metadata",
            commit=False,
        )
        append_daemon_event(
            conn,
            level=EventLevel.ERROR,
            category=EventCategory.UPLOAD_RETRY_SCHEDULED,
            message=f"invalid metadata for file_id={file_id}",
            created_at_utc=now,
            from_state=ClientState.UPLOAD_FILE,
            to_state=ClientState.WAIT_NETWORK,
        )
        conn.commit()
        return {
            "handled": True,
            "progressed": False,
            "errored": True,
            "file_id": file_id,
            "next_state": ClientState.WAIT_NETWORK.value,
        }
    if not isinstance(source_path, str) or not source_path:
        mark_ready_to_upload_retry(conn, file_id, "missing source_path for upload", now)
        set_job_status(conn, job_id, ClientState.WAIT_NETWORK.value, now)
        transition_daemon_state(
            conn,
            ClientState.WAIT_NETWORK,
            now,
            reason=f"upload file blocked for file_id={file_id}: missing source_path",
            commit=False,
        )
        append_daemon_event(
            conn,
            level=EventLevel.ERROR,
            category=EventCategory.UPLOAD_RETRY_SCHEDULED,
            message=f"missing source_path for file_id={file_id}",
            created_at_utc=now,
            from_state=ClientState.UPLOAD_FILE,
            to_state=ClientState.WAIT_NETWORK,
        )
        conn.commit()
        return {
            "handled": True,
            "progressed": False,
            "errored": True,
            "file_id": file_id,
            "next_state": ClientState.WAIT_NETWORK.value,
        }
    if not isinstance(job_name, str) or not job_name:
        mark_ready_to_upload_retry(conn, file_id, "missing job_name for upload", now)
        set_job_status(conn, job_id, ClientState.WAIT_NETWORK.value, now)
        transition_daemon_state(
            conn,
            ClientState.WAIT_NETWORK,
            now,
            reason=f"upload file blocked for file_id={file_id}: missing job_name",
            commit=False,
        )
        append_daemon_event(
            conn,
            level=EventLevel.ERROR,
            category=EventCategory.UPLOAD_RETRY_SCHEDULED,
            message=f"missing job_name for file_id={file_id}",
            created_at_utc=now,
            from_state=ClientState.UPLOAD_FILE,
            to_state=ClientState.WAIT_NETWORK,
        )
        conn.commit()
        return {
            "handled": True,
            "progressed": False,
            "errored": True,
            "file_id": file_id,
            "next_state": ClientState.WAIT_NETWORK.value,
        }

    try:
        content = Path(staged_path).read_bytes()
        try:
            status = core._upload_file_content(
                server_base_url=server_base_url,
                sha256_hex=sha256_hex,
                size_bytes=size_bytes,
                content=content,
                job_name=job_name,
                original_filename=Path(source_path).name,
            )
        except TypeError as exc:
            if "unexpected keyword argument" not in str(exc):
                raise
            status = core._upload_file_content(
                server_base_url=server_base_url,
                sha256_hex=sha256_hex,
                size_bytes=size_bytes,
                content=content,
            )
    except HTTPError as exc:
        auth_detail = core._update_auth_state_from_privileged_http_error(conn, now_utc=now, exc=exc)
        if auth_detail in _AUTH_BLOCKED_DETAILS:
            set_job_status(conn, job_id, ClientState.WAIT_NETWORK.value, now)
            transition_daemon_state(
                conn,
                ClientState.WAIT_NETWORK,
                now,
                reason=f"upload file blocked by client auth: {auth_detail}",
                commit=False,
            )
            append_daemon_event(
                conn,
                level=EventLevel.WARN,
                category="CLIENT_AUTH_BLOCKED",
                message=f"file upload blocked by server auth status for file_id={file_id}: {auth_detail}",
                created_at_utc=now,
                from_state=ClientState.UPLOAD_FILE,
                to_state=ClientState.WAIT_NETWORK,
            )
            conn.commit()
            return {
                "handled": True,
                "progressed": False,
                "errored": False,
                "auth_blocked": True,
                "auth_reason": auth_detail,
                "file_id": file_id,
                "next_state": ClientState.WAIT_NETWORK.value,
            }
        mark_ready_to_upload_retry(conn, file_id, str(exc), now)
        set_job_status(conn, job_id, ClientState.WAIT_NETWORK.value, now)
        transition_daemon_state(
            conn,
            ClientState.WAIT_NETWORK,
            now,
            reason=f"upload file failed for file_id={file_id}; retry scheduled",
            commit=False,
        )
        append_daemon_event(
            conn,
            level=EventLevel.ERROR,
            category=EventCategory.UPLOAD_RETRY_SCHEDULED,
            message=f"upload failed for file_id={file_id}: {exc}",
            created_at_utc=now,
            from_state=ClientState.UPLOAD_FILE,
            to_state=ClientState.WAIT_NETWORK,
        )
        conn.commit()
        return {
            "handled": True,
            "progressed": False,
            "errored": True,
            "file_id": file_id,
            "next_state": ClientState.WAIT_NETWORK.value,
        }
    except (URLError, TimeoutError, ValueError, OSError, json.JSONDecodeError) as exc:
        mark_ready_to_upload_retry(conn, file_id, str(exc), now)
        set_job_status(conn, job_id, ClientState.WAIT_NETWORK.value, now)
        transition_daemon_state(
            conn,
            ClientState.WAIT_NETWORK,
            now,
            reason=f"upload file failed for file_id={file_id}; retry scheduled",
            commit=False,
        )
        append_daemon_event(
            conn,
            level=EventLevel.ERROR,
            category=EventCategory.UPLOAD_RETRY_SCHEDULED,
            message=f"upload failed for file_id={file_id}: {exc}",
            created_at_utc=now,
            from_state=ClientState.UPLOAD_FILE,
            to_state=ClientState.WAIT_NETWORK,
        )
        conn.commit()
        return {
            "handled": True,
            "progressed": False,
            "errored": True,
            "file_id": file_id,
            "next_state": ClientState.WAIT_NETWORK.value,
        }

    if status == "ALREADY_EXISTS":
        mark_file_duplicate_global(conn, file_id, now)
        set_job_status(conn, job_id, ClientState.POST_UPLOAD_VERIFY.value, now)
    else:
        mark_file_uploaded(conn, file_id, now)
        set_job_status(conn, job_id, ClientState.SERVER_VERIFY.value, now)

    next_state = ClientState.SERVER_VERIFY

    transition_daemon_state(
        conn,
        next_state,
        now,
        reason=f"upload file stored for file_id={file_id} with status={status}",
        commit=False,
    )
    append_daemon_event(
        conn,
        level=EventLevel.INFO,
        category=EventCategory.UPLOAD_FILE_STORED,
        message=f"file_id={file_id}, upload_status={status}",
        created_at_utc=now,
        from_state=ClientState.UPLOAD_FILE,
        to_state=next_state,
    )
    conn.commit()
    return {
        "handled": True,
        "progressed": True,
        "errored": False,
        "file_id": file_id,
        "upload_status": status,
        "next_state": next_state.value,
    }


def run_server_verify_tick(conn, *, server_base_url: str = DEFAULT_SERVER_BASE_URL) -> dict[str, object]:
    """Verify one UPLOADED file on the server and persist terminal/retry outcome."""
    now = datetime.now(UTC).isoformat()
    candidate = fetch_next_uploaded_file(conn)
    if candidate is None:
        next_state = (
            ClientState.POST_UPLOAD_VERIFY
            if fetch_next_job_with_status(conn, ClientState.POST_UPLOAD_VERIFY) is not None
            else ClientState.WAIT_NETWORK
        )
        transition_daemon_state(
            conn,
            next_state,
            now,
            reason="server verify tick found no uploaded files",
        )
        return {
            "handled": True,
            "progressed": False,
            "errored": False,
            "next_state": next_state.value,
        }

    file_id = int(candidate["file_id"])
    job_id = int(candidate["job_id"])
    sha256_hex = candidate.get("sha256_hex")
    size_bytes = candidate.get("size_bytes")
    if not isinstance(sha256_hex, str) or len(sha256_hex) != 64 or not isinstance(size_bytes, int):
        mark_uploaded_for_reupload(conn, file_id, "invalid verify metadata for uploaded file", now)
        set_job_status(conn, job_id, ClientState.WAIT_NETWORK.value, now)
        transition_daemon_state(
            conn,
            ClientState.WAIT_NETWORK,
            now,
            reason=f"server verify blocked for file_id={file_id}: invalid metadata",
            commit=False,
        )
        append_daemon_event(
            conn,
            level=EventLevel.ERROR,
            category=EventCategory.SERVER_VERIFY_RETRY_SCHEDULED,
            message=f"invalid verify metadata for file_id={file_id}",
            created_at_utc=now,
            from_state=ClientState.SERVER_VERIFY,
            to_state=ClientState.WAIT_NETWORK,
        )
        conn.commit()
        return {
            "handled": True,
            "progressed": False,
            "errored": True,
            "file_id": file_id,
            "next_state": ClientState.WAIT_NETWORK.value,
        }

    try:
        verify_status = core._post_server_verify(
            server_base_url=server_base_url,
            sha256_hex=sha256_hex,
            size_bytes=size_bytes,
        )
    except HTTPError as exc:
        auth_detail = core._update_auth_state_from_privileged_http_error(conn, now_utc=now, exc=exc)
        if auth_detail in _AUTH_BLOCKED_DETAILS:
            set_job_status(conn, job_id, ClientState.WAIT_NETWORK.value, now)
            transition_daemon_state(
                conn,
                ClientState.WAIT_NETWORK,
                now,
                reason=f"server verify blocked by client auth: {auth_detail}",
                commit=False,
            )
            append_daemon_event(
                conn,
                level=EventLevel.WARN,
                category="CLIENT_AUTH_BLOCKED",
                message=(
                    f"server verification blocked by auth status for file_id={file_id}: {auth_detail}"
                ),
                created_at_utc=now,
                from_state=ClientState.SERVER_VERIFY,
                to_state=ClientState.WAIT_NETWORK,
            )
            conn.commit()
            return {
                "handled": True,
                "progressed": False,
                "errored": False,
                "auth_blocked": True,
                "auth_reason": auth_detail,
                "file_id": file_id,
                "next_state": ClientState.WAIT_NETWORK.value,
            }
        mark_uploaded_retry(conn, file_id, str(exc), now)
        set_job_status(conn, job_id, ClientState.WAIT_NETWORK.value, now)
        transition_daemon_state(
            conn,
            ClientState.WAIT_NETWORK,
            now,
            reason=f"server verify failed for file_id={file_id}; retry scheduled",
            commit=False,
        )
        append_daemon_event(
            conn,
            level=EventLevel.ERROR,
            category=EventCategory.SERVER_VERIFY_RETRY_SCHEDULED,
            message=f"server verify failed for file_id={file_id}: {exc}",
            created_at_utc=now,
            from_state=ClientState.SERVER_VERIFY,
            to_state=ClientState.WAIT_NETWORK,
        )
        conn.commit()
        return {
            "handled": True,
            "progressed": False,
            "errored": True,
            "file_id": file_id,
            "next_state": ClientState.WAIT_NETWORK.value,
        }
    except (URLError, TimeoutError, ValueError, OSError, json.JSONDecodeError) as exc:
        mark_uploaded_retry(conn, file_id, str(exc), now)
        set_job_status(conn, job_id, ClientState.WAIT_NETWORK.value, now)
        transition_daemon_state(
            conn,
            ClientState.WAIT_NETWORK,
            now,
            reason=f"server verify failed for file_id={file_id}; retry scheduled",
            commit=False,
        )
        append_daemon_event(
            conn,
            level=EventLevel.ERROR,
            category=EventCategory.SERVER_VERIFY_RETRY_SCHEDULED,
            message=f"server verify failed for file_id={file_id}: {exc}",
            created_at_utc=now,
            from_state=ClientState.SERVER_VERIFY,
            to_state=ClientState.WAIT_NETWORK,
        )
        conn.commit()
        return {
            "handled": True,
            "progressed": False,
            "errored": True,
            "file_id": file_id,
            "next_state": ClientState.WAIT_NETWORK.value,
        }

    if verify_status == "VERIFY_FAILED":
        mark_uploaded_for_reupload(conn, file_id, "server verification failed", now)
        set_job_status(conn, job_id, ClientState.REUPLOAD_OR_QUARANTINE.value, now)
        next_state = ClientState.REUPLOAD_OR_QUARANTINE
    elif verify_status == "ALREADY_EXISTS":
        mark_file_duplicate_global(conn, file_id, now)
        set_job_status(conn, job_id, ClientState.POST_UPLOAD_VERIFY.value, now)
        next_state = ClientState.POST_UPLOAD_VERIFY
    else:
        mark_file_verified_remote(conn, file_id, now)
        set_job_status(conn, job_id, ClientState.POST_UPLOAD_VERIFY.value, now)
        next_state = ClientState.POST_UPLOAD_VERIFY

    transition_daemon_state(
        conn,
        next_state,
        now,
        reason=f"server verify completed for file_id={file_id} with status={verify_status}",
        commit=False,
    )
    append_daemon_event(
        conn,
        level=EventLevel.INFO,
        category=EventCategory.SERVER_VERIFY_COMPLETED,
        message=f"file_id={file_id}, verify_status={verify_status}",
        created_at_utc=now,
        from_state=ClientState.SERVER_VERIFY,
        to_state=next_state,
    )
    conn.commit()
    return {
        "handled": True,
        "progressed": True,
        "errored": False,
        "file_id": file_id,
        "verify_status": verify_status,
        "next_state": next_state.value,
    }


def run_reupload_or_quarantine_tick(
    conn,
    *,
    max_upload_retries: int = DEFAULT_MAX_UPLOAD_RETRIES,
) -> dict[str, object]:
    """Apply v1 reupload policy after server verify failure."""
    now = datetime.now(UTC).isoformat()
    job_id = fetch_next_job_with_status(conn, ClientState.REUPLOAD_OR_QUARANTINE)
    if job_id is None:
        transition_daemon_state(
            conn,
            ClientState.WAIT_NETWORK,
            now,
            reason="reupload tick found no reupload jobs",
        )
        return {
            "handled": True,
            "progressed": False,
            "errored": False,
            "next_state": ClientState.WAIT_NETWORK.value,
        }

    candidate = fetch_reupload_target_file(conn, job_id)
    if candidate is None:
        transition_daemon_state(
            conn,
            ClientState.WAIT_NETWORK,
            now,
            reason=f"reupload tick found no ready file for job_id={job_id}",
        )
        return {
            "handled": True,
            "progressed": False,
            "errored": True,
            "job_id": job_id,
            "next_state": ClientState.WAIT_NETWORK.value,
        }

    file_id = int(candidate["file_id"])
    retry_count = int(candidate["retry_count"])
    if retry_count >= max_upload_retries:
        mark_ready_to_upload_error(
            conn,
            file_id,
            (
                "server verification failed retries exhausted "
                f"(retry_count={retry_count}, max_retries={max_upload_retries})"
            ),
            now,
        )
        set_job_status(conn, job_id, ClientState.ERROR_FILE.value, now)
        transition_daemon_state(
            conn,
            ClientState.ERROR_FILE,
            now,
            reason=f"reupload exhausted for file_id={file_id}",
            commit=False,
        )
        append_daemon_event(
            conn,
            level=EventLevel.ERROR,
            category=EventCategory.SERVER_VERIFY_RETRY_SCHEDULED,
            message=(
                f"reupload exhausted for file_id={file_id} "
                f"(retry_count={retry_count}, max_retries={max_upload_retries})"
            ),
            created_at_utc=now,
            from_state=ClientState.REUPLOAD_OR_QUARANTINE,
            to_state=ClientState.ERROR_FILE,
        )
        conn.commit()
        return {
            "handled": True,
            "progressed": True,
            "errored": True,
            "file_id": file_id,
            "retry_count": retry_count,
            "max_retries": max_upload_retries,
            "next_state": ClientState.ERROR_FILE.value,
        }

    set_job_status(conn, job_id, ClientState.WAIT_NETWORK.value, now)
    transition_daemon_state(
        conn,
        ClientState.WAIT_NETWORK,
        now,
        reason=f"reupload scheduled for file_id={file_id}",
        commit=False,
    )
    append_daemon_event(
        conn,
        level=EventLevel.INFO,
        category=EventCategory.SERVER_VERIFY_RETRY_SCHEDULED,
        message=f"reupload scheduled for file_id={file_id}",
        created_at_utc=now,
        from_state=ClientState.REUPLOAD_OR_QUARANTINE,
        to_state=ClientState.WAIT_NETWORK,
    )
    conn.commit()
    return {
        "handled": True,
        "progressed": True,
        "errored": False,
        "file_id": file_id,
        "next_state": ClientState.WAIT_NETWORK.value,
    }


def run_post_upload_verify_tick(conn) -> dict[str, object]:
    """Apply v1 post-upload policy and advance to cleanup staging."""
    now = datetime.now(UTC).isoformat()
    job_id = fetch_next_job_with_status(conn, ClientState.POST_UPLOAD_VERIFY)
    if job_id is None:
        next_state = ClientState.WAIT_NETWORK
        transition_daemon_state(
            conn,
            next_state,
            now,
            reason="post upload verify tick found no jobs",
        )
        return {
            "handled": True,
            "progressed": False,
            "errored": False,
            "job_id": None,
            "next_state": next_state.value,
        }

    set_job_status(conn, job_id, ClientState.CLEANUP_STAGING.value, now)
    transition_daemon_state(
        conn,
        ClientState.CLEANUP_STAGING,
        now,
        reason=f"post upload verify completed for job_id={job_id}",
        commit=False,
    )
    append_daemon_event(
        conn,
        level=EventLevel.INFO,
        category=EventCategory.POST_UPLOAD_VERIFY_COMPLETED,
        message=f"job_id={job_id}, policy=pass_through_v1",
        created_at_utc=now,
        from_state=ClientState.POST_UPLOAD_VERIFY,
        to_state=ClientState.CLEANUP_STAGING,
    )
    conn.commit()
    return {
        "handled": True,
        "progressed": True,
        "errored": False,
        "job_id": job_id,
        "next_state": ClientState.CLEANUP_STAGING.value,
    }
