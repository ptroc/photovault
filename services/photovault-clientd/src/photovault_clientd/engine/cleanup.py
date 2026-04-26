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


def run_job_complete_local_tick(conn) -> dict[str, object]:
    """Finalize a locally completed job and return the daemon to IDLE."""
    now = datetime.now(UTC).isoformat()
    job_id = fetch_next_job_with_status(conn, ClientState.JOB_COMPLETE_LOCAL)
    if job_id is None:
        transition_daemon_state(
            conn,
            ClientState.IDLE,
            now,
            reason="job complete local tick found no jobs",
        )
        return {
            "handled": True,
            "progressed": False,
            "errored": False,
            "job_id": None,
            "next_state": ClientState.IDLE.value,
        }

    transition_daemon_state(
        conn,
        ClientState.IDLE,
        now,
        reason=f"local ingest finalized for job_id={job_id}",
        commit=False,
    )
    append_daemon_event(
        conn,
        level=EventLevel.INFO,
        category=EventCategory.JOB_LOCAL_COMPLETED,
        message=f"job_id={job_id} finalized locally with no upload work pending",
        created_at_utc=now,
        from_state=ClientState.JOB_COMPLETE_LOCAL,
        to_state=ClientState.IDLE,
    )
    conn.commit()
    return {
        "handled": True,
        "progressed": True,
        "errored": False,
        "job_id": job_id,
        "next_state": ClientState.IDLE.value,
        }


def run_error_file_requeue(
    conn,
    *,
    file_id: int,
) -> dict[str, object]:
    """Operator action: requeue an ERROR_FILE upload back to READY_TO_UPLOAD."""
    now = datetime.now(UTC).isoformat()
    row = conn.execute(
        """
        SELECT id, job_id
        FROM ingest_files
        WHERE id = ? AND status = ?;
        """,
        (file_id, FileStatus.ERROR_FILE.value),
    ).fetchone()
    if row is None:
        return {
            "handled": False,
            "progressed": False,
            "errored": True,
            "file_id": file_id,
            "error": "file not in ERROR_FILE",
        }

    job_id = int(row[1])
    updated = requeue_error_file_for_upload(conn, file_id, now)
    if updated <= 0:
        return {
            "handled": False,
            "progressed": False,
            "errored": True,
            "file_id": file_id,
            "error": "requeue update failed",
        }

    set_job_status(conn, job_id, ClientState.WAIT_NETWORK.value, now)
    transition_daemon_state(
        conn,
        ClientState.UPLOAD_PREPARE,
        now,
        reason=f"operator requeued error file_id={file_id}",
        commit=False,
    )
    append_daemon_event(
        conn,
        level=EventLevel.INFO,
        category=EventCategory.SERVER_VERIFY_RETRY_SCHEDULED,
        message=f"operator requeued file_id={file_id}",
        created_at_utc=now,
        from_state=ClientState.ERROR_FILE,
        to_state=ClientState.UPLOAD_PREPARE,
    )
    conn.commit()
    return {
        "handled": True,
        "progressed": True,
        "errored": False,
        "file_id": file_id,
        "job_id": job_id,
        "next_state": ClientState.UPLOAD_PREPARE.value,
    }


def run_cleanup_staging_tick(
    conn, *, retain_staged_files: bool = DEFAULT_RETAIN_STAGED_FILES
) -> dict[str, object]:
    """Apply v1 cleanup policy for terminal remote files and advance job status."""
    now = datetime.now(UTC).isoformat()
    job_id = fetch_next_job_with_status(conn, ClientState.CLEANUP_STAGING)
    if job_id is None:
        next_state = ClientState.WAIT_NETWORK
        transition_daemon_state(
            conn,
            next_state,
            now,
            reason="cleanup staging tick found no jobs",
        )
        return {
            "handled": True,
            "progressed": False,
            "errored": False,
            "job_id": None,
            "next_state": next_state.value,
        }

    remote_terminal_files = fetch_cleanup_remote_terminal_files(conn, job_id)
    remote_terminal_count = len(remote_terminal_files)
    deleted_count = 0
    retained_count = 0
    if retain_staged_files:
        retained_count = remote_terminal_count
    else:
        for row in remote_terminal_files:
            staged_path = row.get("staged_path")
            if not isinstance(staged_path, str) or not staged_path:
                continue
            path = Path(staged_path)
            try:
                if path.exists():
                    path.unlink()
                    deleted_count += 1
            except OSError as exc:
                set_job_status(conn, job_id, ClientState.PAUSED_STORAGE.value, now)
                transition_daemon_state(
                    conn,
                    ClientState.PAUSED_STORAGE,
                    now,
                    reason=f"cleanup delete failed for job_id={job_id}",
                    commit=False,
                )
                append_daemon_event(
                    conn,
                    level=EventLevel.ERROR,
                    category=EventCategory.CLEANUP_STAGING_APPLIED,
                    message=f"cleanup delete failed for file_id={row['file_id']}: {exc}",
                    created_at_utc=now,
                    from_state=ClientState.CLEANUP_STAGING,
                    to_state=ClientState.PAUSED_STORAGE,
                )
                conn.commit()
                return {
                    "handled": True,
                    "progressed": True,
                    "errored": True,
                    "job_id": job_id,
                    "remote_terminal_count": remote_terminal_count,
                    "deleted_count": deleted_count,
                    "retained_count": retained_count,
                    "next_state": ClientState.PAUSED_STORAGE.value,
                }
    pending_upload_count = count_job_files_by_statuses(
        conn,
        job_id,
        (FileStatus.READY_TO_UPLOAD.value, FileStatus.UPLOADED.value),
    )
    non_terminal_count = count_non_terminal_files_for_job(conn, job_id)

    if pending_upload_count > 0:
        set_job_status(conn, job_id, ClientState.UPLOAD_PREPARE.value, now)
        next_state = ClientState.UPLOAD_PREPARE
    elif non_terminal_count == 0:
        set_job_status(conn, job_id, ClientState.JOB_COMPLETE_REMOTE.value, now)
        next_state = ClientState.JOB_COMPLETE_REMOTE
    else:
        set_job_status(conn, job_id, ClientState.UPLOAD_PREPARE.value, now)
        next_state = ClientState.UPLOAD_PREPARE

    transition_daemon_state(
        conn,
        next_state,
        now,
        reason=f"cleanup staging completed for job_id={job_id}",
        commit=False,
    )
    append_daemon_event(
        conn,
        level=EventLevel.INFO,
        category=EventCategory.CLEANUP_STAGING_APPLIED,
        message=(
            f"job_id={job_id}, retain_staged_files={retain_staged_files}, "
            f"remote_terminal={remote_terminal_count}, pending_upload={pending_upload_count}, "
            f"deleted={deleted_count}, retained={retained_count}"
        ),
        created_at_utc=now,
        from_state=ClientState.CLEANUP_STAGING,
        to_state=next_state,
    )
    conn.commit()
    return {
        "handled": True,
        "progressed": True,
        "errored": False,
        "job_id": job_id,
        "remote_terminal_count": remote_terminal_count,
        "deleted_count": deleted_count,
        "retained_count": retained_count,
        "next_state": next_state.value,
    }


def run_job_complete_remote_tick(conn) -> dict[str, object]:
    """Finalize a remotely-completed job into local completion flow."""
    now = datetime.now(UTC).isoformat()
    job_id = fetch_next_job_with_status(conn, ClientState.JOB_COMPLETE_REMOTE)
    if job_id is None:
        next_state = ClientState.WAIT_NETWORK
        transition_daemon_state(
            conn,
            next_state,
            now,
            reason="job complete remote tick found no jobs",
        )
        return {
            "handled": True,
            "progressed": False,
            "errored": False,
            "job_id": None,
            "next_state": next_state.value,
        }

    set_job_status(conn, job_id, ClientState.JOB_COMPLETE_LOCAL.value, now)
    transition_daemon_state(
        conn,
        ClientState.JOB_COMPLETE_LOCAL,
        now,
        reason=f"remote completion finalized for job_id={job_id}",
        commit=False,
    )
    append_daemon_event(
        conn,
        level=EventLevel.INFO,
        category=EventCategory.JOB_REMOTE_COMPLETED,
        message=f"job_id={job_id} transitioned to JOB_COMPLETE_LOCAL",
        created_at_utc=now,
        from_state=ClientState.JOB_COMPLETE_REMOTE,
        to_state=ClientState.JOB_COMPLETE_LOCAL,
    )
    conn.commit()
    return {
        "handled": True,
        "progressed": True,
        "errored": False,
        "job_id": job_id,
        "next_state": ClientState.JOB_COMPLETE_LOCAL.value,
    }
