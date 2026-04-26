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


def run_staging_copy_tick(conn, staging_root: Path) -> dict[str, object]:
    """Run one deterministic STAGING_COPY step and persist the resulting state."""
    now = datetime.now(UTC).isoformat()
    job_id = fetch_next_staging_job_with_pending_copy(conn)
    if job_id is None:
        pending_copy = count_pending_copy_files_global(conn)
        staged = count_staged_files_global(conn)
        hash_pending = count_hash_pending_files_global(conn)
        next_state = core._copy_phase_next_state(pending_copy, hash_pending)
        transition_daemon_state(
            conn,
            next_state,
            now,
            reason="staging tick found no copy candidates",
        )
        return {
            "handled": True,
            "progressed": False,
            "errored": False,
            "pending_copy": pending_copy,
            "staged": staged,
            "hash_pending": hash_pending,
            "next_state": next_state.value,
            "job_id": None,
        }

    candidate = fetch_next_copy_candidate(conn, job_id)
    if candidate is None:
        append_daemon_event(
            conn,
            level=EventLevel.WARN,
            category=EventCategory.STAGING_INCONSISTENT,
            message=f"job {job_id} selected but no copy candidate row found",
            created_at_utc=now,
            from_state=get_daemon_state(conn),
            to_state=get_daemon_state(conn),
        )
        conn.commit()
        return {
            "handled": True,
            "progressed": False,
            "errored": True,
            "pending_copy": count_pending_copy_files_global(conn),
            "staged": count_staged_files_global(conn),
            "hash_pending": count_hash_pending_files_global(conn),
            "next_state": get_daemon_state(conn).value if get_daemon_state(conn) else "UNKNOWN",
            "job_id": job_id,
        }

    file_id, source_path = candidate
    source = Path(source_path)
    if source.is_dir():
        try:
            directory_result = enumerate_directory_media_files(source)
            directory_files = directory_result.discovered_files
        except OSError as exc:
            retry_message = f"Directory source path could not be read: {source_path} ({exc})"
            mark_file_copy_retry(conn, file_id, retry_message, now)
            append_daemon_event(
                conn,
                level=EventLevel.ERROR,
                category=EventCategory.COPY_RETRY_SCHEDULED,
                message=f"COPY_IO_ERROR: file_id={file_id}, error={retry_message}",
                created_at_utc=now,
                from_state=ClientState.STAGING_COPY,
                to_state=ClientState.STAGING_COPY,
            )
            pending_copy = count_pending_copy_files_global(conn)
            staged = count_staged_files_global(conn)
            hash_pending = count_hash_pending_files_global(conn)
            next_state = core._copy_phase_next_state(pending_copy, hash_pending)
            set_job_status(conn, job_id, next_state.value, now)
            transition_daemon_state(
                conn,
                next_state,
                now,
                reason=f"staging tick directory read failed for file_id={file_id}; retry scheduled",
                commit=False,
            )
            conn.commit()
            return {
                "handled": True,
                "progressed": False,
                "errored": True,
                "error": retry_message,
                "pending_copy": pending_copy,
                "staged": staged,
                "hash_pending": hash_pending,
                "next_state": next_state.value,
                "job_id": job_id,
                "file_id": file_id,
            }

        if not directory_files:
            retry_message = f"Directory source path has no readable files: {source_path}"
            mark_file_copy_retry(conn, file_id, retry_message, now)
            append_daemon_event(
                conn,
                level=EventLevel.ERROR,
                category=EventCategory.COPY_RETRY_SCHEDULED,
                message=f"COPY_IO_ERROR: file_id={file_id}, error={retry_message}",
                created_at_utc=now,
                from_state=ClientState.STAGING_COPY,
                to_state=ClientState.STAGING_COPY,
            )
            pending_copy = count_pending_copy_files_global(conn)
            staged = count_staged_files_global(conn)
            hash_pending = count_hash_pending_files_global(conn)
            next_state = core._copy_phase_next_state(pending_copy, hash_pending)
            set_job_status(conn, job_id, next_state.value, now)
            transition_daemon_state(
                conn,
                next_state,
                now,
                reason=f"staging tick found empty directory source for file_id={file_id}; retry scheduled",
                commit=False,
            )
            conn.commit()
            return {
                "handled": True,
                "progressed": False,
                "errored": True,
                "error": retry_message,
                "pending_copy": pending_copy,
                "staged": staged,
                "hash_pending": hash_pending,
                "next_state": next_state.value,
                "job_id": job_id,
                "file_id": file_id,
            }

        replaced, inserted = replace_copy_candidate_with_discovered_files(
            conn,
            job_id=job_id,
            file_id=file_id,
            source_paths=directory_files,
            now_utc=now,
        )
        if not replaced:
            append_daemon_event(
                conn,
                level=EventLevel.WARN,
                category=EventCategory.STAGING_INCONSISTENT,
                message=f"unable to replace directory copy candidate file_id={file_id} for job_id={job_id}",
                created_at_utc=now,
                from_state=ClientState.STAGING_COPY,
                to_state=ClientState.STAGING_COPY,
            )
            conn.commit()
            return {
                "handled": True,
                "progressed": False,
                "errored": True,
                "pending_copy": count_pending_copy_files_global(conn),
                "staged": count_staged_files_global(conn),
                "hash_pending": count_hash_pending_files_global(conn),
                "next_state": get_daemon_state(conn).value if get_daemon_state(conn) else "UNKNOWN",
                "job_id": job_id,
                "file_id": file_id,
            }

        pending_copy = count_pending_copy_files_global(conn)
        staged = count_staged_files_global(conn)
        hash_pending = count_hash_pending_files_global(conn)
        next_state = core._copy_phase_next_state(pending_copy, hash_pending)
        set_job_status(conn, job_id, next_state.value, now)
        transition_daemon_state(
            conn,
            next_state,
            now,
            reason=f"staging tick expanded directory source file_id={file_id} into {inserted} file(s)",
            commit=False,
        )
        conn.commit()
        return {
            "handled": True,
            "progressed": True,
            "errored": False,
            "pending_copy": pending_copy,
            "staged": staged,
            "hash_pending": hash_pending,
            "next_state": next_state.value,
            "job_id": job_id,
            "file_id": file_id,
            "expanded_directory_source": source_path,
            "expanded_file_count": inserted,
            "filtered_file_count": directory_result.filtered_count,
        }

    staged_path = build_staged_path(staging_root, job_id, file_id, source_path)
    try:
        copied_size = copy_with_fsync(source_path, staged_path)
        mark_file_staged(conn, file_id, str(staged_path), copied_size, now)
        pending_copy = count_pending_copy_files_global(conn)
        staged = count_staged_files_global(conn)
        hash_pending = count_hash_pending_files_global(conn)
        next_state = core._copy_phase_next_state(pending_copy, hash_pending)
        set_job_status(conn, job_id, next_state.value, now)
        transition_daemon_state(
            conn,
            next_state,
            now,
            reason=f"staging tick copied file_id={file_id} for job_id={job_id}",
            commit=False,
        )
        conn.commit()
        return {
            "handled": True,
            "progressed": True,
            "errored": False,
            "pending_copy": pending_copy,
            "staged": staged,
            "hash_pending": hash_pending,
            "next_state": next_state.value,
            "job_id": job_id,
            "file_id": file_id,
            "staged_path": str(staged_path),
        }
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
        pending_copy = count_pending_copy_files_global(conn)
        staged = count_staged_files_global(conn)
        hash_pending = count_hash_pending_files_global(conn)
        next_state = core._copy_phase_next_state(pending_copy, hash_pending)
        set_job_status(conn, job_id, next_state.value, now)
        transition_daemon_state(
            conn,
            next_state,
            now,
            reason=f"staging tick copy failed for file_id={file_id}; retry scheduled",
            commit=False,
        )
        conn.commit()
        return {
            "handled": True,
            "progressed": False,
            "errored": True,
            "error": str(exc),
            "pending_copy": pending_copy,
            "staged": staged,
            "hash_pending": hash_pending,
            "next_state": next_state.value,
            "job_id": job_id,
            "file_id": file_id,
        }


def run_hashing_tick(conn) -> dict[str, object]:
    """Run one deterministic HASHING step and persist the resulting state."""
    now = datetime.now(UTC).isoformat()
    job_id = fetch_next_hashing_job_with_pending_hash(conn)
    if job_id is None:
        pending_copy = count_pending_copy_files_global(conn)
        hash_pending = count_hash_pending_files_global(conn)
        hashed_ready = count_hashed_files_global(conn)
        next_state = core._hash_phase_next_state(pending_copy, hash_pending, hashed_ready)
        transition_daemon_state(
            conn,
            next_state,
            now,
            reason="hashing tick found no hash candidates",
        )
        return {
            "handled": True,
            "progressed": False,
            "errored": False,
            "pending_copy": pending_copy,
            "hash_pending": hash_pending,
            "hashed_ready": hashed_ready,
            "next_state": next_state.value,
            "job_id": None,
        }

    candidate = fetch_next_hash_candidate(conn, job_id)
    if candidate is None:
        append_daemon_event(
            conn,
            level=EventLevel.WARN,
            category=EventCategory.HASHING_INCONSISTENT,
            message=f"job {job_id} selected but no hash candidate row found",
            created_at_utc=now,
            from_state=get_daemon_state(conn),
            to_state=get_daemon_state(conn),
        )
        conn.commit()
        return {
            "handled": True,
            "progressed": False,
            "errored": True,
            "pending_copy": count_pending_copy_files_global(conn),
            "hash_pending": count_hash_pending_files_global(conn),
            "hashed_ready": count_hashed_files_global(conn),
            "next_state": get_daemon_state(conn).value if get_daemon_state(conn) else "UNKNOWN",
            "job_id": job_id,
        }

    file_id, staged_path = candidate
    try:
        if not staged_path:
            raise FileNotFoundError(f"missing staged_path for file_id={file_id}")
        sha256_hex, _ = compute_sha256(Path(staged_path))
        mark_file_hashed(conn, file_id, sha256_hex, now)
        pending_copy = count_pending_copy_files_global(conn)
        hash_pending = count_hash_pending_files_global(conn)
        hashed_ready = count_hashed_files_global(conn)
        next_state = core._hash_phase_next_state(pending_copy, hash_pending, hashed_ready)
        set_job_status(conn, job_id, next_state.value, now)
        transition_daemon_state(
            conn,
            next_state,
            now,
            reason=f"hashing tick hashed file_id={file_id} for job_id={job_id}",
            commit=False,
        )
        conn.commit()
        return {
            "handled": True,
            "progressed": True,
            "errored": False,
            "pending_copy": pending_copy,
            "hash_pending": hash_pending,
            "hashed_ready": hashed_ready,
            "next_state": next_state.value,
            "job_id": job_id,
            "file_id": file_id,
            "sha256_hex": sha256_hex,
        }
    except OSError as exc:
        mark_file_hash_retry(conn, file_id, str(exc), now)
        append_daemon_event(
            conn,
            level=EventLevel.ERROR,
            category=EventCategory.HASH_RETRY_SCHEDULED,
            message=f"{classify_hash_error(exc).value}: file_id={file_id}, error={exc}",
            created_at_utc=now,
            from_state=ClientState.HASHING,
            to_state=ClientState.HASHING,
        )
        pending_copy = count_pending_copy_files_global(conn)
        hash_pending = count_hash_pending_files_global(conn)
        hashed_ready = count_hashed_files_global(conn)
        next_state = core._hash_phase_next_state(pending_copy, hash_pending, hashed_ready)
        set_job_status(conn, job_id, next_state.value, now)
        transition_daemon_state(
            conn,
            next_state,
            now,
            reason=f"hashing tick failed for file_id={file_id}; retry scheduled",
            commit=False,
        )
        conn.commit()
        return {
            "handled": True,
            "progressed": False,
            "errored": True,
            "error": str(exc),
            "pending_copy": pending_copy,
            "hash_pending": hash_pending,
            "hashed_ready": hashed_ready,
            "next_state": next_state.value,
            "job_id": job_id,
            "file_id": file_id,
        }


def run_session_dedup_tick(conn) -> dict[str, object]:
    """Apply same-job SHA deduplication and advance to local dedup."""
    now = datetime.now(UTC).isoformat()
    job_id = fetch_next_job_with_status(conn, ClientState.DEDUP_SESSION_SHA)
    if job_id is None:
        hashed_ready = count_hashed_files_global(conn)
        next_state = ClientState.DEDUP_SESSION_SHA if hashed_ready > 0 else ClientState.IDLE
        transition_daemon_state(
            conn,
            next_state,
            now,
            reason="session dedup tick found no jobs",
        )
        return {
            "handled": True,
            "progressed": False,
            "errored": False,
            "job_id": None,
            "hashed_ready": hashed_ready,
            "next_state": next_state.value,
        }

    hashed_rows = fetch_hashed_files_for_job(conn, job_id)
    duplicate_ids: list[int] = []
    canonical_count = 0
    for file_ids in core._hashed_groups(hashed_rows).values():
        canonical_count += 1
        if len(file_ids) > 1:
            duplicate_ids.extend(file_ids[1:])

    marked_duplicates = mark_files_duplicate_session(conn, duplicate_ids, now)
    set_job_status(conn, job_id, ClientState.DEDUP_LOCAL_SHA.value, now)
    transition_daemon_state(
        conn,
        ClientState.DEDUP_LOCAL_SHA,
        now,
        reason=f"session dedup completed for job_id={job_id}",
        commit=False,
    )
    append_daemon_event(
        conn,
        level=EventLevel.INFO,
        category=EventCategory.SESSION_DEDUP_APPLIED,
        message=(
            f"job_id={job_id}, canonical={canonical_count}, "
            f"duplicate_session={marked_duplicates}"
        ),
        created_at_utc=now,
        from_state=ClientState.DEDUP_SESSION_SHA,
        to_state=ClientState.DEDUP_LOCAL_SHA,
    )
    conn.commit()
    return {
        "handled": True,
        "progressed": True,
        "errored": False,
        "job_id": job_id,
        "canonical_count": canonical_count,
        "duplicate_session_count": marked_duplicates,
        "next_state": ClientState.DEDUP_LOCAL_SHA.value,
    }


def run_local_dedup_tick(conn) -> dict[str, object]:
    """Apply historical SHA deduplication against the local registry."""
    now = datetime.now(UTC).isoformat()
    job_id = fetch_next_job_with_status(conn, ClientState.DEDUP_LOCAL_SHA)
    if job_id is None:
        next_state = ClientState.IDLE
        transition_daemon_state(
            conn,
            next_state,
            now,
            reason="local dedup tick found no jobs",
        )
        return {
            "handled": True,
            "progressed": False,
            "errored": False,
            "job_id": None,
            "next_state": next_state.value,
        }

    hashed_rows = fetch_hashed_files_for_job(conn, job_id)
    duplicate_ids: list[int] = []
    for row in hashed_rows:
        sha256_hex = str(row["sha256_hex"])
        if local_sha_exists(conn, sha256_hex):
            duplicate_ids.append(int(row["file_id"]))

    marked_duplicates = mark_files_duplicate_local(conn, duplicate_ids, now)
    remaining_unique = count_hashed_files(conn, job_id)
    next_state = ClientState.QUEUE_UPLOAD if remaining_unique > 0 else ClientState.JOB_COMPLETE_LOCAL
    set_job_status(conn, job_id, next_state.value, now)
    transition_daemon_state(
        conn,
        next_state,
        now,
        reason=f"local dedup completed for job_id={job_id}",
        commit=False,
    )
    append_daemon_event(
        conn,
        level=EventLevel.INFO,
        category=EventCategory.LOCAL_DEDUP_APPLIED,
        message=f"job_id={job_id}, duplicate_local={marked_duplicates}, unique={remaining_unique}",
        created_at_utc=now,
        from_state=ClientState.DEDUP_LOCAL_SHA,
        to_state=next_state,
    )
    conn.commit()
    return {
        "handled": True,
        "progressed": True,
        "errored": False,
        "job_id": job_id,
        "duplicate_local_count": marked_duplicates,
        "unique_count": remaining_unique,
        "next_state": next_state.value,
    }
