"""Single-thread daemon tick and recovery helpers for photovault-clientd."""

import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from photovault_clientd.db import (
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
    fetch_hashed_files_for_job,
    fetch_next_copy_candidate,
    fetch_next_hash_candidate,
    fetch_next_hashing_job_with_pending_hash,
    fetch_next_job_with_status,
    fetch_next_ready_to_upload_file,
    fetch_next_staging_job_with_pending_copy,
    fetch_next_uploaded_file,
    fetch_ready_to_upload_files_global,
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
    set_job_status,
    transition_daemon_state,
)
from photovault_clientd.events import EventCategory, EventLevel, classify_copy_error, classify_hash_error
from photovault_clientd.hashing import compute_sha256
from photovault_clientd.state_machine import ClientState, FileStatus
from photovault_clientd.storage import build_staged_path, copy_with_fsync

DEFAULT_SERVER_BASE_URL = "http://127.0.0.1:9301"
DEFAULT_HANDSHAKE_TIMEOUT_SECONDS = 5.0
DEFAULT_RETAIN_STAGED_FILES = True
DEFAULT_MAX_UPLOAD_RETRIES = 3


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
        headers={"Content-Type": "application/json"},
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
    timeout_seconds: float = DEFAULT_HANDSHAKE_TIMEOUT_SECONDS,
) -> str:
    request = Request(
        url=f"{server_base_url.rstrip('/')}/v1/upload/content/{sha256_hex}",
        data=content,
        headers={"x-size-bytes": str(size_bytes)},
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
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=timeout_seconds) as response:
        body = json.loads(response.read().decode("utf-8"))

    status = body.get("status")
    if status not in {"VERIFIED", "ALREADY_EXISTS", "VERIFY_FAILED"}:
        raise ValueError("verify response has invalid status")
    return str(status)


def run_staging_copy_tick(conn, staging_root: Path) -> dict[str, object]:
    """Run one deterministic STAGING_COPY step and persist the resulting state."""
    now = datetime.now(UTC).isoformat()
    job_id = fetch_next_staging_job_with_pending_copy(conn)
    if job_id is None:
        pending_copy = count_pending_copy_files_global(conn)
        staged = count_staged_files_global(conn)
        hash_pending = count_hash_pending_files_global(conn)
        next_state = _copy_phase_next_state(pending_copy, hash_pending)
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
    staged_path = build_staged_path(staging_root, job_id, file_id, source_path)
    try:
        copied_size = copy_with_fsync(source_path, staged_path)
        mark_file_staged(conn, file_id, str(staged_path), copied_size, now)
        pending_copy = count_pending_copy_files_global(conn)
        staged = count_staged_files_global(conn)
        hash_pending = count_hash_pending_files_global(conn)
        next_state = _copy_phase_next_state(pending_copy, hash_pending)
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
        next_state = _copy_phase_next_state(pending_copy, hash_pending)
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
        next_state = _hash_phase_next_state(pending_copy, hash_pending, hashed_ready)
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
        next_state = _hash_phase_next_state(pending_copy, hash_pending, hashed_ready)
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
        next_state = _hash_phase_next_state(pending_copy, hash_pending, hashed_ready)
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
    for file_ids in _hashed_groups(hashed_rows).values():
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


def run_wait_network_tick(conn) -> dict[str, object]:
    """Advance from WAIT_NETWORK to UPLOAD_PREPARE when ready files exist."""
    now = datetime.now(UTC).isoformat()
    ready_rows = fetch_ready_to_upload_files_global(conn)
    if not ready_rows:
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
            "next_state": ClientState.WAIT_NETWORK.value,
        }

    transition_daemon_state(
        conn,
        ClientState.UPLOAD_PREPARE,
        now,
        reason=f"wait network gate opened for {len(ready_rows)} ready files",
    )
    return {
        "handled": True,
        "progressed": True,
        "errored": False,
        "ready_to_upload": len(ready_rows),
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
        decisions = _post_metadata_handshake(server_base_url=server_base_url, files=ready_rows)
    except (HTTPError, URLError, TimeoutError, ValueError, OSError, json.JSONDecodeError) as exc:
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

    try:
        content = Path(staged_path).read_bytes()
        status = _upload_file_content(
            server_base_url=server_base_url,
            sha256_hex=sha256_hex,
            size_bytes=size_bytes,
            content=content,
        )
    except (HTTPError, URLError, TimeoutError, ValueError, OSError, json.JSONDecodeError) as exc:
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
        verify_status = _post_server_verify(
            server_base_url=server_base_url,
            sha256_hex=sha256_hex,
            size_bytes=size_bytes,
        )
    except (HTTPError, URLError, TimeoutError, ValueError, OSError, json.JSONDecodeError) as exc:
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
    candidate = fetch_next_ready_to_upload_file(conn)
    if candidate is None:
        transition_daemon_state(
            conn,
            ClientState.WAIT_NETWORK,
            now,
            reason="reupload tick found no ready files",
        )
        return {
            "handled": True,
            "progressed": False,
            "errored": False,
            "next_state": ClientState.WAIT_NETWORK.value,
        }

    file_id = int(candidate["file_id"])
    job_id = int(candidate["job_id"])
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

    remote_terminal_count = count_job_files_by_statuses(
        conn,
        job_id,
        (FileStatus.VERIFIED_REMOTE.value, FileStatus.DUPLICATE_SHA_GLOBAL.value),
    )
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
            f"remote_terminal={remote_terminal_count}, pending_upload={pending_upload_count}"
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


def run_daemon_tick(
    conn,
    staging_root: Path,
    *,
    server_base_url: str = DEFAULT_SERVER_BASE_URL,
    retain_staged_files: bool = DEFAULT_RETAIN_STAGED_FILES,
    max_upload_retries: int = DEFAULT_MAX_UPLOAD_RETRIES,
) -> dict[str, object]:
    """Run one daemon tick for the current state."""
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
        return run_wait_network_tick(conn)
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
    retain_staged_files: bool = DEFAULT_RETAIN_STAGED_FILES,
    max_upload_retries: int = DEFAULT_MAX_UPLOAD_RETRIES,
    max_steps: int = 1000,
) -> dict[str, object]:
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
            retain_staged_files=retain_staged_files,
            max_upload_retries=max_upload_retries,
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
