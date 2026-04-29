"""Upload prepare and file-transfer phase handlers."""

import json
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError

from photovault_clientd.db import (
    append_daemon_event,
    clear_ready_to_upload_error,
    count_uploaded_files_global,
    fetch_next_ready_to_upload_file,
    fetch_ready_to_upload_files_global,
    mark_file_duplicate_global,
    mark_file_uploaded,
    mark_files_upload_retry,
    mark_ready_to_upload_retry,
    set_job_status,
    transition_daemon_state,
)
from photovault_clientd.events import EventCategory, EventLevel
from photovault_clientd.state_machine import ClientState

from . import core
from .upload_common import AUTH_BLOCKED_DETAILS, DEFAULT_SERVER_BASE_URL


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
        if auth_detail in AUTH_BLOCKED_DETAILS:
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
        if auth_detail in AUTH_BLOCKED_DETAILS:
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

