"""Server-verify and post-upload phase handlers."""

import json
from datetime import UTC, datetime
from urllib.error import HTTPError, URLError

from photovault_clientd.db import (
    append_daemon_event,
    fetch_next_job_with_status,
    fetch_next_uploaded_file,
    fetch_reupload_target_file,
    mark_file_duplicate_global,
    mark_file_verified_remote,
    mark_ready_to_upload_error,
    mark_uploaded_for_reupload,
    mark_uploaded_retry,
    set_job_status,
    transition_daemon_state,
)
from photovault_clientd.events import EventCategory, EventLevel
from photovault_clientd.state_machine import ClientState

from . import core
from .upload_common import AUTH_BLOCKED_DETAILS, DEFAULT_MAX_UPLOAD_RETRIES, DEFAULT_SERVER_BASE_URL


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
        if auth_detail in AUTH_BLOCKED_DETAILS:
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
