"""Queue upload and wait-network phase handlers."""

from datetime import UTC, datetime

from photovault_clientd.db import (
    append_daemon_event,
    count_ready_to_upload_files,
    count_ready_to_upload_files_global,
    fetch_hashed_files_for_job,
    fetch_next_job_with_status,
    fetch_wait_network_retry_candidates,
    mark_files_ready_to_upload,
    register_local_sha,
    set_job_status,
    transition_daemon_state,
)
from photovault_clientd.events import EventCategory, EventLevel
from photovault_clientd.state_machine import ClientState, FileStatus

from . import core
from .upload_common import DEFAULT_SERVER_BASE_URL


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
