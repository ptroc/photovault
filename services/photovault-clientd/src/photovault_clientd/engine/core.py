"""Single-thread daemon tick and recovery helpers for photovault-clientd."""

import subprocess
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError as CoreHTTPError
from urllib.error import URLError as CoreURLError
from urllib.request import Request as CoreRequest
from urllib.request import urlopen as core_urlopen

from photovault_clientd.db import (
    append_daemon_event,
    count_ready_to_upload_files_global,
    count_uploaded_files_global,
    fetch_next_job_with_status,
    get_daemon_state,
)
from photovault_clientd.events import EventCategory, EventLevel
from photovault_clientd.networking import parse_nmcli_multiline
from photovault_clientd.state_machine import ClientState

from . import heartbeat, http_helpers

DEFAULT_SERVER_BASE_URL = "http://127.0.0.1:9301"
DEFAULT_HANDSHAKE_TIMEOUT_SECONDS = http_helpers.DEFAULT_HANDSHAKE_TIMEOUT_SECONDS
DEFAULT_ENROLL_TIMEOUT_SECONDS = http_helpers.DEFAULT_ENROLL_TIMEOUT_SECONDS
DEFAULT_RETAIN_STAGED_FILES = True
DEFAULT_MAX_UPLOAD_RETRIES = 3
DEFAULT_AUTO_PROGRESS_MAX_STEPS = 32
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = heartbeat.DEFAULT_HEARTBEAT_INTERVAL_SECONDS

Request = CoreRequest
HTTPError = CoreHTTPError
URLError = CoreURLError
urlopen = core_urlopen

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


def _retry_backoff_seconds(retry_count: int) -> int:
    return heartbeat._retry_backoff_seconds(retry_count)


def _retry_due_time(updated_at_utc: str, retry_count: int) -> datetime:
    return heartbeat._retry_due_time(updated_at_utc, retry_count)


def _set_client_request_auth_headers(headers: dict[str, str] | None) -> None:
    http_helpers._set_client_request_auth_headers(headers)


def _extract_http_error_detail(exc: CoreHTTPError) -> str:
    return http_helpers._extract_http_error_detail(exc)


def _build_client_auth_headers(
    conn,
    *,
    server_base_url: str,
    client_id: str,
    display_name: str,
    bootstrap_token: str | None,
    now_utc: str,
) -> tuple[dict[str, str] | None, str | None]:
    return http_helpers._build_client_auth_headers(
        conn,
        server_base_url=server_base_url,
        client_id=client_id,
        display_name=display_name,
        bootstrap_token=bootstrap_token,
        now_utc=now_utc,
    )


def _update_auth_state_from_privileged_http_error(
    conn,
    *,
    now_utc: str,
    exc: CoreHTTPError,
) -> str | None:
    return http_helpers._update_auth_state_from_privileged_http_error(
        conn,
        now_utc=now_utc,
        exc=exc,
    )


def _post_metadata_handshake(
    *,
    server_base_url: str,
    files: list[dict[str, object]],
    timeout_seconds: float = DEFAULT_HANDSHAKE_TIMEOUT_SECONDS,
) -> dict[int, str]:
    return http_helpers._post_metadata_handshake(
        server_base_url=server_base_url,
        files=files,
        timeout_seconds=timeout_seconds,
    )


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
    return http_helpers._upload_file_content(
        server_base_url=server_base_url,
        sha256_hex=sha256_hex,
        size_bytes=size_bytes,
        content=content,
        job_name=job_name,
        original_filename=original_filename,
        timeout_seconds=timeout_seconds,
    )


def _post_server_verify(
    *,
    server_base_url: str,
    sha256_hex: str,
    size_bytes: int,
    timeout_seconds: float = DEFAULT_HANDSHAKE_TIMEOUT_SECONDS,
) -> str:
    return http_helpers._post_server_verify(
        server_base_url=server_base_url,
        sha256_hex=sha256_hex,
        size_bytes=size_bytes,
        timeout_seconds=timeout_seconds,
    )


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
    """Run one daemon tick for the current state."""

    from .cleanup import (
        run_cleanup_staging_tick,
        run_job_complete_local_tick,
        run_job_complete_remote_tick,
    )
    from .ingest import (
        run_hashing_tick,
        run_local_dedup_tick,
        run_session_dedup_tick,
        run_staging_copy_tick,
    )
    from .upload import (
        run_post_upload_verify_tick,
        run_queue_upload_tick,
        run_reupload_or_quarantine_tick,
        run_server_verify_tick,
        run_upload_file_tick,
        run_upload_prepare_tick,
        run_wait_network_tick,
    )

    heartbeat.run_client_heartbeat_tick(
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
