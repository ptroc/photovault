# ruff: noqa: E501
"""Formatting and view models for the UI."""
from typing import Any

# Constants
_REMOTE_ALREADY_EXISTS_STATUSES = {"DUPLICATE_SHA_GLOBAL"}
_UPLOAD_REQUIRED_STATUSES = {
    "READY_TO_UPLOAD",
    "UPLOADED",
    "VERIFY_RUNNING",
    "VERIFIED_REMOTE",
    "ERROR_FILE",
}
_REMOTE_TERMINAL_STATUSES = {"VERIFIED_REMOTE", "DUPLICATE_SHA_GLOBAL"}
_FILE_TERMINAL_STATUSES = {
    "VERIFIED_REMOTE",
    "DUPLICATE_SHA_GLOBAL",
    "DUPLICATE_SHA_LOCAL",
    "DUPLICATE_SESSION_SHA",
    "ERROR_FILE",
    "QUARANTINED_LOCAL",
}
_IGNORED_FILE_STATUSES = {"DUPLICATE_SHA_GLOBAL", "DUPLICATE_SHA_LOCAL", "DUPLICATE_SESSION_SHA"}
_ERROR_FILE_STATUSES = {"ERROR_FILE", "QUARANTINED_LOCAL"}
_PAUSED_ERROR_JOB_STATUSES = {"ERROR_FILE", "ERROR_JOB", "PAUSED_STORAGE"}
_REMOTE_COMPLETE_JOB_STATUSES = {"JOB_COMPLETE_REMOTE", "JOB_COMPLETE_LOCAL"}
_ACTIVE_DAEMON_STATES = {
    "STAGING_COPY", "HASHING", "DEDUP_SESSION_SHA", "DEDUP_LOCAL_SHA", 
    "QUEUE_UPLOAD", "UPLOAD_PREPARE", "UPLOAD_FILE", "SERVER_VERIFY", 
    "REUPLOAD_OR_QUARANTINE", "POST_UPLOAD_VERIFY", "CLEANUP_STAGING", 
    "VERIFY_IDLE", "VERIFY_HASH",
}
_AUTO_PROGRESS_DAEMON_STATES = {
    "WAIT_NETWORK", "UPLOAD_PREPARE", "UPLOAD_FILE", "SERVER_VERIFY", 
    "REUPLOAD_OR_QUARANTINE", "POST_UPLOAD_VERIFY", "CLEANUP_STAGING", 
    "JOB_COMPLETE_REMOTE", "JOB_COMPLETE_LOCAL",
}
_WAITING_DAEMON_STATES = {"WAIT_NETWORK", "WAIT_MEDIA"}
_BLOCKED_DAEMON_STATES = {"ERROR_DAEMON", "ERROR_JOB", "PAUSED_STORAGE"}
_M2_PHASE_LABELS = {
    "READY_TO_UPLOAD": "queued for upload",
    "UPLOADED": "uploaded; waiting for server verify",
    "VERIFY_RUNNING": "server verify in progress",
    "VERIFIED_REMOTE": "verified on server",
    "DUPLICATE_SHA_GLOBAL": "already existed on server",
    "ERROR_FILE": "paused after upload/verify error",
    "QUARANTINED_LOCAL": "quarantined after local verify mismatch",
}
_TICK_ACTION_STATES = {
    "STAGING_COPY", "HASHING", "DEDUP_SESSION_SHA", "DEDUP_LOCAL_SHA", 
    "QUEUE_UPLOAD", "WAIT_NETWORK", "UPLOAD_PREPARE", "UPLOAD_FILE", 
    "SERVER_VERIFY", "REUPLOAD_OR_QUARANTINE", "POST_UPLOAD_VERIFY", 
    "CLEANUP_STAGING", "JOB_COMPLETE_REMOTE", "JOB_COMPLETE_LOCAL",
}
_STATE_GUIDANCE = {
    "WAIT_NETWORK": {
        "kind": "waiting",
        "title": "Waiting for network connectivity",
        "summary": "Upload and server verification are paused until connectivity is restored.",
        "operator_action": "Connect to Wi-Fi or wait for connectivity to return, then refresh.",
    },
    "WAIT_MEDIA": {
        "kind": "waiting",
        "title": "Waiting for source media",
        "summary": "The daemon cannot continue copy work until source media is available.",
        "operator_action": "Reconnect the source device/path and run one daemon tick.",
    },
    "PAUSED_STORAGE": {
        "kind": "blocked",
        "title": "Storage health pause",
        "summary": "Ingest and upload progression are paused because local storage is unhealthy.",
        "operator_action": "Restore storage health, then run one daemon tick to resume.",
    },
    "ERROR_FILE": {
        "kind": "blocked",
        "title": "File-level errors need action",
        "summary": "At least one file hit retry exhaustion or verification failure.",
        "operator_action": "Open blocked jobs and retry or isolate affected files.",
    },
    "ERROR_JOB": {
        "kind": "blocked",
        "title": "Job-level failure",
        "summary": "A job cannot proceed automatically.",
        "operator_action": "Inspect job detail and diagnostics, then recover before new ingest.",
    },
    "ERROR_DAEMON": {
        "kind": "blocked",
        "title": "Daemon fault state",
        "summary": "The daemon entered a fatal state and cannot self-recover.",
        "operator_action": "Resolve daemon/service errors and return to IDLE before operations.",
    },
}
_AUTH_BLOCK_DETAILS = {"CLIENT_AUTH_REQUIRED", "CLIENT_AUTH_INVALID"}
_INGEST_BLOCKED_GUIDANCE = {
    "STAGING_COPY": {
        "summary": "A prior ingest job is still in copy/staging.",
        "operator_action": "If source media/path issues were corrected, run one daemon tick to retry the next file copy.",
    },
    "HASHING": {
        "summary": "A prior ingest job is still hashing staged files.",
        "operator_action": "Run one daemon tick to continue hashing or retry failed hash work.",
    },
    "JOB_COMPLETE_LOCAL": {
        "summary": "Local ingest finalization is still in progress.",
        "operator_action": "Run one daemon tick to return to IDLE, then start the next ingest.",
    },
    "WAIT_NETWORK": {
        "summary": "The daemon is waiting for network before continuing queued upload work.",
        "operator_action": "Do not start a new ingest yet. Wait for automatic daemon progression and refresh status.",
    },
    "ERROR_JOB": {
        "summary": "A prior ingest job failed and daemon recovery is required.",
        "operator_action": "Inspect job errors first; once corrected, return daemon to IDLE using the operator recovery procedure.",
    },
    "PAUSED_STORAGE": {
        "summary": "Ingest is paused because local storage is unhealthy.",
        "operator_action": "Restore storage health, then resume daemon processing before starting a new ingest.",
    },
    "ERROR_DAEMON": {
        "summary": "Daemon is in a fatal error state.",
        "operator_action": "Resolve daemon startup/runtime errors, then restore daemon to IDLE before ingesting.",
    },
}



def _derive_file_m2_view(file_record: dict[str, Any]) -> dict[str, str]:
    status = str(file_record.get("status", ""))
    if status in _REMOTE_ALREADY_EXISTS_STATUSES:
        classification_key = "REMOTE_ALREADY_EXISTS"
        classification_label = "already existed remotely"
    elif status in _UPLOAD_REQUIRED_STATUSES:
        classification_key = "UPLOAD_REQUIRED"
        classification_label = "upload required"
    else:
        classification_key = "NOT_CLASSIFIED_REMOTE"
        classification_label = "not yet remote-classified"

    return {
        "classification_key": classification_key,
        "classification_label": classification_label,
        "phase_label": _M2_PHASE_LABELS.get(status, "not in upload/verify path yet"),
    }


def _derive_job_m2_view(job: dict[str, Any]) -> dict[str, Any]:
    status_counts = dict(job.get("status_counts", {}))
    status = str(job.get("status", ""))
    local_ingest_complete = bool(job.get("local_ingest_complete"))

    remote_already_exists_count = int(
        sum(status_counts.get(file_status, 0) for file_status in _REMOTE_ALREADY_EXISTS_STATUSES)
    )
    upload_required_count = int(
        sum(status_counts.get(file_status, 0) for file_status in _UPLOAD_REQUIRED_STATUSES)
    )
    remote_terminal_count = int(
        sum(status_counts.get(file_status, 0) for file_status in _REMOTE_TERMINAL_STATUSES)
    )
    paused_on_error = status in _PAUSED_ERROR_JOB_STATUSES or int(status_counts.get("ERROR_FILE", 0)) > 0
    cleanup_complete = status in _REMOTE_COMPLETE_JOB_STATUSES
    remote_complete = cleanup_complete and remote_terminal_count > 0

    if paused_on_error:
        operation_state_label = "paused on error"
    elif remote_complete:
        operation_state_label = "remote complete"
    elif local_ingest_complete:
        operation_state_label = "local complete"
    else:
        operation_state_label = "local processing"

    if remote_terminal_count <= 0 and upload_required_count <= 0:
        cleanup_label = "n/a"
    elif cleanup_complete:
        cleanup_label = "complete"
    elif status == "CLEANUP_STAGING":
        cleanup_label = "in progress"
    elif paused_on_error:
        cleanup_label = "blocked by error"
    else:
        cleanup_label = "pending"

    return {
        "operation_state_label": operation_state_label,
        "remote_already_exists_count": remote_already_exists_count,
        "upload_required_count": upload_required_count,
        "remote_terminal_count": remote_terminal_count,
        "paused_on_error": paused_on_error,
        "remote_complete": remote_complete,
        "cleanup_complete": cleanup_complete,
        "cleanup_label": cleanup_label,
    }


def _build_ingest_gate(state: dict[str, Any] | None) -> dict[str, Any]:
    if not state:
        return {
            "can_start": False,
            "current_state": "UNKNOWN",
            "summary": "Daemon state is unavailable.",
            "operator_action": (
                "Refresh status and confirm the daemon is reachable before creating ingest jobs."
            ),
            "show_tick_action": False,
        }

    current_state = str(state.get("current_state", "UNKNOWN"))
    if current_state == "IDLE":
        return {
            "can_start": True,
            "current_state": current_state,
            "summary": "Daemon is ready for a new ingest job.",
            "operator_action": "Create ingest job",
            "show_tick_action": False,
        }

    state_guidance = _INGEST_BLOCKED_GUIDANCE.get(
        current_state,
        {
            "summary": "The daemon is actively processing prior work.",
            "operator_action": "Wait for IDLE or run a daemon tick if manual progression is needed.",
        },
    )
    return {
        "can_start": False,
        "current_state": current_state,
        "summary": state_guidance["summary"],
        "operator_action": state_guidance["operator_action"],
        "show_tick_action": current_state in _TICK_ACTION_STATES
        and current_state not in _AUTO_PROGRESS_DAEMON_STATES,
    }


def _format_size_bytes(size_bytes: object) -> str:
    try:
        value = int(size_bytes or 0)
    except (TypeError, ValueError):
        return "0 B"
    if value < 1024:
        return f"{value} B"
    units = ["KiB", "MiB", "GiB", "TiB"]
    scaled = float(value)
    for unit in units:
        scaled /= 1024.0
        if scaled < 1024.0 or unit == units[-1]:
            return f"{scaled:.1f} {unit}"
    return f"{value} B"


def _block_partition_ingest_prefill(partition: dict[str, Any]) -> dict[str, str]:
    mount_path = str(partition.get("target_mount_path", "")).strip()
    filesystem_label = str(partition.get("filesystem_label", "")).strip()
    device_path = str(partition.get("path", "")).strip()
    media_label = filesystem_label or device_path or "mounted-media"
    return {"media_label": media_label, "source_paths": mount_path}


def _derive_daemon_progress_view(state: dict[str, Any] | None) -> dict[str, Any]:
    if not state:
        return {
            "is_auto_progressing": False,
            "badge_label": "Progress unknown",
            "message": "Daemon activity is unavailable; refresh status.",
        }

    current_state = str(state.get("current_state", "UNKNOWN"))
    is_auto_progressing = current_state in _AUTO_PROGRESS_DAEMON_STATES
    if is_auto_progressing:
        return {
            "is_auto_progressing": True,
            "badge_label": "Auto progression active",
            "message": (
                "Upload/completion progression runs automatically. Wait and refresh instead of forcing ticks."
            ),
        }
    return {
        "is_auto_progressing": False,
        "badge_label": "Manual tick available",
        "message": "Use manual tick only for debugging or explicit recovery steps.",
    }


def _annotate_job_record(job: dict[str, Any]) -> dict[str, Any]:
    annotated = dict(job)
    annotated["m2"] = _derive_job_m2_view(annotated)
    files = annotated.get("files")
    if isinstance(files, list):
        normalized_files: list[dict[str, Any]] = []
        for file_record in files:
            file_copy = dict(file_record)
            file_copy["m2"] = _derive_file_m2_view(file_copy)
            normalized_files.append(file_copy)
        annotated["files"] = normalized_files
    annotated["operator_view"] = _derive_job_operator_view(annotated)
    return annotated


def _job_phase_label(status: str) -> str:
    labels = {
        "DISCOVERING": "discovering source files",
        "STAGING_COPY": "copying files into staging",
        "HASHING": "hashing staged files",
        "DEDUP_SESSION_SHA": "deduplicating within this job",
        "DEDUP_LOCAL_SHA": "checking local SHA history",
        "QUEUE_UPLOAD": "queuing remote upload work",
        "WAIT_NETWORK": "waiting for network to continue upload",
        "UPLOAD_PREPARE": "preparing server handshake",
        "UPLOAD_FILE": "uploading file bytes",
        "SERVER_VERIFY": "waiting for server-side verification",
        "REUPLOAD_OR_QUARANTINE": "deciding retry or quarantine",
        "POST_UPLOAD_VERIFY": "running post-upload local verify",
        "CLEANUP_STAGING": "cleaning staged files",
        "JOB_COMPLETE_REMOTE": "remote completion finalization",
        "JOB_COMPLETE_LOCAL": "local completion finalization",
        "ERROR_JOB": "job blocked by failure",
        "PAUSED_STORAGE": "paused due to storage issue",
        "ERROR_DAEMON": "blocked by daemon error",
    }
    return labels.get(status, "state not yet classified")


def _derive_job_operator_view(job: dict[str, Any]) -> dict[str, Any]:
    status = str(job.get("status", ""))
    status_counts = dict(job.get("status_counts", {}))
    files = job.get("files")
    file_rows = files if isinstance(files, list) else []
    error_files = [row for row in file_rows if str(row.get("status", "")) == "ERROR_FILE"]
    retrying_files = [row for row in file_rows if int(row.get("retry_count", 0) or 0) > 0]
    max_retry_count = 0
    for row in file_rows:
        retry_count = int(row.get("retry_count", 0) or 0)
        if retry_count > max_retry_count:
            max_retry_count = retry_count
    upload_required_count = int(
        sum(status_counts.get(file_status, 0) for file_status in _UPLOAD_REQUIRED_STATUSES)
    )
    total_file_count = int(sum(int(count) for count in status_counts.values()))
    transferred_file_count = int(status_counts.get("VERIFIED_REMOTE", 0))
    ignored_file_count = int(sum(status_counts.get(file_status, 0) for file_status in _IGNORED_FILE_STATUSES))
    failed_file_count = int(sum(status_counts.get(file_status, 0) for file_status in _ERROR_FILE_STATUSES))
    pending_file_count = max(
        total_file_count - transferred_file_count - ignored_file_count - failed_file_count,
        0,
    )

    def _segment_percent(count: int, total: int) -> float:
        if total <= 0 or count <= 0:
            return 0.0
        return (float(count) / float(total)) * 100.0

    waiting_on_network = status == "WAIT_NETWORK"
    retry_backoff_active = waiting_on_network and (len(retrying_files) > 0 or upload_required_count > 0)
    requires_operator_action = status in _BLOCKED_DAEMON_STATES or len(error_files) > 0
    local_ingest_complete = bool(job.get("local_ingest_complete"))
    remote_complete = bool(job.get("m2", {}).get("remote_complete"))
    retry_exhausted_file_count = max(len(error_files), int(status_counts.get("ERROR_FILE", 0)))

    if requires_operator_action:
        next_action = "Open job detail and resolve failed files before new ingest."
    elif waiting_on_network:
        next_action = "Wait for network connectivity; upload retry progression is automatic."
    elif status in _AUTO_PROGRESS_DAEMON_STATES:
        next_action = "Wait for auto-progression and refresh."
    else:
        next_action = "Monitor progress; run one manual tick only for explicit recovery."

    if remote_complete:
        completion_summary = "Remote upload and cleanup are complete for all remote-targeted files."
    elif local_ingest_complete and upload_required_count > 0:
        completion_summary = "Local ingest is complete; only remote upload, verify, or cleanup work remains."
    elif local_ingest_complete:
        completion_summary = "Local ingest is complete; remaining work is limited to job finalization."
    else:
        completion_summary = "Local ingest is still in progress before remote completion can finish."

    if retry_exhausted_file_count > 0:
        retry_summary = (
            f"{retry_exhausted_file_count} file(s) need manual retry or isolation "
            "after upload/verify failure."
        )
    elif retry_backoff_active:
        retry_summary = "Retry backoff is active while the daemon waits in WAIT_NETWORK."
    elif len(retrying_files) > 0:
        retry_summary = "Retry history exists, but the daemon is currently progressing normally."
    else:
        retry_summary = "No file is currently paused on upload retry handling."

    if waiting_on_network and upload_required_count > 0:
        wait_summary = (
            f"{upload_required_count} file(s) are queued for remote upload/verify once connectivity returns."
        )
    elif waiting_on_network:
        wait_summary = "The daemon is waiting for network before remote progression can continue."
    elif requires_operator_action:
        wait_summary = "Operator action is required before this job can continue remote progression."
    else:
        wait_summary = "No external dependency is currently blocking this job."

    return {
        "phase_label": _job_phase_label(status),
        "error_file_count": int(status_counts.get("ERROR_FILE", 0)),
        "upload_required_count": upload_required_count,
        "verified_remote_count": int(status_counts.get("VERIFIED_REMOTE", 0)),
        "transferred_file_count": transferred_file_count,
        "ignored_file_count": ignored_file_count,
        "failed_file_count": failed_file_count,
        "pending_file_count": pending_file_count,
        "total_file_count": total_file_count,
        "uploaded_percent": _segment_percent(transferred_file_count, total_file_count),
        "ignored_percent": _segment_percent(ignored_file_count, total_file_count),
        "failed_percent": _segment_percent(failed_file_count, total_file_count),
        "pending_percent": _segment_percent(pending_file_count, total_file_count),
        "retrying_file_count": len(retrying_files),
        "retry_exhausted_file_count": retry_exhausted_file_count,
        "max_retry_count": max_retry_count,
        "retry_backoff_active": retry_backoff_active,
        "requires_operator_action": requires_operator_action,
        "next_action": next_action,
        "completion_summary": completion_summary,
        "retry_summary": retry_summary,
        "wait_summary": wait_summary,
        "error_files": error_files,
    }


def _job_filter_key(job: dict[str, Any]) -> str:
    status = str(job.get("status", ""))
    m2 = job.get("m2", {})
    paused_on_error = bool(m2.get("paused_on_error"))
    remote_complete = bool(m2.get("remote_complete")) or status in _REMOTE_COMPLETE_JOB_STATUSES
    if remote_complete:
        return "completed"
    if paused_on_error or status in _PAUSED_ERROR_JOB_STATUSES:
        return "blocked"
    if status in _WAITING_DAEMON_STATES:
        return "waiting"
    return "active"


def _filter_jobs(jobs: list[dict[str, Any]], selected_filter: str) -> list[dict[str, Any]]:
    if selected_filter == "all":
        return jobs
    return [job for job in jobs if _job_filter_key(job) == selected_filter]


def _daemon_health_label(state: dict[str, Any] | None, daemon_error: str | None) -> tuple[str, str]:
    if daemon_error:
        return "critical", "Daemon API unreachable"
    if not state:
        return "warning", "State unavailable"
    current_state = str(state.get("current_state", "UNKNOWN"))
    if current_state in _BLOCKED_DAEMON_STATES:
        return "critical", "Blocked by daemon or storage error"
    if current_state in _WAITING_DAEMON_STATES:
        return "warning", "Waiting for external dependency"
    if current_state == "IDLE":
        return "ok", "Idle and ready"
    if current_state in _ACTIVE_DAEMON_STATES:
        return "active", "Processing workload"
    return "warning", "Unknown daemon state"


def _derive_client_auth_guidance(state: dict[str, Any] | None) -> dict[str, str] | None:
    if not isinstance(state, dict):
        return None
    auth_state = state.get("server_auth")
    if not isinstance(auth_state, dict):
        return None

    enrollment_status = str(auth_state.get("enrollment_status", "")).strip()
    last_error = str(auth_state.get("last_error", "")).strip()

    if enrollment_status == "pending":
        return {
            "kind": "blocked",
            "title": "Client enrollment pending approval",
            "summary": "Privileged upload and verify calls are blocked until server approval is granted.",
            "operator_action": "Approve this client from the server UI, then run one daemon tick.",
        }
    if enrollment_status == "revoked":
        return {
            "kind": "blocked",
            "title": "Client access revoked",
            "summary": "Server-side revocation is blocking privileged upload and verify operations.",
            "operator_action": "Re-approve the client on the server if access should be restored.",
        }
    if last_error in _AUTH_BLOCK_DETAILS:
        return {
            "kind": "blocked",
            "title": "Client auth rejected by server",
            "summary": f"Privileged API calls are blocked by server auth response: {last_error}.",
            "operator_action": (
                "Check client enrollment/token on the server and clientd env configuration, "
                "then run one daemon tick."
            ),
        }
    return None


def _derive_state_guidance(state: dict[str, Any] | None, daemon_error: str | None) -> dict[str, str]:
    if daemon_error:
        return {
            "kind": "blocked",
            "title": "Daemon API unavailable",
            "summary": "The UI cannot retrieve current daemon state.",
            "operator_action": "Check photovault-clientd.service and local daemon API reachability.",
        }

    client_auth_guidance = _derive_client_auth_guidance(state)
    if client_auth_guidance is not None:
        return client_auth_guidance

    current_state = str((state or {}).get("current_state", "UNKNOWN"))
    if current_state in _STATE_GUIDANCE:
        return dict(_STATE_GUIDANCE[current_state])
    if current_state == "IDLE":
        return {
            "kind": "healthy",
            "title": "System ready",
            "summary": "Daemon is idle and ready for ingest.",
            "operator_action": "Start ingest when media is ready.",
        }
    if current_state in _AUTO_PROGRESS_DAEMON_STATES:
        return {
            "kind": "active",
            "title": "Auto progression active",
            "summary": "Daemon is advancing upload/completion work without manual ticks.",
            "operator_action": "Wait and refresh status unless recovery actions are required.",
        }
    if current_state in _TICK_ACTION_STATES:
        return {
            "kind": "active",
            "title": "Work in progress",
            "summary": "Daemon is processing state-machine work.",
            "operator_action": "Wait and monitor; use manual tick for explicit recovery only.",
        }
    return {
        "kind": "waiting",
        "title": "State unknown",
        "summary": f"Daemon reported unclassified state: {current_state}.",
        "operator_action": "Use diagnostics/events to understand recent transitions.",
    }


def _summarize_recent_events(events: list[dict[str, Any]]) -> dict[str, Any]:
    if not events:
        return {
            "highlights": [],
            "error_count": 0,
            "warn_count": 0,
            "latest_error": None,
            "latest_activity": None,
        }

    error_events = [event for event in events if str(event.get("level", "")) == "ERROR"]
    warn_events = [event for event in events if str(event.get("level", "")) == "WARN"]
    latest_error = error_events[0] if error_events else None
    latest_activity = events[0]
    highlights: list[dict[str, str]] = []
    seen_categories: set[str] = set()
    for event in events:
        category = str(event.get("category", "UNKNOWN"))
        if category in seen_categories:
            continue
        seen_categories.add(category)
        highlights.append(
            {
                "category": category,
                "created_at_utc": str(event.get("created_at_utc", "")),
                "message": str(event.get("message", "")),
                "level": str(event.get("level", "INFO")),
            }
        )
        if len(highlights) >= 4:
            break
    return {
        "highlights": highlights,
        "error_count": len(error_events),
        "warn_count": len(warn_events),
        "latest_error": latest_error,
        "latest_activity": latest_activity,
    }


def _dependency_health_label(dependencies: list[dict[str, str]]) -> tuple[str, int]:
    degraded_count = 0
    for dependency in dependencies:
        status = str(dependency.get("status", ""))
        if status not in {"ready", "active"}:
            degraded_count += 1
    if degraded_count == 0:
        return "ok", 0
    return "warning", degraded_count


def _build_overview_metrics(
    *,
    jobs: list[dict[str, Any]],
    state: dict[str, Any] | None,
    daemon_error: str | None,
    diagnostics: dict[str, Any] | None,
    dependencies: list[dict[str, str]],
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    active_jobs = [job for job in jobs if _job_filter_key(job) == "active"]
    waiting_jobs = [job for job in jobs if _job_filter_key(job) == "waiting"]
    blocked_jobs = [job for job in jobs if _job_filter_key(job) == "blocked"]
    completed_jobs = [job for job in jobs if _job_filter_key(job) == "completed"]
    upload_pending_jobs = [job for job in jobs if bool(job.get("upload_pending"))]

    daemon_health_level, daemon_health_label = _daemon_health_label(state, daemon_error)
    dependency_health_level, dependency_degraded_count = _dependency_health_label(dependencies)
    state_guidance = _derive_state_guidance(state, daemon_error)
    client_auth_guidance = _derive_client_auth_guidance(state)
    event_summary = _summarize_recent_events(events)

    alerts: list[dict[str, str]] = []
    if daemon_error:
        alerts.append(
            {
                "severity": "critical",
                "title": "Daemon API unavailable",
                "message": "Control surface cannot refresh state; check photovault-clientd.service first.",
            }
        )
    if blocked_jobs:
        alerts.append(
            {
                "severity": "critical",
                "title": f"{len(blocked_jobs)} blocked job(s)",
                "message": (
                    "Open Jobs and resolve upload/verify/storage errors before starting "
                    "new ingest work."
                ),
            }
        )
    if waiting_jobs:
        alerts.append(
            {
                "severity": "warning",
                "title": f"{len(waiting_jobs)} waiting job(s)",
                "message": (
                    "Jobs are paused on dependencies such as network/media; "
                    "operator wait/fix required."
                ),
            }
        )
    if client_auth_guidance is not None:
        alerts.append(
            {
                "severity": "critical",
                "title": client_auth_guidance["title"],
                "message": client_auth_guidance["summary"],
            }
        )
    current_state = str((state or {}).get("current_state", "UNKNOWN"))
    if current_state in _WAITING_DAEMON_STATES:
        alerts.append(
            {
                "severity": "warning",
                "title": f"Daemon waiting in {current_state}",
                "message": state_guidance["summary"],
            }
        )
    if dependency_degraded_count > 0:
        alerts.append(
            {
                "severity": "warning",
                "title": f"{dependency_degraded_count} degraded dependency",
                "message": "Review dependency health panel for service/storage readiness issues.",
            }
        )
    if diagnostics and int(diagnostics.get("invariant_issue_count", 0)) > 0:
        alerts.append(
            {
                "severity": "critical",
                "title": "Invariant issues detected",
                "message": "Open diagnostics and inspect daemon events before continuing normal operations.",
            }
        )

    if alerts and state_guidance["kind"] == "blocked":
        next_action = "Resolve blocked conditions first, then run one daemon tick to confirm recovery."
    else:
        next_action = state_guidance["operator_action"]

    return {
        "daemon_health_level": daemon_health_level,
        "daemon_health_label": daemon_health_label,
        "dependency_health_level": dependency_health_level,
        "active_jobs_count": len(active_jobs),
        "waiting_jobs_count": len(waiting_jobs),
        "blocked_jobs_count": len(blocked_jobs),
        "completed_jobs_count": len(completed_jobs),
        "upload_pending_jobs_count": len(upload_pending_jobs),
        "alerts": alerts,
        "highlight_jobs": (blocked_jobs + waiting_jobs + active_jobs)[:3],
        "client_auth_guidance": client_auth_guidance,
        "state_guidance": state_guidance,
        "event_summary": event_summary,
        "next_action": next_action,
    }
