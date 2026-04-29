"""Overview and daemon-guidance view models for the client UI."""

from typing import Any

from .view_models_jobs import _job_filter_key

_ACTIVE_DAEMON_STATES = {
    "STAGING_COPY",
    "HASHING",
    "DEDUP_SESSION_SHA",
    "DEDUP_LOCAL_SHA",
    "QUEUE_UPLOAD",
    "UPLOAD_PREPARE",
    "UPLOAD_FILE",
    "SERVER_VERIFY",
    "REUPLOAD_OR_QUARANTINE",
    "POST_UPLOAD_VERIFY",
    "CLEANUP_STAGING",
    "VERIFY_IDLE",
    "VERIFY_HASH",
}
_AUTO_PROGRESS_DAEMON_STATES = {
    "WAIT_NETWORK",
    "UPLOAD_PREPARE",
    "UPLOAD_FILE",
    "SERVER_VERIFY",
    "REUPLOAD_OR_QUARANTINE",
    "POST_UPLOAD_VERIFY",
    "CLEANUP_STAGING",
    "JOB_COMPLETE_REMOTE",
    "JOB_COMPLETE_LOCAL",
}
_WAITING_DAEMON_STATES = {"WAIT_NETWORK", "WAIT_MEDIA"}
_BLOCKED_DAEMON_STATES = {"ERROR_DAEMON", "ERROR_JOB", "PAUSED_STORAGE"}
_TICK_ACTION_STATES = {
    "STAGING_COPY",
    "HASHING",
    "DEDUP_SESSION_SHA",
    "DEDUP_LOCAL_SHA",
    "QUEUE_UPLOAD",
    "WAIT_NETWORK",
    "UPLOAD_PREPARE",
    "UPLOAD_FILE",
    "SERVER_VERIFY",
    "REUPLOAD_OR_QUARANTINE",
    "POST_UPLOAD_VERIFY",
    "CLEANUP_STAGING",
    "JOB_COMPLETE_REMOTE",
    "JOB_COMPLETE_LOCAL",
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
        "operator_action": (
            "If source media/path issues were corrected, run one daemon tick to retry the next file copy."
        ),
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
        "operator_action": (
            "Do not start a new ingest yet. Wait for automatic daemon progression and refresh status."
        ),
    },
    "ERROR_JOB": {
        "summary": "A prior ingest job failed and daemon recovery is required.",
        "operator_action": (
            "Inspect job errors first; once corrected, return daemon to IDLE "
            "using the operator recovery procedure."
        ),
    },
    "PAUSED_STORAGE": {
        "summary": "Ingest is paused because local storage is unhealthy.",
        "operator_action": (
            "Restore storage health, then resume daemon processing before starting a new ingest."
        ),
    },
    "ERROR_DAEMON": {
        "summary": "Daemon is in a fatal error state.",
        "operator_action": (
            "Resolve daemon startup/runtime errors, then restore daemon to IDLE "
            "before ingesting."
        ),
    },
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
