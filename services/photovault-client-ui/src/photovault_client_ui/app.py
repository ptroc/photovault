"""SSR control-plane UI for the photovault client."""

import os
import sqlite3
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
from flask import Flask, Response, abort, make_response, redirect, render_template, request, url_for

DEFAULT_DAEMON_BASE_URL = "http://127.0.0.1:9101"
DEFAULT_HTTP_TIMEOUT_SECONDS = 2.0
DEFAULT_TICK_TIMEOUT_SECONDS = 2.0
DEFAULT_TICK_STATUS_REFRESH_MS = 1500
DEFAULT_CLIENT_DB_PATH = Path("/var/lib/photovault-clientd/state.sqlite3")
DEFAULT_STAGING_ROOT = Path("/var/lib/photovault-clientd/staging")
DEFAULT_SERVER_API_URL = "http://127.0.0.1:9301"

_REMOTE_ALREADY_EXISTS_STATUSES = {"DUPLICATE_SHA_GLOBAL"}
_UPLOAD_REQUIRED_STATUSES = {
    "READY_TO_UPLOAD",
    "UPLOADED",
    "VERIFY_RUNNING",
    "VERIFIED_REMOTE",
    "ERROR_FILE",
}
_REMOTE_TERMINAL_STATUSES = {"VERIFIED_REMOTE", "DUPLICATE_SHA_GLOBAL"}
_PAUSED_ERROR_JOB_STATUSES = {"ERROR_FILE", "ERROR_JOB", "PAUSED_STORAGE"}
_REMOTE_COMPLETE_JOB_STATUSES = {"JOB_COMPLETE_REMOTE", "JOB_COMPLETE_LOCAL"}
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
            "Resolve daemon startup/runtime errors, then restore daemon to IDLE before ingesting."
        ),
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


def _describe_http_error(exc: httpx.HTTPError) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        status_code = exc.response.status_code
        detail = ""
        try:
            payload = exc.response.json()
            if isinstance(payload, dict):
                detail = str(payload.get("detail", "")).strip()
        except ValueError:
            detail = exc.response.text.strip()
        summary = f"daemon API returned HTTP {status_code}"
        if detail:
            return f"{summary}: {detail}"
        return summary

    message = str(exc).strip()
    if isinstance(exc, httpx.ConnectError):
        return f"connection failure: {message or 'unable to reach daemon endpoint'}"
    if isinstance(exc, httpx.TimeoutException):
        return f"request timeout: {message or 'daemon did not respond in time'}"
    if message:
        return message
    return exc.__class__.__name__


def _format_ingest_source_validation_error(exc: httpx.HTTPStatusError) -> tuple[str | None, str | None]:
    try:
        payload = exc.response.json()
    except ValueError:
        return None, None

    if not isinstance(payload, dict):
        return None, None

    detail = payload.get("detail")
    if not isinstance(detail, dict):
        return None, None
    if str(detail.get("code", "")).strip() != "INGEST_SOURCE_PATH_INVALID":
        return None, None

    message = str(detail.get("message", "")).strip() or "One or more source paths are invalid."
    suggestion = str(detail.get("suggestion", "")).strip()
    invalid_sources = detail.get("invalid_sources")

    source_lines: list[str] = []
    if isinstance(invalid_sources, list):
        for item in invalid_sources:
            if not isinstance(item, dict):
                continue
            source_path = str(item.get("source_path", "")).strip()
            reason = str(item.get("reason", "")).strip()
            if source_path and reason:
                source_lines.append(f"{source_path}: {reason}")

    operator_message = (
        f"{message} {suggestion}".strip() if suggestion else message
    )
    technical_detail = _describe_http_error(exc)
    if source_lines:
        technical_detail = "\n".join([technical_detail, *source_lines])
    return operator_message, technical_detail


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
    waiting_on_network = status == "WAIT_NETWORK"
    retry_backoff_active = waiting_on_network and (len(retrying_files) > 0 or upload_required_count > 0)
    requires_operator_action = status in _BLOCKED_DAEMON_STATES or len(error_files) > 0
    if requires_operator_action:
        next_action = "Open job detail and resolve failed files before new ingest."
    elif waiting_on_network:
        next_action = "Wait for network connectivity; upload retry progression is automatic."
    elif status in _AUTO_PROGRESS_DAEMON_STATES:
        next_action = "Wait for auto-progression and refresh."
    else:
        next_action = "Monitor progress; run one manual tick only for explicit recovery."

    return {
        "phase_label": _job_phase_label(status),
        "error_file_count": int(status_counts.get("ERROR_FILE", 0)),
        "upload_required_count": upload_required_count,
        "verified_remote_count": int(status_counts.get("VERIFIED_REMOTE", 0)),
        "retrying_file_count": len(retrying_files),
        "max_retry_count": max_retry_count,
        "retry_backoff_active": retry_backoff_active,
        "requires_operator_action": requires_operator_action,
        "next_action": next_action,
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


def _derive_state_guidance(state: dict[str, Any] | None, daemon_error: str | None) -> dict[str, str]:
    if daemon_error:
        return {
            "kind": "blocked",
            "title": "Daemon API unavailable",
            "summary": "The UI cannot retrieve current daemon state.",
            "operator_action": "Check photovault-clientd.service and local daemon API reachability.",
        }

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
        "state_guidance": state_guidance,
        "event_summary": event_summary,
        "next_action": next_action,
    }


def _daemon_get(daemon_base_url: str, path: str) -> Any:
    with httpx.Client(base_url=daemon_base_url, timeout=DEFAULT_HTTP_TIMEOUT_SECONDS) as client:
        response = client.get(path)
        response.raise_for_status()
        return response.json()


def _daemon_post(
    daemon_base_url: str,
    path: str,
    payload: dict[str, Any],
    *,
    timeout_seconds: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
) -> Any:
    with httpx.Client(base_url=daemon_base_url, timeout=timeout_seconds) as client:
        response = client.post(path, json=payload)
        response.raise_for_status()
        return response.json()


def _run_command(args: list[str]) -> str:
    completed = subprocess.run(
        args,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout


def _parse_nmcli_multiline(output: str) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    current: dict[str, str] = {}
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line:
            if current:
                records.append(current)
                current = {}
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        normalized_key = key.strip()
        if normalized_key in current and current:
            records.append(current)
            current = {}
        current[normalized_key] = value.strip()
    if current:
        records.append(current)
    return records


def _get_network_snapshot(command_runner: Callable[[list[str]], str] = _run_command) -> dict[str, Any]:
    general_output = command_runner(["nmcli", "-m", "multiline", "-f", "STATE,CONNECTIVITY,WIFI", "general"])
    devices_output = command_runner(
        ["nmcli", "-m", "multiline", "-f", "DEVICE,TYPE,STATE,CONNECTION", "device", "status"]
    )
    wifi_output = command_runner(
        ["nmcli", "-m", "multiline", "-f", "IN-USE,SSID,SIGNAL,SECURITY,CHAN,RATE", "device", "wifi", "list"]
    )

    general_records = _parse_nmcli_multiline(general_output)
    general = general_records[0] if general_records else {}
    devices = _parse_nmcli_multiline(devices_output)
    wifi_networks = _parse_nmcli_multiline(wifi_output)

    return {
        "general": {
            "state": general.get("STATE", "unknown"),
            "connectivity": general.get("CONNECTIVITY", "unknown"),
            "wifi": general.get("WIFI", "unknown"),
        },
        "devices": [
            {
                "device": item.get("DEVICE", ""),
                "type": item.get("TYPE", ""),
                "state": item.get("STATE", ""),
                "connection": item.get("CONNECTION", ""),
            }
            for item in devices
        ],
        "wifi_networks": [
            {
                "in_use": item.get("IN-USE", ""),
                "ssid": item.get("SSID", ""),
                "signal": item.get("SIGNAL", ""),
                "security": item.get("SECURITY", ""),
                "channel": item.get("CHAN", ""),
                "rate": item.get("RATE", ""),
            }
            for item in wifi_networks
            if item.get("SSID", "")
        ],
    }


def _scan_networks(command_runner: Callable[[list[str]], str] = _run_command) -> None:
    command_runner(["nmcli", "device", "wifi", "rescan"])


def _connect_network(
    ssid: str,
    password: str | None,
    command_runner: Callable[[list[str]], str] = _run_command,
) -> None:
    args = ["nmcli", "device", "wifi", "connect", ssid]
    if password:
        args.extend(["password", password])
    command_runner(args)


def _format_network_error(action: str, exc: subprocess.CalledProcessError | FileNotFoundError) -> str:
    if isinstance(exc, FileNotFoundError):
        return f"Failed to {action}: nmcli is not installed on this device."

    stderr = (exc.stderr or "").strip()
    stdout = (exc.stdout or "").strip()
    details = stderr or stdout
    if "not authorized" in details.lower():
        return (
            f"Failed to {action}: NetworkManager denied the photovault service user. "
            "This device needs a polkit rule that allows Wi-Fi management."
        )
    if details:
        return f"Failed to {action}: {details}"
    return f"Failed to {action}: nmcli exited with status {exc.returncode}."


def _systemd_service_state(
    service_name: str,
    command_runner: Callable[[list[str]], str] = _run_command,
) -> str:
    try:
        return command_runner(["systemctl", "is-active", service_name]).strip() or "unknown"
    except FileNotFoundError:
        return "systemctl unavailable"
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        stdout = (exc.stdout or "").strip()
        return stderr or stdout or f"exit {exc.returncode}"


def _get_dependency_snapshot() -> list[dict[str, str]]:
    dependencies: list[dict[str, str]] = []

    if DEFAULT_CLIENT_DB_PATH.exists():
        sqlite_status = "ready"
        sqlite_detail = str(DEFAULT_CLIENT_DB_PATH)
        try:
            with sqlite3.connect(DEFAULT_CLIENT_DB_PATH) as conn:
                conn.execute("SELECT 1;").fetchone()
        except sqlite3.Error as exc:
            sqlite_status = "error"
            sqlite_detail = f"{DEFAULT_CLIENT_DB_PATH}: {exc}"
    else:
        sqlite_status = "missing"
        sqlite_detail = str(DEFAULT_CLIENT_DB_PATH)
    dependencies.append({"name": "SQLite", "status": sqlite_status, "detail": sqlite_detail})

    storage_status = "ready"
    if DEFAULT_STAGING_ROOT.exists():
        storage_detail = str(DEFAULT_STAGING_ROOT)
        if not DEFAULT_STAGING_ROOT.is_dir():
            storage_status = "error"
            storage_detail = f"{DEFAULT_STAGING_ROOT}: not a directory"
    else:
        parent = DEFAULT_STAGING_ROOT.parent
        if parent.exists():
            writable = parent.is_dir() and os.access(parent, os.W_OK)
            storage_status = "provisionable" if writable else "missing"
            storage_detail = f"{DEFAULT_STAGING_ROOT} (parent {parent})"
        else:
            storage_status = "missing"
            storage_detail = f"{DEFAULT_STAGING_ROOT} (parent missing)"
    dependencies.append({"name": "Storage", "status": storage_status, "detail": storage_detail})

    dependencies.append(
        {
            "name": "photovault-clientd.service",
            "status": _systemd_service_state("photovault-clientd.service"),
            "detail": f"local daemon API at {DEFAULT_DAEMON_BASE_URL}",
        }
    )
    dependencies.append(
        {
            "name": "NetworkManager.service",
            "status": _systemd_service_state("NetworkManager.service"),
            "detail": "network connectivity and Wi-Fi control",
        }
    )
    dependencies.append(
        {
            "name": "photovault-api.service",
            "status": _systemd_service_state("photovault-api.service"),
            "detail": f"server upload and verify API at {DEFAULT_SERVER_API_URL}",
        }
    )

    return dependencies


def create_app(
    daemon_base_url: str = DEFAULT_DAEMON_BASE_URL,
    daemon_get: Callable[[str, str], Any] = _daemon_get,
    daemon_post: Callable[[str, str, dict[str, Any]], Any] = _daemon_post,
    network_snapshot_get: Callable[[], dict[str, Any]] = _get_network_snapshot,
    network_connect: Callable[[str, str | None], None] = _connect_network,
    network_scan: Callable[[], None] = _scan_networks,
    dependency_snapshot_get: Callable[[], list[dict[str, str]]] = _get_dependency_snapshot,
) -> Flask:
    app = Flask(__name__)

    def _is_ajax_request() -> bool:
        return request.headers.get("X-Requested-With") == "XMLHttpRequest"

    def _load_daemon_context(*, events_limit: int = 10) -> dict[str, Any]:
        context: dict[str, Any] = {
            "daemon_error": None,
            "daemon_error_detail": None,
            "state": None,
            "diagnostics": None,
            "jobs": [],
            "events": [],
        }
        try:
            context["state"] = daemon_get(daemon_base_url, "/state")
            context["diagnostics"] = daemon_get(daemon_base_url, "/diagnostics/m0")
            jobs_payload = daemon_get(daemon_base_url, "/ingest/jobs")
            events_payload = daemon_get(daemon_base_url, f"/events?limit={events_limit}")
            context["jobs"] = list(jobs_payload.get("jobs", []))
            context["events"] = list(events_payload.get("events", []))
        except httpx.HTTPError as exc:
            context["daemon_error"] = (
                "Unable to reach the local daemon API. Check photovault-clientd.service and try refresh."
            )
            context["daemon_error_detail"] = _describe_http_error(exc)
        return context

    def _load_selected_job(job_id: int | None) -> tuple[dict[str, Any] | None, str | None]:
        if job_id is None:
            return None, None
        try:
            return daemon_get(daemon_base_url, f"/ingest/jobs/{job_id}"), None
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return None, f"job_id {job_id} not found"
            return None, str(exc)
        except httpx.HTTPError as exc:
            return None, str(exc)

    def _load_network_context(
        network_error: str | None = None,
        network_form_data: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        context: dict[str, Any] = {
            "network_snapshot": None,
            "network_error": network_error,
            "network_form": {"ssid": "", "password": ""},
        }
        if network_form_data:
            context["network_form"].update(network_form_data)
        try:
            context["network_snapshot"] = network_snapshot_get()
        except (subprocess.CalledProcessError, FileNotFoundError) as exc:
            if context["network_error"] is None:
                context["network_error"] = _format_network_error("load NetworkManager status", exc)
        return context

    def _render_overview(
        *,
        ingest_error: str | None = None,
        ingest_error_detail: str | None = None,
        ingest_notice: str | None = None,
        ingest_notice_detail: str | None = None,
        operator_notice: str | None = None,
        operator_notice_pending: bool = False,
        auto_refresh_ms: int | None = None,
        form_data: dict[str, str] | None = None,
    ) -> Response:
        ingest_form = {"media_label": "", "source_paths": ""}
        if form_data:
            ingest_form.update(form_data)
        context = _load_daemon_context()
        context["jobs"] = [_annotate_job_record(job) for job in context["jobs"]]
        dependencies = dependency_snapshot_get()
        ingest_gate = _build_ingest_gate(context["state"])
        overview_metrics = _build_overview_metrics(
            jobs=context["jobs"],
            state=context["state"],
            daemon_error=context["daemon_error"],
            diagnostics=context["diagnostics"],
            dependencies=dependencies,
            events=context["events"],
        )
        context.update(
            {
                "dependencies": dependencies,
                "overview_metrics": overview_metrics,
                "daemon_progress": _derive_daemon_progress_view(context["state"]),
                "daemon_base_url": daemon_base_url,
                "ingest_error": ingest_error,
                "ingest_error_detail": ingest_error_detail,
                "ingest_notice": ingest_notice,
                "ingest_notice_detail": ingest_notice_detail,
                "operator_notice": operator_notice,
                "operator_notice_pending": operator_notice_pending,
                "auto_refresh_ms": auto_refresh_ms,
                "auto_refresh_path": url_for("index"),
                "ingest_form": ingest_form,
                "ingest_gate": ingest_gate,
                "active_page": "overview",
            }
        )
        template_name = "_overview_content.html" if _is_ajax_request() else "overview.html"
        response = make_response(render_template(template_name, **context))
        response.headers["X-Client-Location"] = url_for("index")
        return response

    def _render_jobs(*, selected_filter: str = "active") -> str:
        context = _load_daemon_context()
        jobs = [_annotate_job_record(job) for job in context["jobs"]]
        effective_filter = (
            selected_filter
            if selected_filter in {"active", "waiting", "blocked", "completed", "all"}
            else "active"
        )
        filtered_jobs = _filter_jobs(jobs, effective_filter)
        context.update(
            {
                "daemon_base_url": daemon_base_url,
                "jobs": filtered_jobs,
                "job_filter": effective_filter,
                "job_filter_counts": {
                    "active": len(_filter_jobs(jobs, "active")),
                    "waiting": len(_filter_jobs(jobs, "waiting")),
                    "blocked": len(_filter_jobs(jobs, "blocked")),
                    "completed": len(_filter_jobs(jobs, "completed")),
                    "all": len(jobs),
                },
                "active_page": "jobs",
            }
        )
        return render_template("jobs.html", **context)

    def _render_job_detail(
        job_id: int,
        *,
        action_error: str | None = None,
        action_notice: str | None = None,
        action_notice_pending: bool = False,
    ) -> str:
        context = _load_daemon_context()
        selected_job, selected_job_error = _load_selected_job(job_id)
        if selected_job is None:
            if selected_job_error == f"job_id {job_id} not found":
                abort(404, description=selected_job_error)
            if context["daemon_error"] is None:
                context["daemon_error"] = selected_job_error
            return _render_jobs(selected_filter="all")

        selected_job = _annotate_job_record(selected_job)
        job_events: list[dict[str, Any]] = []
        for event in context["events"]:
            message = str(event.get("message", ""))
            if f"job_id={job_id}" in message:
                job_events.append(event)
        if not job_events:
            job_events = context["events"][:4]
        context.update(
            {
                "daemon_base_url": daemon_base_url,
                "daemon_progress": _derive_daemon_progress_view(context["state"]),
                "state_guidance": _derive_state_guidance(context["state"], context["daemon_error"]),
                "job_events": job_events[:4],
                "selected_job": selected_job,
                "active_page": "jobs",
                "action_error": action_error,
                "action_notice": action_notice,
                "action_notice_pending": action_notice_pending,
            }
        )
        return render_template("job_detail.html", **context)

    def _render_events() -> str:
        context = _load_daemon_context(events_limit=30)
        context.update(
            {
                "daemon_base_url": daemon_base_url,
                "event_summary": _summarize_recent_events(context["events"]),
                "state_guidance": _derive_state_guidance(context["state"], context["daemon_error"]),
                "active_page": "events",
            }
        )
        return render_template("events.html", **context)

    def _render_network(
        *,
        network_error: str | None = None,
        network_form_data: dict[str, str] | None = None,
    ) -> str:
        context = _load_daemon_context()
        context.update(
            _load_network_context(
                network_error=network_error,
                network_form_data=network_form_data,
            )
        )
        context.update(
            {
                "daemon_base_url": daemon_base_url,
                "active_page": "network",
            }
        )
        return render_template("network.html", **context)

    @app.get("/")
    def index() -> Response:
        return _render_overview()

    @app.get("/jobs")
    def jobs_page() -> str:
        return _render_jobs(selected_filter=request.args.get("filter", "active"))

    @app.get("/events")
    def events_page() -> str:
        return _render_events()

    @app.get("/network")
    def network_page() -> str:
        return _render_network()

    @app.post("/ingest/jobs")
    def create_ingest_job() -> Any:
        media_label = request.form.get("media_label", "").strip()
        source_paths_text = request.form.get("source_paths", "")
        source_paths = [line.strip() for line in source_paths_text.splitlines() if line.strip()]
        form_data = {"media_label": media_label, "source_paths": source_paths_text}

        if not media_label:
            return _render_overview(ingest_error="Media label is required.", form_data=form_data)
        if not source_paths:
            return _render_overview(
                ingest_error="At least one source path is required.",
                form_data=form_data,
            )

        try:
            state = daemon_get(daemon_base_url, "/state")
        except httpx.HTTPError as exc:
            return _render_overview(
                ingest_error="Cannot start ingest because daemon readiness could not be confirmed.",
                ingest_error_detail=_describe_http_error(exc),
                form_data=form_data,
            )

        ingest_gate = _build_ingest_gate(state)
        if not ingest_gate["can_start"]:
            return _render_overview(
                ingest_error=(
                    "Cannot start ingest while daemon state is "
                    f"{ingest_gate['current_state']}. {ingest_gate['operator_action']}"
                ),
                form_data=form_data,
            )

        try:
            created = daemon_post(
                daemon_base_url,
                "/ingest/jobs",
                {"media_label": media_label, "source_paths": source_paths},
            )
        except httpx.HTTPStatusError as exc:
            validation_error, validation_detail = _format_ingest_source_validation_error(exc)
            if validation_error:
                return _render_overview(
                    ingest_error=validation_error,
                    ingest_error_detail=validation_detail,
                    form_data=form_data,
                )
            if exc.response.status_code == 409:
                conflict_state: dict[str, Any] | None = None
                try:
                    conflict_state = daemon_get(daemon_base_url, "/state")
                except httpx.HTTPError:
                    conflict_state = state
                conflict_gate = _build_ingest_gate(conflict_state)
                return _render_overview(
                    ingest_error=(
                        "Daemon rejected ingest creation because it is not ready yet. "
                        f"Current state: {conflict_gate['current_state']}. {conflict_gate['operator_action']}"
                    ),
                    ingest_error_detail=_describe_http_error(exc),
                    form_data=form_data,
                )
            return _render_overview(
                ingest_error="Daemon failed to create ingest job.",
                ingest_error_detail=_describe_http_error(exc),
                form_data=form_data,
            )
        except httpx.HTTPError as exc:
            return _render_overview(
                ingest_error="Failed to create ingest job due to daemon communication error.",
                ingest_error_detail=_describe_http_error(exc),
                form_data=form_data,
            )

        discovered_count = created.get("discovered_count")
        filtered_count = int(created.get("filtered_count", 0) or 0)
        filtered_sources = created.get("filtered_sources")
        if discovered_count is None:
            notice = f"Created ingest job #{created['job_id']}."
        else:
            notice = f"Created ingest job #{created['job_id']} with {discovered_count} discovered file(s)."
        notice_detail = None
        if filtered_count > 0:
            notice = f"{notice} Skipped {filtered_count} file(s) by the v1 ingest policy."
            if isinstance(filtered_sources, list) and filtered_sources:
                detail_lines: list[str] = []
                for item in filtered_sources:
                    if not isinstance(item, dict):
                        continue
                    source_path = str(item.get("source_path", "")).strip()
                    reason = str(item.get("reason", "")).strip()
                    if source_path and reason:
                        detail_lines.append(f"{source_path}: {reason}")
                if detail_lines:
                    notice_detail = "\n".join(detail_lines)
        if _is_ajax_request():
            return _render_overview(ingest_notice=notice, ingest_notice_detail=notice_detail)
        return redirect(url_for("job_detail", job_id=created["job_id"]))

    @app.post("/actions/daemon/tick")
    def tick_daemon() -> Response:
        return_to = request.form.get("return_to", "").strip()
        try:
            outcome = daemon_post(
                daemon_base_url,
                "/daemon/tick",
                {},
                timeout_seconds=DEFAULT_TICK_TIMEOUT_SECONDS,
            )
        except httpx.TimeoutException:
            if return_to and return_to.startswith("/jobs/"):
                return redirect(return_to)
            return _render_overview(
                operator_notice="Daemon action is still running. Refreshing status...",
                operator_notice_pending=True,
                auto_refresh_ms=DEFAULT_TICK_STATUS_REFRESH_MS,
            )
        except httpx.HTTPError as exc:
            if return_to and return_to.startswith("/jobs/"):
                try:
                    job_id = int(return_to.rsplit("/", 1)[-1])
                except ValueError:
                    return _render_overview(
                        ingest_error="Failed to run daemon tick.",
                        ingest_error_detail=_describe_http_error(exc),
                    )
                return make_response(
                    _render_job_detail(
                        job_id,
                        action_error=f"Failed to run daemon tick: {_describe_http_error(exc)}",
                    )
                )
            return _render_overview(
                ingest_error="Failed to run daemon tick.",
                ingest_error_detail=_describe_http_error(exc),
            )

        if outcome.get("handled"):
            if outcome.get("already_progressing"):
                next_state = outcome.get("next_state", outcome.get("state", "UNKNOWN"))
                message = (
                    "Daemon is already progressing in state "
                    f"{next_state}; wait and refresh instead of running manual ticks."
                )
            else:
                next_state = outcome.get("next_state", outcome.get("state", "UNKNOWN"))
                message = f"Daemon tick completed in state {next_state}."
        else:
            message = f"Daemon tick was a no-op in state {outcome.get('state', 'UNKNOWN')}."

        if return_to and return_to.startswith("/jobs/"):
            try:
                job_id = int(return_to.rsplit("/", 1)[-1])
            except ValueError:
                return _render_overview(operator_notice=message)
            return make_response(
                _render_job_detail(
                    job_id,
                    action_notice=message,
                    action_notice_pending=bool(outcome.get("already_progressing")),
                )
            )
        return _render_overview(
            operator_notice=message,
            operator_notice_pending=bool(outcome.get("already_progressing")),
        )

    @app.post("/actions/retry-upload")
    def retry_error_upload() -> Response:
        file_id = request.form.get("file_id", type=int)
        job_id = request.form.get("job_id", type=int)
        if file_id is None:
            if job_id is not None:
                return make_response(
                    _render_job_detail(
                        job_id,
                        action_error="Missing file_id for retry action.",
                    )
                )
            return _render_overview(ingest_error="Missing file_id for retry action.")
        try:
            outcome = daemon_post(daemon_base_url, f"/ingest/files/{file_id}/retry-upload", {})
        except httpx.HTTPError as exc:
            detail = _describe_http_error(exc)
            if job_id is not None:
                return make_response(
                    _render_job_detail(
                        job_id,
                        action_error=f"Failed to requeue file #{file_id} for upload: {detail}",
                    )
                )
            return _render_overview(
                ingest_error=f"Failed to requeue file #{file_id} for upload.",
                ingest_error_detail=detail,
            )

        next_state = outcome.get("next_state", "UPLOAD_PREPARE")
        message = f"File #{file_id} requeued for upload; daemon moved to {next_state}."
        if job_id is not None:
            return make_response(_render_job_detail(job_id, action_notice=message))
        return _render_overview(operator_notice=message)

    @app.post("/network/scan")
    def scan_wifi() -> Any:
        try:
            network_scan()
        except (subprocess.CalledProcessError, FileNotFoundError) as exc:
            return _render_network(network_error=_format_network_error("scan Wi-Fi", exc))
        return redirect(url_for("network_page"))

    @app.post("/network/connect")
    def connect_wifi() -> str:
        ssid = request.form.get("ssid", "").strip()
        password = request.form.get("password", "")
        network_form = {"ssid": ssid, "password": password}

        if not ssid:
            return _render_network(
                network_error="SSID is required.",
                network_form_data=network_form,
            )

        try:
            network_connect(ssid, password or None)
        except (subprocess.CalledProcessError, FileNotFoundError) as exc:
            return _render_network(
                network_error=_format_network_error("connect Wi-Fi", exc),
                network_form_data=network_form,
            )

        return redirect(url_for("network_page"))

    @app.get("/jobs/<int:job_id>")
    def job_detail(job_id: int) -> str:
        return _render_job_detail(job_id)

    return app
