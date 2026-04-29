"""Job and file-centric view models for the client UI."""

from typing import Any

_REMOTE_ALREADY_EXISTS_STATUSES = {"DUPLICATE_SHA_GLOBAL"}
_UPLOAD_REQUIRED_STATUSES = {
    "READY_TO_UPLOAD",
    "UPLOADED",
    "VERIFY_RUNNING",
    "VERIFIED_REMOTE",
    "ERROR_FILE",
}
_REMOTE_TERMINAL_STATUSES = {"VERIFIED_REMOTE", "DUPLICATE_SHA_GLOBAL"}
_IGNORED_FILE_STATUSES = {"DUPLICATE_SHA_GLOBAL", "DUPLICATE_SHA_LOCAL", "DUPLICATE_SESSION_SHA"}
_ERROR_FILE_STATUSES = {"ERROR_FILE", "QUARANTINED_LOCAL"}
_PAUSED_ERROR_JOB_STATUSES = {"ERROR_FILE", "ERROR_JOB", "PAUSED_STORAGE"}
_REMOTE_COMPLETE_JOB_STATUSES = {"JOB_COMPLETE_REMOTE", "JOB_COMPLETE_LOCAL"}
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
