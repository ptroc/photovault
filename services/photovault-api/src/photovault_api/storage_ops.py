"""Catalog and storage operation helpers for photovault-api."""
from __future__ import annotations

import logging
import math
import re
from datetime import UTC, datetime
from pathlib import Path

from fastapi import HTTPException, Request

from .media import (
    _ALLOWED_EXTRACTION_STATUS,
    _ALLOWED_MEDIA_TYPE,
    _ALLOWED_ORIGIN_KIND,
    _ALLOWED_PREVIEW_CAPABILITY,
    _ALLOWED_PREVIEW_STATUS,
    _media_type_for_relative_path,
    _preview_capability_for_relative_path,
    _HEARTBEAT_ONLINE_MAX_AGE_SECONDS,
    _CLIENT_LIST_SCAN_MAX,
    _CLIENT_LIST_SCAN_PAGE_SIZE,
)
from .models import (
    AdminCatalogBackfillRunSummary,
    AdminCatalogItem,
    AdminClientItem,
    ClientEnrollmentStatus,
    ClientPresenceStatus,
    ClientWorkloadStatus,
    HeartbeatActiveJobSummary,
    HeartbeatRecentErrorSummary,
    HeartbeatRetryBackoffSummary,
)
from photovault_api.state_store import (
    CatalogBackfillRunRecord,
    ClientHeartbeatRecord,
    ClientRecord,
    MediaAssetRecord,
    UploadStateStore,
)

APP_LOGGER = logging.getLogger("photovault-api.app")



def _to_admin_catalog_item(record: object, *, is_rejected: bool = False) -> AdminCatalogItem:
    return AdminCatalogItem(
        relative_path=str(record.relative_path),
        sha256_hex=str(record.sha256_hex),
        size_bytes=int(record.size_bytes),
        media_type=_media_type_for_relative_path(str(record.relative_path)),
        preview_capability=_preview_capability_for_relative_path(str(record.relative_path)),
        origin_kind=str(record.origin_kind),
        last_observed_origin_kind=str(record.last_observed_origin_kind),
        provenance_job_name=(
            str(record.provenance_job_name) if record.provenance_job_name is not None else None
        ),
        provenance_original_filename=(
            str(record.provenance_original_filename)
            if record.provenance_original_filename is not None
            else None
        ),
        first_cataloged_at_utc=str(record.first_cataloged_at_utc),
        last_cataloged_at_utc=str(record.last_cataloged_at_utc),
        extraction_status=str(record.extraction_status),
        extraction_last_attempted_at_utc=(
            str(record.extraction_last_attempted_at_utc)
            if record.extraction_last_attempted_at_utc is not None
            else None
        ),
        extraction_last_succeeded_at_utc=(
            str(record.extraction_last_succeeded_at_utc)
            if record.extraction_last_succeeded_at_utc is not None
            else None
        ),
        extraction_last_failed_at_utc=(
            str(record.extraction_last_failed_at_utc)
            if record.extraction_last_failed_at_utc is not None
            else None
        ),
        extraction_failure_detail=(
            str(record.extraction_failure_detail) if record.extraction_failure_detail is not None else None
        ),
        preview_status=str(record.preview_status),
        preview_relative_path=(
            str(record.preview_relative_path) if record.preview_relative_path is not None else None
        ),
        preview_last_attempted_at_utc=(
            str(record.preview_last_attempted_at_utc)
            if record.preview_last_attempted_at_utc is not None
            else None
        ),
        preview_last_succeeded_at_utc=(
            str(record.preview_last_succeeded_at_utc)
            if record.preview_last_succeeded_at_utc is not None
            else None
        ),
        preview_last_failed_at_utc=(
            str(record.preview_last_failed_at_utc) if record.preview_last_failed_at_utc is not None else None
        ),
        preview_failure_detail=(
            str(record.preview_failure_detail) if record.preview_failure_detail is not None else None
        ),
        capture_timestamp_utc=(
            str(record.capture_timestamp_utc) if record.capture_timestamp_utc is not None else None
        ),
        camera_make=str(record.camera_make) if record.camera_make is not None else None,
        camera_model=str(record.camera_model) if record.camera_model is not None else None,
        image_width=int(record.image_width) if record.image_width is not None else None,
        image_height=int(record.image_height) if record.image_height is not None else None,
        orientation=int(record.orientation) if record.orientation is not None else None,
        lens_model=str(record.lens_model) if record.lens_model is not None else None,
        exposure_time_s=(
            float(getattr(record, "exposure_time_s", None))
            if getattr(record, "exposure_time_s", None) is not None
            else None
        ),
        f_number=(
            float(getattr(record, "f_number", None))
            if getattr(record, "f_number", None) is not None
            else None
        ),
        iso_speed=(
            int(getattr(record, "iso_speed", None))
            if getattr(record, "iso_speed", None) is not None
            else None
        ),
        focal_length_mm=(
            float(getattr(record, "focal_length_mm", None))
            if getattr(record, "focal_length_mm", None) is not None
            else None
        ),
        focal_length_35mm_mm=(
            int(getattr(record, "focal_length_35mm_mm", None))
            if getattr(record, "focal_length_35mm_mm", None) is not None
            else None
        ),
        is_favorite=bool(getattr(record, "is_favorite", False)),
        is_archived=bool(getattr(record, "is_archived", False)),
        is_rejected=bool(is_rejected),
    )


def _parse_boolean_filter(raw_value: str | None, *, field_name: str) -> bool | None:
    if raw_value is None:
        return None
    lowered = raw_value.strip().lower()
    if lowered == "":
        return None
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    raise HTTPException(status_code=400, detail=f"invalid {field_name} filter")


def _normalize_catalog_folder_prefix(raw_value: str | None) -> str | None:
    """Validate and normalize a catalog folder prefix.

    Accepts a forward-slash separated path (e.g. ``"2024/08"``) and returns
    it stripped of surrounding whitespace and trailing slashes. Rejects
    absolute paths, empty segments, ``..`` segments, and backslashes so the
    filter cannot be abused to reach outside of the managed catalog.
    """

    if raw_value is None:
        return None
    value = raw_value.strip()
    if value == "":
        return None
    if value.startswith("/") or "\\" in value:
        raise HTTPException(status_code=400, detail="invalid relative_path_prefix")
    trimmed = value.strip("/")
    if trimmed == "":
        raise HTTPException(status_code=400, detail="invalid relative_path_prefix")
    segments = trimmed.split("/")
    for segment in segments:
        if segment == "" or segment == "." or segment == "..":
            raise HTTPException(status_code=400, detail="invalid relative_path_prefix")
    return trimmed


def _validate_catalog_filter_selection(
    *,
    extraction_status: str | None = None,
    preview_status: str | None = None,
    origin_kind: str | None = None,
    media_type: str | None = None,
    preview_capability: str | None = None,
) -> None:
    if extraction_status is not None and extraction_status not in _ALLOWED_EXTRACTION_STATUS:
        raise HTTPException(status_code=400, detail="invalid extraction_status filter")
    if preview_status is not None and preview_status not in _ALLOWED_PREVIEW_STATUS:
        raise HTTPException(status_code=400, detail="invalid preview_status filter")
    if origin_kind is not None and origin_kind not in _ALLOWED_ORIGIN_KIND:
        raise HTTPException(status_code=400, detail="invalid origin_kind filter")
    if media_type is not None and media_type not in _ALLOWED_MEDIA_TYPE:
        raise HTTPException(status_code=400, detail="invalid media_type filter")
    if preview_capability is not None and preview_capability not in _ALLOWED_PREVIEW_CAPABILITY:
        raise HTTPException(status_code=400, detail="invalid preview_capability filter")


def _validate_backfill_target_statuses(
    *,
    target_statuses: list[str],
    allowed_statuses: set[str],
) -> list[str]:
    requested_statuses = list(dict.fromkeys(target_statuses))
    invalid_statuses = [status for status in requested_statuses if status not in allowed_statuses]
    if invalid_statuses:
        raise HTTPException(
            status_code=400,
            detail=f"invalid target_statuses: {','.join(invalid_statuses)}",
        )
    return requested_statuses


def _to_backfill_run_summary(record: CatalogBackfillRunRecord) -> AdminCatalogBackfillRunSummary:
    return AdminCatalogBackfillRunSummary(
        backfill_kind=record.backfill_kind,
        requested_statuses=list(record.requested_statuses),
        limit=record.limit_count,
        origin_kind=record.filter_origin_kind,
        media_type=record.filter_media_type,
        preview_capability=record.filter_preview_capability,
        cataloged_since_utc=record.filter_cataloged_since_utc,
        cataloged_before_utc=record.filter_cataloged_before_utc,
        selected_count=record.selected_count,
        processed_count=record.processed_count,
        succeeded_count=record.succeeded_count,
        failed_count=record.failed_count,
        remaining_pending_count=record.remaining_pending_count,
        remaining_failed_count=record.remaining_failed_count,
        completed_at_utc=record.completed_at_utc,
    )


def _heartbeat_presence_status(record: ClientHeartbeatRecord | None, *, now_utc: datetime) -> str:
    if record is None:
        return ClientPresenceStatus.UNKNOWN.value
    try:
        last_seen = datetime.fromisoformat(record.last_seen_at_utc)
        if last_seen.tzinfo is None:
            last_seen = last_seen.replace(tzinfo=UTC)
    except ValueError:
        return ClientPresenceStatus.UNKNOWN.value
    age_seconds = max(0.0, (now_utc - last_seen).total_seconds())
    if age_seconds <= _HEARTBEAT_ONLINE_MAX_AGE_SECONDS:
        return ClientPresenceStatus.ONLINE.value
    return ClientPresenceStatus.STALE.value


def _to_admin_client_item(
    record: ClientRecord,
    *,
    heartbeat: ClientHeartbeatRecord | None = None,
    now_utc: datetime,
) -> AdminClientItem:
    active_job_summary = None
    if heartbeat is not None and heartbeat.active_job_id is not None:
        job_label = heartbeat.active_job_label or "job"
        active_job_parts = [
            f"{job_label} (id={heartbeat.active_job_id}, status={heartbeat.active_job_status or 'unknown'}, "
            f"ready={heartbeat.active_job_ready_to_upload or 0}, "
            f"uploaded={heartbeat.active_job_uploaded or 0}, "
            f"retrying={heartbeat.active_job_retrying or 0}"
        ]
        if heartbeat.active_job_total_files is not None:
            active_job_parts.append(f", total={heartbeat.active_job_total_files}")
        if heartbeat.active_job_non_terminal_files is not None:
            active_job_parts.append(f", non_terminal={heartbeat.active_job_non_terminal_files}")
        if heartbeat.active_job_error_files is not None:
            active_job_parts.append(f", errors={heartbeat.active_job_error_files}")
        active_job_parts.append(")")
        if heartbeat.active_job_blocking_reason:
            active_job_parts.append(f" blocked={heartbeat.active_job_blocking_reason}")
        active_job_summary = "".join(active_job_parts)
    retry_backoff_summary = None
    if heartbeat is not None and heartbeat.retry_pending_count is not None:
        retry_backoff_summary = (
            f"pending={heartbeat.retry_pending_count}, next={heartbeat.retry_next_at_utc or 'n/a'}, "
            f"reason={heartbeat.retry_reason or 'n/a'}"
        )
    recent_error_summary = None
    if heartbeat is not None and heartbeat.recent_error_message is not None:
        recent_error_summary = (
            f"{heartbeat.recent_error_category or 'error'} at "
            f"{heartbeat.recent_error_at_utc or 'unknown'}: {heartbeat.recent_error_message}"
        )
    heartbeat_workload_status = None
    if heartbeat is not None:
        try:
            heartbeat_workload_status = ClientWorkloadStatus(heartbeat.workload_status)
        except ValueError:
            heartbeat_workload_status = None

    return AdminClientItem(
        client_id=record.client_id,
        display_name=record.display_name,
        enrollment_status=ClientEnrollmentStatus(record.enrollment_status),
        first_seen_at_utc=record.first_seen_at_utc,
        last_enrolled_at_utc=record.last_enrolled_at_utc,
        approved_at_utc=record.approved_at_utc,
        revoked_at_utc=record.revoked_at_utc,
        auth_token=record.auth_token,
        heartbeat_last_seen_at_utc=heartbeat.last_seen_at_utc if heartbeat is not None else None,
        heartbeat_presence_status=_heartbeat_presence_status(heartbeat, now_utc=now_utc),
        heartbeat_daemon_state=heartbeat.daemon_state if heartbeat is not None else None,
        heartbeat_workload_status=heartbeat_workload_status,
        heartbeat_active_job_summary=active_job_summary,
        heartbeat_retry_backoff_summary=retry_backoff_summary,
        heartbeat_auth_block_reason=heartbeat.auth_block_reason if heartbeat is not None else None,
        heartbeat_recent_error_summary=recent_error_summary,
    )


def _list_clients_for_admin_view(store: UploadStateStore) -> list[ClientRecord]:
    clients: list[ClientRecord] = []
    offset = 0
    while offset < _CLIENT_LIST_SCAN_MAX:
        batch_limit = min(_CLIENT_LIST_SCAN_PAGE_SIZE, _CLIENT_LIST_SCAN_MAX - offset)
        total, batch = store.list_clients(limit=batch_limit, offset=offset)
        if not batch:
            break
        clients.extend(batch)
        offset += len(batch)
        if len(clients) >= total:
            break
    return clients


def _presence_sort_rank(value: str) -> int:
    ranks = {
        ClientPresenceStatus.ONLINE.value: 0,
        ClientPresenceStatus.STALE.value: 1,
        ClientPresenceStatus.UNKNOWN.value: 2,
    }
    return ranks.get(value, 99)


def _workload_sort_rank(value: str | None) -> int:
    ranks = {
        ClientWorkloadStatus.WORKING.value: 0,
        ClientWorkloadStatus.BLOCKED.value: 1,
        ClientWorkloadStatus.WAITING.value: 2,
        ClientWorkloadStatus.IDLE.value: 3,
    }
    if value is None:
        return 99
    return ranks.get(value, 98)


def _require_approved_client(request: Request, store: UploadStateStore) -> ClientRecord:
    client_id = request.headers.get("x-photovault-client-id", "").strip()
    auth_token = request.headers.get("x-photovault-client-token", "").strip()
    if not client_id or not auth_token:
        raise HTTPException(status_code=401, detail="CLIENT_AUTH_REQUIRED")

    client = store.get_client(client_id)
    if client is None:
        raise HTTPException(status_code=401, detail="CLIENT_AUTH_INVALID")
    if client.enrollment_status == ClientEnrollmentStatus.PENDING.value:
        raise HTTPException(status_code=403, detail="CLIENT_PENDING_APPROVAL")
    if client.enrollment_status == ClientEnrollmentStatus.REVOKED.value:
        raise HTTPException(status_code=403, detail="CLIENT_REVOKED")
    if client.auth_token is None or client.auth_token != auth_token:
        raise HTTPException(status_code=401, detail="CLIENT_AUTH_INVALID")
    return client

