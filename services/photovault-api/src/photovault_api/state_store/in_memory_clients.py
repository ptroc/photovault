"""Client helpers for InMemoryUploadStateStore."""

from __future__ import annotations

from .records import ClientHeartbeatRecord, ClientRecord


def upsert_client_pending(
    self,
    *,
    client_id: str,
    display_name: str,
    enrolled_at_utc: str,
) -> ClientRecord:
    with self._lock:
        existing = self.clients.get(client_id)
        first_seen = existing.first_seen_at_utc if existing is not None else enrolled_at_utc
        enrollment_status = "pending" if existing is None else existing.enrollment_status
        keep_existing_identity = existing is not None and existing.enrollment_status != "pending"
        approved_at_utc = (
            existing.approved_at_utc if keep_existing_identity and existing is not None else None
        )
        revoked_at_utc = (
            existing.revoked_at_utc if keep_existing_identity and existing is not None else None
        )
        auth_token = existing.auth_token if keep_existing_identity and existing is not None else None
        updated = ClientRecord(
            client_id=client_id,
            display_name=display_name,
            enrollment_status=enrollment_status,
            first_seen_at_utc=first_seen,
            last_enrolled_at_utc=enrolled_at_utc,
            approved_at_utc=approved_at_utc,
            revoked_at_utc=revoked_at_utc,
            auth_token=auth_token,
        )
        self.clients[client_id] = updated
        return updated

def get_client(self, client_id: str) -> ClientRecord | None:
    with self._lock:
        return self.clients.get(client_id)

def list_clients(self, *, limit: int, offset: int) -> tuple[int, list[ClientRecord]]:
    with self._lock:
        rows = sorted(
            self.clients.values(),
            key=lambda client: (client.first_seen_at_utc, client.client_id),
            reverse=True,
        )
        total = len(rows)
        return total, rows[offset : offset + limit]

def approve_client(
    self,
    *,
    client_id: str,
    approved_at_utc: str,
    auth_token: str,
) -> ClientRecord | None:
    with self._lock:
        existing = self.clients.get(client_id)
        if existing is None:
            return None
        updated = ClientRecord(
            client_id=existing.client_id,
            display_name=existing.display_name,
            enrollment_status="approved",
            first_seen_at_utc=existing.first_seen_at_utc,
            last_enrolled_at_utc=existing.last_enrolled_at_utc,
            approved_at_utc=approved_at_utc,
            revoked_at_utc=None,
            auth_token=auth_token,
        )
        self.clients[client_id] = updated
        return updated

def revoke_client(
    self,
    *,
    client_id: str,
    revoked_at_utc: str,
) -> ClientRecord | None:
    with self._lock:
        existing = self.clients.get(client_id)
        if existing is None:
            return None
        updated = ClientRecord(
            client_id=existing.client_id,
            display_name=existing.display_name,
            enrollment_status="revoked",
            first_seen_at_utc=existing.first_seen_at_utc,
            last_enrolled_at_utc=existing.last_enrolled_at_utc,
            approved_at_utc=existing.approved_at_utc,
            revoked_at_utc=revoked_at_utc,
            auth_token=existing.auth_token,
        )
        self.clients[client_id] = updated
        return updated

def upsert_client_heartbeat(
    self,
    *,
    client_id: str,
    last_seen_at_utc: str,
    daemon_state: str,
    workload_status: str,
    active_job_id: int | None,
    active_job_label: str | None,
    active_job_status: str | None,
    active_job_ready_to_upload: int | None,
    active_job_uploaded: int | None,
    active_job_retrying: int | None,
    active_job_total_files: int | None,
    active_job_non_terminal_files: int | None,
    active_job_error_files: int | None,
    active_job_blocking_reason: str | None,
    retry_pending_count: int | None,
    retry_next_at_utc: str | None,
    retry_reason: str | None,
    auth_block_reason: str | None,
    recent_error_category: str | None,
    recent_error_message: str | None,
    recent_error_at_utc: str | None,
    updated_at_utc: str,
) -> ClientHeartbeatRecord:
    with self._lock:
        record = ClientHeartbeatRecord(
            client_id=client_id,
            last_seen_at_utc=last_seen_at_utc,
            daemon_state=daemon_state,
            workload_status=workload_status,
            active_job_id=active_job_id,
            active_job_label=active_job_label,
            active_job_status=active_job_status,
            active_job_ready_to_upload=active_job_ready_to_upload,
            active_job_uploaded=active_job_uploaded,
            active_job_retrying=active_job_retrying,
            active_job_total_files=active_job_total_files,
            active_job_non_terminal_files=active_job_non_terminal_files,
            active_job_error_files=active_job_error_files,
            active_job_blocking_reason=active_job_blocking_reason,
            retry_pending_count=retry_pending_count,
            retry_next_at_utc=retry_next_at_utc,
            retry_reason=retry_reason,
            auth_block_reason=auth_block_reason,
            recent_error_category=recent_error_category,
            recent_error_message=recent_error_message,
            recent_error_at_utc=recent_error_at_utc,
            updated_at_utc=updated_at_utc,
        )
        self.client_heartbeats[client_id] = record
        return record

def get_client_heartbeat(self, client_id: str) -> ClientHeartbeatRecord | None:
    with self._lock:
        return self.client_heartbeats.get(client_id)
