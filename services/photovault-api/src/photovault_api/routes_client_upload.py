"""Client, upload, tombstone-report, and storage route registration."""

import hashlib
import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from .models import (
    BootstrapEnrollRequest,
    BootstrapEnrollResponse,
    ClientEnrollmentStatus,
    ClientHeartbeatRequest,
    ClientHeartbeatResponse,
    ClientWorkloadStatus,
    HandshakeDecision,
    HandshakeFileResult,
    IndexStorageResponse,
    MetadataHandshakeRequest,
    MetadataHandshakeResponse,
    UploadContentResponse,
    VerifyRequest,
    VerifyResponse,
)
from .state_store import StorageIndexRunRecord, UploadStateStore


class ClientTombstoneReportItem(BaseModel):
    sha256_hex: str
    relative_path: str
    trashed_at_utc: str


class ClientTombstoneReportRequest(BaseModel):
    sha256_hex: list[str] = Field(min_length=0, max_length=500)


class ClientTombstoneReportResponse(BaseModel):
    tombstoned: list[ClientTombstoneReportItem]


def register_client_upload_routes(app: FastAPI, helpers: Any) -> None:
    @app.post("/v1/client/enroll/bootstrap", response_model=BootstrapEnrollResponse)
    def bootstrap_enroll(payload: BootstrapEnrollRequest) -> BootstrapEnrollResponse:
        configured_bootstrap_token = str(app.state.bootstrap_token)
        if not configured_bootstrap_token:
            raise HTTPException(status_code=503, detail="bootstrap enrollment is disabled")
        if payload.bootstrap_token != configured_bootstrap_token:
            raise HTTPException(status_code=401, detail="invalid bootstrap token")

        store: UploadStateStore = app.state.upload_state_store
        now = helpers.datetime.now(helpers.UTC).isoformat()
        record = store.upsert_client_pending(
            client_id=payload.client_id,
            display_name=payload.display_name,
            enrolled_at_utc=now,
        )
        return BootstrapEnrollResponse(
            client_id=record.client_id,
            display_name=record.display_name,
            enrollment_status=ClientEnrollmentStatus(record.enrollment_status),
            auth_token=record.auth_token,
            first_seen_at_utc=record.first_seen_at_utc,
            last_enrolled_at_utc=record.last_enrolled_at_utc,
        )

    @app.post("/v1/client/heartbeat", response_model=ClientHeartbeatResponse)
    def client_heartbeat(payload: ClientHeartbeatRequest, request: Request) -> ClientHeartbeatResponse:
        store: UploadStateStore = app.state.upload_state_store
        client = helpers._require_approved_client(request, store)
        updated = store.upsert_client_heartbeat(
            client_id=client.client_id,
            last_seen_at_utc=payload.last_seen_at_utc.astimezone(helpers.UTC).isoformat(),
            daemon_state=payload.daemon_state,
            workload_status=payload.workload_status.value,
            active_job_id=payload.active_job.job_id if payload.active_job is not None else None,
            active_job_label=payload.active_job.media_label if payload.active_job is not None else None,
            active_job_status=payload.active_job.job_status if payload.active_job is not None else None,
            active_job_ready_to_upload=(
                payload.active_job.ready_to_upload if payload.active_job is not None else None
            ),
            active_job_uploaded=payload.active_job.uploaded if payload.active_job is not None else None,
            active_job_retrying=payload.active_job.retrying if payload.active_job is not None else None,
            active_job_total_files=payload.active_job.total_files if payload.active_job is not None else None,
            active_job_non_terminal_files=(
                payload.active_job.non_terminal_files if payload.active_job is not None else None
            ),
            active_job_error_files=payload.active_job.error_files if payload.active_job is not None else None,
            active_job_blocking_reason=(
                payload.active_job.blocking_reason if payload.active_job is not None else None
            ),
            retry_pending_count=(
                payload.retry_backoff.pending_count if payload.retry_backoff is not None else None
            ),
            retry_next_at_utc=(
                payload.retry_backoff.next_retry_at_utc.astimezone(helpers.UTC).isoformat()
                if payload.retry_backoff is not None and payload.retry_backoff.next_retry_at_utc is not None
                else None
            ),
            retry_reason=payload.retry_backoff.reason if payload.retry_backoff is not None else None,
            auth_block_reason=payload.auth_block_reason,
            recent_error_category=(
                payload.recent_error.category if payload.recent_error is not None else None
            ),
            recent_error_message=(
                payload.recent_error.message if payload.recent_error is not None else None
            ),
            recent_error_at_utc=(
                payload.recent_error.created_at_utc.astimezone(helpers.UTC).isoformat()
                if payload.recent_error is not None
                else None
            ),
            updated_at_utc=helpers.datetime.now(helpers.UTC).isoformat(),
        )
        return ClientHeartbeatResponse(
            status="RECORDED",
            client_id=updated.client_id,
            last_seen_at_utc=updated.last_seen_at_utc,
            daemon_state=updated.daemon_state,
            workload_status=ClientWorkloadStatus(updated.workload_status),
        )

    @app.post("/v1/upload/metadata-handshake", response_model=MetadataHandshakeResponse)
    def metadata_handshake(payload: MetadataHandshakeRequest, request: Request) -> MetadataHandshakeResponse:
        results: list[HandshakeFileResult] = []
        store: UploadStateStore = app.state.upload_state_store
        helpers._require_approved_client(request, store)
        known_shas = store.has_shas([file_item.sha256_hex for file_item in payload.files])

        for file_item in payload.files:
            decision = (
                HandshakeDecision.ALREADY_EXISTS
                if file_item.sha256_hex in known_shas
                else HandshakeDecision.UPLOAD_REQUIRED
            )
            results.append(
                HandshakeFileResult(
                    client_file_id=file_item.client_file_id,
                    decision=decision,
                )
            )

        return MetadataHandshakeResponse(results=results)

    @app.put("/v1/upload/content/{sha256_hex}", response_model=UploadContentResponse)
    async def upload_content(sha256_hex: str, request: Request) -> UploadContentResponse:
        if len(sha256_hex) != 64:
            raise HTTPException(status_code=400, detail="sha256_hex must be 64 hex characters")

        store: UploadStateStore = app.state.upload_state_store
        helpers._require_approved_client(request, store)
        if store.has_sha(sha256_hex):
            return UploadContentResponse(status="ALREADY_EXISTS")

        raw_size = request.headers.get("x-size-bytes")
        if raw_size is None:
            raise HTTPException(status_code=400, detail="missing x-size-bytes header")
        try:
            expected_size = int(raw_size)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="invalid x-size-bytes header") from exc
        if expected_size < 0:
            raise HTTPException(status_code=400, detail="x-size-bytes must be non-negative")

        raw_job_name = request.headers.get("x-job-name")
        if raw_job_name is None or not raw_job_name.strip():
            raise HTTPException(status_code=400, detail="missing x-job-name header")
        raw_original_filename = request.headers.get("x-original-filename")
        if raw_original_filename is None or not raw_original_filename.strip():
            raise HTTPException(status_code=400, detail="missing x-original-filename header")

        content = await request.body()
        if len(content) != expected_size:
            raise HTTPException(status_code=400, detail="payload size does not match x-size-bytes")

        observed_sha = hashlib.sha256(content).hexdigest()
        if observed_sha != sha256_hex:
            raise HTTPException(status_code=400, detail="payload sha256 mismatch")

        storage_root_path = Path(app.state.storage_root)
        temp_relative_path = f".temp_uploads/{sha256_hex}.upload"
        temp_path = storage_root_path / temp_relative_path
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path.write_bytes(content)
        received_at_utc = helpers.datetime.now(helpers.UTC).isoformat()
        store.upsert_temp_upload(
            sha256_hex=sha256_hex,
            size_bytes=expected_size,
            temp_relative_path=temp_relative_path,
            job_name=raw_job_name,
            original_filename=raw_original_filename,
            received_at_utc=received_at_utc,
        )
        return UploadContentResponse(status="STORED_TEMP")

    @app.post("/v1/upload/verify", response_model=VerifyResponse)
    def verify_upload(payload: VerifyRequest, request: Request) -> VerifyResponse:
        store: UploadStateStore = app.state.upload_state_store
        helpers._require_approved_client(request, store)

        if store.is_sha_tombstoned(payload.sha256_hex):
            raise HTTPException(
                status_code=409,
                detail={"code": "sha_tombstoned", "sha256_hex": payload.sha256_hex},
            )

        if store.has_sha(payload.sha256_hex):
            return VerifyResponse(status="ALREADY_EXISTS")

        upload_row = store.get_temp_upload(payload.sha256_hex)
        if upload_row is None:
            return VerifyResponse(status="VERIFY_FAILED")

        storage_root_path = Path(app.state.storage_root)
        temp_path = storage_root_path / upload_row.temp_relative_path
        if not temp_path.is_file():
            return VerifyResponse(status="VERIFY_FAILED")
        observed_size = temp_path.stat().st_size
        if upload_row.size_bytes != payload.size_bytes or observed_size != payload.size_bytes:
            return VerifyResponse(status="VERIFY_FAILED")
        if helpers._compute_sha256(temp_path) != payload.sha256_hex:
            return VerifyResponse(status="VERIFY_FAILED")

        received_at = helpers.datetime.fromisoformat(upload_row.received_at_utc)
        year_part = f"{received_at.year:04d}"
        month_part = f"{received_at.month:02d}"
        job_part = helpers._sanitize_component(upload_row.job_name, default_value="unknown_job")
        original_name = helpers._sanitize_component(
            upload_row.original_filename,
            default_value=f"{payload.sha256_hex}.bin",
        )
        base_relative_path = Path(year_part) / month_part / job_part / original_name

        target_relative_path = base_relative_path
        target_path = storage_root_path / target_relative_path
        if target_path.exists():
            existing_sha = helpers._compute_sha256(target_path)
            if existing_sha != payload.sha256_hex:
                base_stem = Path(original_name).stem
                suffix = Path(original_name).suffix
                fallback_name = f"{base_stem}__{payload.sha256_hex[:12]}{suffix}"
                target_relative_path = Path(year_part) / month_part / job_part / fallback_name
                target_path = storage_root_path / target_relative_path
                if target_path.exists() and helpers._compute_sha256(target_path) != payload.sha256_hex:
                    return VerifyResponse(status="VERIFY_FAILED")

        target_path.parent.mkdir(parents=True, exist_ok=True)
        if not target_path.exists():
            os.replace(temp_path, target_path)
        else:
            temp_path.unlink(missing_ok=True)

        now = helpers.datetime.now(helpers.UTC).isoformat()
        store.mark_sha_verified(payload.sha256_hex)
        helpers._upsert_storage_and_catalog_record(
            store=store,
            relative_path=str(target_relative_path.as_posix()),
            sha256_hex=payload.sha256_hex,
            size_bytes=payload.size_bytes,
            source_kind="upload_verify",
            seen_at_utc=now,
            provenance_job_name=upload_row.job_name,
            provenance_original_filename=upload_row.original_filename,
        )
        helpers._attempt_media_extraction(
            store=store,
            storage_root_path=storage_root_path,
            relative_path=str(target_relative_path.as_posix()),
        )
        store.remove_temp_upload(payload.sha256_hex)
        return VerifyResponse(status="VERIFIED")

    @app.post("/v1/storage/index", response_model=IndexStorageResponse)
    def index_storage() -> IndexStorageResponse:
        store: UploadStateStore = app.state.upload_state_store
        storage_root_path = Path(app.state.storage_root)
        scanned_files = 0
        indexed_files = 0
        new_sha_entries = 0
        existing_sha_matches = 0
        path_conflicts = 0
        errors = 0
        now = helpers.datetime.now(helpers.UTC).isoformat()

        for candidate in helpers._iter_storage_files(storage_root_path):
            relative_path = candidate.relative_to(storage_root_path)
            scanned_files += 1
            try:
                observed_sha = helpers._compute_sha256(candidate)
                size_bytes = candidate.stat().st_size
                existing = store.get_stored_file_by_path(str(relative_path.as_posix()))
                if existing is not None and existing.sha256_hex != observed_sha:
                    path_conflicts += 1
                    store.record_path_conflict(
                        relative_path=str(relative_path.as_posix()),
                        previous_sha256_hex=existing.sha256_hex,
                        current_sha256_hex=observed_sha,
                        detected_at_utc=now,
                    )
                if store.mark_sha_verified(observed_sha):
                    new_sha_entries += 1
                else:
                    existing_sha_matches += 1
                helpers._upsert_storage_and_catalog_record(
                    store=store,
                    relative_path=str(relative_path.as_posix()),
                    sha256_hex=observed_sha,
                    size_bytes=size_bytes,
                    source_kind="index_scan",
                    seen_at_utc=now,
                )
                helpers._attempt_media_extraction(
                    store=store,
                    storage_root_path=storage_root_path,
                    relative_path=str(relative_path.as_posix()),
                )
                indexed_files += 1
            except OSError:
                errors += 1

        result = IndexStorageResponse(
            scanned_files=scanned_files,
            indexed_files=indexed_files,
            new_sha_entries=new_sha_entries,
            existing_sha_matches=existing_sha_matches,
            path_conflicts=path_conflicts,
            errors=errors,
        )
        store.record_storage_index_run(
            StorageIndexRunRecord(
                scanned_files=result.scanned_files,
                indexed_files=result.indexed_files,
                new_sha_entries=result.new_sha_entries,
                existing_sha_matches=result.existing_sha_matches,
                path_conflicts=result.path_conflicts,
                errors=result.errors,
                completed_at_utc=now,
            )
        )
        return result

    @app.post("/v1/client/tombstone-report", response_model=ClientTombstoneReportResponse)
    def client_tombstone_report(
        payload: ClientTombstoneReportRequest, request: Request
    ) -> ClientTombstoneReportResponse:
        store: UploadStateStore = app.state.upload_state_store
        helpers._require_approved_client(request, store)

        tombstones = store.list_sha_tombstones(payload.sha256_hex)
        items = [
            ClientTombstoneReportItem(
                sha256_hex=ts.sha256_hex,
                relative_path=ts.relative_path,
                trashed_at_utc=ts.trashed_at_utc,
            )
            for ts in tombstones
        ]
        return ClientTombstoneReportResponse(tombstoned=items)
