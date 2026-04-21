"""Server-side API skeleton for photovault."""

import hashlib
import os
import re
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, Field

from photovault_api.state_store import (
    InMemoryUploadStateStore,
    PostgresUploadStateStore,
    StorageIndexRunRecord,
    StorageSummary,
    UploadStateStore,
)


class HandshakeDecision(StrEnum):
    ALREADY_EXISTS = "ALREADY_EXISTS"
    UPLOAD_REQUIRED = "UPLOAD_REQUIRED"


class HandshakeFileRequest(BaseModel):
    client_file_id: int = Field(ge=1)
    sha256_hex: str = Field(min_length=64, max_length=64)
    size_bytes: int = Field(ge=0)


class MetadataHandshakeRequest(BaseModel):
    files: list[HandshakeFileRequest] = Field(min_length=1)


class HandshakeFileResult(BaseModel):
    client_file_id: int
    decision: HandshakeDecision


class MetadataHandshakeResponse(BaseModel):
    results: list[HandshakeFileResult]


class UploadContentResponse(BaseModel):
    status: str


class VerifyRequest(BaseModel):
    sha256_hex: str = Field(min_length=64, max_length=64)
    size_bytes: int = Field(ge=0)


class VerifyResponse(BaseModel):
    status: str


class IndexStorageResponse(BaseModel):
    scanned_files: int
    indexed_files: int
    new_sha_entries: int
    existing_sha_matches: int
    path_conflicts: int
    errors: int


class AdminOverviewResponse(BaseModel):
    total_known_sha256: int
    total_stored_files: int
    indexed_files: int
    uploaded_files: int
    duplicate_file_paths: int
    recent_indexed_files_24h: int
    recent_uploaded_files_24h: int
    last_indexed_at_utc: str | None
    last_uploaded_at_utc: str | None


class AdminFileItem(BaseModel):
    relative_path: str
    sha256_hex: str
    size_bytes: int
    source_kind: str
    first_seen_at_utc: str
    last_seen_at_utc: str


class AdminFileListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[AdminFileItem]


class LatestIndexRunResponse(BaseModel):
    scanned_files: int
    indexed_files: int
    new_sha_entries: int
    existing_sha_matches: int
    path_conflicts: int
    errors: int
    completed_at_utc: str


class LatestIndexRunEnvelope(BaseModel):
    latest_run: LatestIndexRunResponse | None


class DuplicateShaGroupItem(BaseModel):
    sha256_hex: str
    file_count: int
    first_seen_at_utc: str
    last_seen_at_utc: str
    relative_paths: list[str]


class DuplicateShaGroupListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[DuplicateShaGroupItem]


class PathConflictItem(BaseModel):
    relative_path: str
    previous_sha256_hex: str
    current_sha256_hex: str
    detected_at_utc: str


class PathConflictListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[PathConflictItem]


def _sanitize_component(raw_value: str, *, default_value: str) -> str:
    normalized = raw_value.strip()
    if not normalized:
        return default_value
    normalized = normalized.replace("\\", "_").replace("/", "_")
    normalized = re.sub(r"[^A-Za-z0-9._ -]+", "_", normalized)
    normalized = normalized.replace(" ", "_")
    normalized = re.sub(r"_+", "_", normalized)
    normalized = normalized.strip("._-")
    return normalized or default_value


def _compute_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _iter_storage_files(storage_root_path: Path) -> list[Path]:
    candidates = [
        candidate
        for candidate in storage_root_path.rglob("*")
        if candidate.is_file() and ".temp_uploads" not in candidate.parts
    ]
    return sorted(
        candidates,
        key=lambda candidate: candidate.relative_to(storage_root_path).as_posix(),
    )


def create_app(
    initial_known_sha256: set[str] | None = None,
    *,
    state_store: UploadStateStore | None = None,
    database_url: str | None = None,
    storage_root: str | Path | None = None,
) -> FastAPI:
    resolved_storage_root = storage_root or os.getenv("PHOTOVAULT_API_STORAGE_ROOT")
    if not resolved_storage_root:
        raise RuntimeError("PHOTOVAULT_API_STORAGE_ROOT must be set")
    storage_root_path = Path(resolved_storage_root).expanduser().resolve()
    temp_root = storage_root_path / ".temp_uploads"
    temp_root.mkdir(parents=True, exist_ok=True)

    app = FastAPI(title="photovault-api", version="0.1.0")
    if state_store is not None:
        store = state_store
    else:
        resolved_url = database_url or os.getenv("PHOTOVAULT_API_DATABASE_URL")
        if resolved_url:
            store = PostgresUploadStateStore(database_url=resolved_url)
        else:
            store = InMemoryUploadStateStore(known_sha256=set(initial_known_sha256 or set()))
    store.initialize()
    app.state.upload_state_store = store
    app.state.storage_root = storage_root_path
    app.state.storage_temp_root = temp_root

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/v1/upload/metadata-handshake", response_model=MetadataHandshakeResponse)
    def metadata_handshake(payload: MetadataHandshakeRequest) -> MetadataHandshakeResponse:
        results: list[HandshakeFileResult] = []
        store: UploadStateStore = app.state.upload_state_store
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

        temp_relative_path = f".temp_uploads/{sha256_hex}.upload"
        temp_path = storage_root_path / temp_relative_path
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path.write_bytes(content)
        received_at_utc = datetime.now(UTC).isoformat()
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
    def verify_upload(payload: VerifyRequest) -> VerifyResponse:
        store: UploadStateStore = app.state.upload_state_store

        if store.has_sha(payload.sha256_hex):
            return VerifyResponse(status="ALREADY_EXISTS")

        upload_row = store.get_temp_upload(payload.sha256_hex)
        if upload_row is None:
            return VerifyResponse(status="VERIFY_FAILED")

        temp_path = storage_root_path / upload_row.temp_relative_path
        if not temp_path.is_file():
            return VerifyResponse(status="VERIFY_FAILED")
        observed_size = temp_path.stat().st_size
        if upload_row.size_bytes != payload.size_bytes or observed_size != payload.size_bytes:
            return VerifyResponse(status="VERIFY_FAILED")
        if _compute_sha256(temp_path) != payload.sha256_hex:
            return VerifyResponse(status="VERIFY_FAILED")

        received_at = datetime.fromisoformat(upload_row.received_at_utc)
        year_part = f"{received_at.year:04d}"
        month_part = f"{received_at.month:02d}"
        job_part = _sanitize_component(upload_row.job_name, default_value="unknown_job")
        original_name = _sanitize_component(
            upload_row.original_filename,
            default_value=f"{payload.sha256_hex}.bin",
        )
        base_relative_path = Path(year_part) / month_part / job_part / original_name

        target_relative_path = base_relative_path
        target_path = storage_root_path / target_relative_path
        if target_path.exists():
            existing_sha = _compute_sha256(target_path)
            if existing_sha != payload.sha256_hex:
                base_stem = Path(original_name).stem
                suffix = Path(original_name).suffix
                fallback_name = f"{base_stem}__{payload.sha256_hex[:12]}{suffix}"
                target_relative_path = Path(year_part) / month_part / job_part / fallback_name
                target_path = storage_root_path / target_relative_path
                if target_path.exists() and _compute_sha256(target_path) != payload.sha256_hex:
                    return VerifyResponse(status="VERIFY_FAILED")

        target_path.parent.mkdir(parents=True, exist_ok=True)
        if not target_path.exists():
            os.replace(temp_path, target_path)
        else:
            temp_path.unlink(missing_ok=True)

        now = datetime.now(UTC).isoformat()
        store.mark_sha_verified(payload.sha256_hex)
        store.upsert_stored_file(
            relative_path=str(target_relative_path.as_posix()),
            sha256_hex=payload.sha256_hex,
            size_bytes=payload.size_bytes,
            source_kind="upload_verify",
            seen_at_utc=now,
        )
        store.remove_temp_upload(payload.sha256_hex)
        return VerifyResponse(status="VERIFIED")

    @app.post("/v1/storage/index", response_model=IndexStorageResponse)
    def index_storage() -> IndexStorageResponse:
        store: UploadStateStore = app.state.upload_state_store
        scanned_files = 0
        indexed_files = 0
        new_sha_entries = 0
        existing_sha_matches = 0
        path_conflicts = 0
        errors = 0
        now = datetime.now(UTC).isoformat()

        for candidate in _iter_storage_files(storage_root_path):
            relative_path = candidate.relative_to(storage_root_path)
            scanned_files += 1
            try:
                observed_sha = _compute_sha256(candidate)
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
                store.upsert_stored_file(
                    relative_path=str(relative_path.as_posix()),
                    sha256_hex=observed_sha,
                    size_bytes=size_bytes,
                    source_kind="index_scan",
                    seen_at_utc=now,
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

    @app.get("/v1/admin/overview", response_model=AdminOverviewResponse)
    def admin_overview() -> AdminOverviewResponse:
        store: UploadStateStore = app.state.upload_state_store
        summary: StorageSummary = store.summarize_storage()
        return AdminOverviewResponse(
            total_known_sha256=summary.total_known_sha256,
            total_stored_files=summary.total_stored_files,
            indexed_files=summary.indexed_files,
            uploaded_files=summary.uploaded_files,
            duplicate_file_paths=summary.duplicate_file_paths,
            recent_indexed_files_24h=summary.recent_indexed_files_24h,
            recent_uploaded_files_24h=summary.recent_uploaded_files_24h,
            last_indexed_at_utc=summary.last_indexed_at_utc,
            last_uploaded_at_utc=summary.last_uploaded_at_utc,
        )

    @app.get("/v1/admin/files", response_model=AdminFileListResponse)
    def admin_files(
        limit: int = Query(default=50, ge=1, le=200),
        offset: int = Query(default=0, ge=0),
    ) -> AdminFileListResponse:
        store: UploadStateStore = app.state.upload_state_store
        total, records = store.list_stored_files(limit=limit, offset=offset)
        return AdminFileListResponse(
            total=total,
            limit=limit,
            offset=offset,
            items=[
                AdminFileItem(
                    relative_path=record.relative_path,
                    sha256_hex=record.sha256_hex,
                    size_bytes=record.size_bytes,
                    source_kind=record.source_kind,
                    first_seen_at_utc=record.first_seen_at_utc,
                    last_seen_at_utc=record.last_seen_at_utc,
                )
                for record in records
            ],
        )

    @app.get("/v1/admin/duplicates", response_model=DuplicateShaGroupListResponse)
    def admin_duplicates(
        limit: int = Query(default=25, ge=1, le=100),
        offset: int = Query(default=0, ge=0),
    ) -> DuplicateShaGroupListResponse:
        store: UploadStateStore = app.state.upload_state_store
        total, groups = store.list_duplicate_sha_groups(limit=limit, offset=offset)
        return DuplicateShaGroupListResponse(
            total=total,
            limit=limit,
            offset=offset,
            items=[
                DuplicateShaGroupItem(
                    sha256_hex=group.sha256_hex,
                    file_count=group.file_count,
                    first_seen_at_utc=group.first_seen_at_utc,
                    last_seen_at_utc=group.last_seen_at_utc,
                    relative_paths=list(group.relative_paths),
                )
                for group in groups
            ],
        )

    @app.get("/v1/admin/path-conflicts", response_model=PathConflictListResponse)
    def admin_path_conflicts(
        limit: int = Query(default=25, ge=1, le=100),
        offset: int = Query(default=0, ge=0),
    ) -> PathConflictListResponse:
        store: UploadStateStore = app.state.upload_state_store
        total, records = store.list_path_conflicts(limit=limit, offset=offset)
        return PathConflictListResponse(
            total=total,
            limit=limit,
            offset=offset,
            items=[
                PathConflictItem(
                    relative_path=record.relative_path,
                    previous_sha256_hex=record.previous_sha256_hex,
                    current_sha256_hex=record.current_sha256_hex,
                    detected_at_utc=record.detected_at_utc,
                )
                for record in records
            ],
        )

    @app.get("/v1/admin/latest-index-run", response_model=LatestIndexRunEnvelope)
    def admin_latest_index_run() -> LatestIndexRunEnvelope:
        store: UploadStateStore = app.state.upload_state_store
        latest_run = store.get_latest_storage_index_run()
        if latest_run is None:
            return LatestIndexRunEnvelope(latest_run=None)
        return LatestIndexRunEnvelope(
            latest_run=LatestIndexRunResponse(
                scanned_files=latest_run.scanned_files,
                indexed_files=latest_run.indexed_files,
                new_sha_entries=latest_run.new_sha_entries,
                existing_sha_matches=latest_run.existing_sha_matches,
                path_conflicts=latest_run.path_conflicts,
                errors=latest_run.errors,
                completed_at_utc=latest_run.completed_at_utc,
            )
        )

    return app
