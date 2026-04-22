"""Server-side API skeleton for photovault."""

import hashlib
import os
import re
import secrets
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from PIL import Image, UnidentifiedImageError
from pydantic import BaseModel, Field

from photovault_api.state_store import (
    ClientRecord,
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


class AdminCatalogItem(BaseModel):
    relative_path: str
    sha256_hex: str
    size_bytes: int
    origin_kind: str
    last_observed_origin_kind: str
    provenance_job_name: str | None
    provenance_original_filename: str | None
    first_cataloged_at_utc: str
    last_cataloged_at_utc: str
    extraction_status: str
    extraction_last_attempted_at_utc: str | None
    extraction_last_succeeded_at_utc: str | None
    extraction_last_failed_at_utc: str | None
    extraction_failure_detail: str | None
    capture_timestamp_utc: str | None
    camera_make: str | None
    camera_model: str | None
    image_width: int | None
    image_height: int | None
    orientation: int | None
    lens_model: str | None


class AdminCatalogListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[AdminCatalogItem]


class AdminRetryExtractionRequest(BaseModel):
    relative_path: str = Field(min_length=1)


class AdminRetryExtractionResponse(BaseModel):
    item: AdminCatalogItem


class AdminBackfillExtractionRequest(BaseModel):
    target_statuses: list[str] = Field(default_factory=lambda: ["pending", "failed"], min_length=1)
    limit: int = Field(default=100, ge=1, le=500)


class AdminBackfillExtractionResponse(BaseModel):
    requested_statuses: list[str]
    selected_count: int
    processed_count: int
    succeeded_count: int
    failed_count: int
    items: list[AdminCatalogItem]


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


class ClientEnrollmentStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    REVOKED = "revoked"


class BootstrapEnrollRequest(BaseModel):
    client_id: str = Field(min_length=1, max_length=200)
    display_name: str = Field(min_length=1, max_length=200)
    bootstrap_token: str = Field(min_length=1)


class BootstrapEnrollResponse(BaseModel):
    client_id: str
    display_name: str
    enrollment_status: ClientEnrollmentStatus
    auth_token: str | None
    first_seen_at_utc: str
    last_enrolled_at_utc: str


class AdminClientItem(BaseModel):
    client_id: str
    display_name: str
    enrollment_status: ClientEnrollmentStatus
    first_seen_at_utc: str
    last_enrolled_at_utc: str
    approved_at_utc: str | None
    revoked_at_utc: str | None
    auth_token: str | None


class AdminClientListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[AdminClientItem]


class AdminClientActionResponse(BaseModel):
    item: AdminClientItem


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


def _catalog_origin_for_source_kind(source_kind: str) -> str:
    if source_kind == "upload_verify":
        return "uploaded"
    return "indexed"


def _normalize_exif_text(value: object) -> str | None:
    if isinstance(value, bytes):
        decoded = value.decode("utf-8", errors="ignore").strip()
        return decoded or None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return None


def _normalize_exif_int(value: object) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return int(stripped)
        except ValueError:
            return None
    return None


def _extract_capture_timestamp_utc(exif_map: object) -> str | None:
    if not hasattr(exif_map, "get"):
        return None

    # EXIF DateTimeOriginal and OffsetTimeOriginal.
    raw_timestamp = _normalize_exif_text(exif_map.get(36867) or exif_map.get(306))
    raw_offset = _normalize_exif_text(exif_map.get(36881) or exif_map.get(36880))
    if raw_timestamp is None:
        return None

    try:
        parsed = datetime.strptime(raw_timestamp, "%Y:%m:%d %H:%M:%S")
    except ValueError:
        return None

    if raw_offset:
        try:
            parsed_offset = datetime.strptime(raw_offset, "%z").tzinfo
        except ValueError:
            parsed_offset = None
        if parsed_offset is not None:
            return parsed.replace(tzinfo=parsed_offset).astimezone(UTC).isoformat()

    # If EXIF omits timezone, keep deterministic behavior by treating it as UTC.
    return parsed.replace(tzinfo=UTC).isoformat()


def _extract_media_metadata(path: Path) -> dict[str, str | int | None]:
    file_suffix = path.suffix.lower()
    if file_suffix not in {".png", ".jpg", ".jpeg"}:
        raise ValueError(f"unsupported media format for extraction: {file_suffix or 'unknown'}")

    try:
        with Image.open(path) as image:
            image.load()
            width, height = image.size
            exif_map = image.getexif()
    except (UnidentifiedImageError, OSError, SyntaxError, ValueError) as exc:
        raise ValueError(f"invalid media content for extraction: {exc}") from exc

    return {
        "capture_timestamp_utc": _extract_capture_timestamp_utc(exif_map),
        "camera_make": _normalize_exif_text(exif_map.get(271)),
        "camera_model": _normalize_exif_text(exif_map.get(272)),
        "image_width": int(width),
        "image_height": int(height),
        "orientation": _normalize_exif_int(exif_map.get(274)),
        "lens_model": _normalize_exif_text(exif_map.get(42036)),
    }


def _upsert_storage_and_catalog_record(
    *,
    store: UploadStateStore,
    relative_path: str,
    sha256_hex: str,
    size_bytes: int,
    source_kind: str,
    seen_at_utc: str,
    provenance_job_name: str | None = None,
    provenance_original_filename: str | None = None,
) -> None:
    store.upsert_stored_file(
        relative_path=relative_path,
        sha256_hex=sha256_hex,
        size_bytes=size_bytes,
        source_kind=source_kind,
        seen_at_utc=seen_at_utc,
    )
    store.upsert_media_asset(
        relative_path=relative_path,
        sha256_hex=sha256_hex,
        size_bytes=size_bytes,
        origin_kind=_catalog_origin_for_source_kind(source_kind),
        observed_at_utc=seen_at_utc,
        provenance_job_name=provenance_job_name,
        provenance_original_filename=provenance_original_filename,
    )


def _attempt_media_extraction(
    *,
    store: UploadStateStore,
    storage_root_path: Path,
    relative_path: str,
) -> None:
    now = datetime.now(UTC).isoformat()
    store.ensure_media_asset_extraction_row(relative_path=relative_path, recorded_at_utc=now)
    asset_path = storage_root_path / relative_path
    try:
        metadata = _extract_media_metadata(asset_path)
    except (OSError, ValueError) as exc:
        store.upsert_media_asset_extraction(
            relative_path=relative_path,
            extraction_status="failed",
            attempted_at_utc=now,
            succeeded_at_utc=None,
            failed_at_utc=now,
            failure_detail=str(exc),
            capture_timestamp_utc=None,
            camera_make=None,
            camera_model=None,
            image_width=None,
            image_height=None,
            orientation=None,
            lens_model=None,
            recorded_at_utc=now,
        )
        return

    store.upsert_media_asset_extraction(
        relative_path=relative_path,
        extraction_status="succeeded",
        attempted_at_utc=now,
        succeeded_at_utc=now,
        failed_at_utc=None,
        failure_detail=None,
        capture_timestamp_utc=(
            str(metadata["capture_timestamp_utc"])
            if metadata["capture_timestamp_utc"] is not None
            else None
        ),
        camera_make=str(metadata["camera_make"]) if metadata["camera_make"] is not None else None,
        camera_model=str(metadata["camera_model"]) if metadata["camera_model"] is not None else None,
        image_width=int(metadata["image_width"]) if metadata["image_width"] is not None else None,
        image_height=int(metadata["image_height"]) if metadata["image_height"] is not None else None,
        orientation=int(metadata["orientation"]) if metadata["orientation"] is not None else None,
        lens_model=str(metadata["lens_model"]) if metadata["lens_model"] is not None else None,
        recorded_at_utc=now,
    )


def _to_admin_catalog_item(record: object) -> AdminCatalogItem:
    return AdminCatalogItem(
        relative_path=str(record.relative_path),
        sha256_hex=str(record.sha256_hex),
        size_bytes=int(record.size_bytes),
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
        capture_timestamp_utc=(
            str(record.capture_timestamp_utc) if record.capture_timestamp_utc is not None else None
        ),
        camera_make=str(record.camera_make) if record.camera_make is not None else None,
        camera_model=str(record.camera_model) if record.camera_model is not None else None,
        image_width=int(record.image_width) if record.image_width is not None else None,
        image_height=int(record.image_height) if record.image_height is not None else None,
        orientation=int(record.orientation) if record.orientation is not None else None,
        lens_model=str(record.lens_model) if record.lens_model is not None else None,
    )


def _to_admin_client_item(record: ClientRecord) -> AdminClientItem:
    return AdminClientItem(
        client_id=record.client_id,
        display_name=record.display_name,
        enrollment_status=ClientEnrollmentStatus(record.enrollment_status),
        first_seen_at_utc=record.first_seen_at_utc,
        last_enrolled_at_utc=record.last_enrolled_at_utc,
        approved_at_utc=record.approved_at_utc,
        revoked_at_utc=record.revoked_at_utc,
        auth_token=record.auth_token,
    )


def _require_approved_client(request: Request, store: UploadStateStore) -> None:
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


def create_app(
    initial_known_sha256: set[str] | None = None,
    *,
    state_store: UploadStateStore | None = None,
    database_url: str | None = None,
    storage_root: str | Path | None = None,
    bootstrap_token: str | None = None,
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
    app.state.bootstrap_token = bootstrap_token or os.getenv("PHOTOVAULT_API_BOOTSTRAP_TOKEN", "")

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/v1/client/enroll/bootstrap", response_model=BootstrapEnrollResponse)
    def bootstrap_enroll(payload: BootstrapEnrollRequest) -> BootstrapEnrollResponse:
        configured_bootstrap_token = str(app.state.bootstrap_token)
        if not configured_bootstrap_token:
            raise HTTPException(status_code=503, detail="bootstrap enrollment is disabled")
        if payload.bootstrap_token != configured_bootstrap_token:
            raise HTTPException(status_code=401, detail="invalid bootstrap token")

        store: UploadStateStore = app.state.upload_state_store
        now = datetime.now(UTC).isoformat()
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

    @app.post("/v1/upload/metadata-handshake", response_model=MetadataHandshakeResponse)
    def metadata_handshake(payload: MetadataHandshakeRequest, request: Request) -> MetadataHandshakeResponse:
        results: list[HandshakeFileResult] = []
        store: UploadStateStore = app.state.upload_state_store
        _require_approved_client(request, store)
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
        _require_approved_client(request, store)
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
    def verify_upload(payload: VerifyRequest, request: Request) -> VerifyResponse:
        store: UploadStateStore = app.state.upload_state_store
        _require_approved_client(request, store)

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
        _upsert_storage_and_catalog_record(
            store=store,
            relative_path=str(target_relative_path.as_posix()),
            sha256_hex=payload.sha256_hex,
            size_bytes=payload.size_bytes,
            source_kind="upload_verify",
            seen_at_utc=now,
            provenance_job_name=upload_row.job_name,
            provenance_original_filename=upload_row.original_filename,
        )
        _attempt_media_extraction(
            store=store,
            storage_root_path=storage_root_path,
            relative_path=str(target_relative_path.as_posix()),
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
                _upsert_storage_and_catalog_record(
                    store=store,
                    relative_path=str(relative_path.as_posix()),
                    sha256_hex=observed_sha,
                    size_bytes=size_bytes,
                    source_kind="index_scan",
                    seen_at_utc=now,
                )
                _attempt_media_extraction(
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

    @app.get("/v1/admin/clients", response_model=AdminClientListResponse)
    def admin_clients(
        limit: int = Query(default=50, ge=1, le=200),
        offset: int = Query(default=0, ge=0),
    ) -> AdminClientListResponse:
        store: UploadStateStore = app.state.upload_state_store
        total, clients = store.list_clients(limit=limit, offset=offset)
        return AdminClientListResponse(
            total=total,
            limit=limit,
            offset=offset,
            items=[_to_admin_client_item(client) for client in clients],
        )

    @app.post("/v1/admin/clients/{client_id}/approve", response_model=AdminClientActionResponse)
    def admin_approve_client(client_id: str) -> AdminClientActionResponse:
        store: UploadStateStore = app.state.upload_state_store
        existing = store.get_client(client_id)
        if existing is None:
            raise HTTPException(status_code=404, detail="client not found")

        issued_token = existing.auth_token or secrets.token_urlsafe(32)
        approved = store.approve_client(
            client_id=client_id,
            approved_at_utc=datetime.now(UTC).isoformat(),
            auth_token=issued_token,
        )
        if approved is None:
            raise HTTPException(status_code=404, detail="client not found")
        return AdminClientActionResponse(item=_to_admin_client_item(approved))

    @app.post("/v1/admin/clients/{client_id}/revoke", response_model=AdminClientActionResponse)
    def admin_revoke_client(client_id: str) -> AdminClientActionResponse:
        store: UploadStateStore = app.state.upload_state_store
        revoked = store.revoke_client(client_id=client_id, revoked_at_utc=datetime.now(UTC).isoformat())
        if revoked is None:
            raise HTTPException(status_code=404, detail="client not found")
        return AdminClientActionResponse(item=_to_admin_client_item(revoked))

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

    @app.get("/v1/admin/catalog", response_model=AdminCatalogListResponse)
    def admin_catalog(
        limit: int = Query(default=50, ge=1, le=200),
        offset: int = Query(default=0, ge=0),
        extraction_status: str | None = Query(default=None),
        origin_kind: str | None = Query(default=None),
        cataloged_since_utc: str | None = Query(default=None),
        cataloged_before_utc: str | None = Query(default=None),
    ) -> AdminCatalogListResponse:
        allowed_extraction_status = {"pending", "succeeded", "failed"}
        if extraction_status is not None and extraction_status not in allowed_extraction_status:
            raise HTTPException(status_code=400, detail="invalid extraction_status filter")
        allowed_origin_kind = {"uploaded", "indexed"}
        if origin_kind is not None and origin_kind not in allowed_origin_kind:
            raise HTTPException(status_code=400, detail="invalid origin_kind filter")

        store: UploadStateStore = app.state.upload_state_store
        total, records = store.list_media_assets(
            limit=limit,
            offset=offset,
            extraction_status=extraction_status,
            origin_kind=origin_kind,
            cataloged_since_utc=cataloged_since_utc,
            cataloged_before_utc=cataloged_before_utc,
        )
        return AdminCatalogListResponse(
            total=total,
            limit=limit,
            offset=offset,
            items=[_to_admin_catalog_item(record) for record in records],
        )

    @app.post("/v1/admin/catalog/extraction/retry", response_model=AdminRetryExtractionResponse)
    def admin_retry_catalog_extraction(payload: AdminRetryExtractionRequest) -> AdminRetryExtractionResponse:
        store: UploadStateStore = app.state.upload_state_store
        existing = store.get_media_asset_by_path(payload.relative_path)
        if existing is None:
            raise HTTPException(status_code=404, detail="catalog asset not found")

        _attempt_media_extraction(
            store=store,
            storage_root_path=storage_root_path,
            relative_path=payload.relative_path,
        )
        updated = store.get_media_asset_by_path(payload.relative_path)
        if updated is None:
            raise HTTPException(status_code=404, detail="catalog asset not found after retry")
        return AdminRetryExtractionResponse(item=_to_admin_catalog_item(updated))

    @app.post("/v1/admin/catalog/extraction/backfill", response_model=AdminBackfillExtractionResponse)
    def admin_backfill_catalog_extraction(
        payload: AdminBackfillExtractionRequest,
    ) -> AdminBackfillExtractionResponse:
        allowed_statuses = {"pending", "failed"}
        requested_statuses = list(dict.fromkeys(payload.target_statuses))
        invalid_statuses = [status for status in requested_statuses if status not in allowed_statuses]
        if invalid_statuses:
            raise HTTPException(
                status_code=400,
                detail=f"invalid target_statuses: {','.join(invalid_statuses)}",
            )

        store: UploadStateStore = app.state.upload_state_store
        candidates = store.list_media_assets_for_extraction(
            extraction_statuses=requested_statuses,
            limit=payload.limit,
        )
        updated_items: list[AdminCatalogItem] = []
        for candidate in candidates:
            _attempt_media_extraction(
                store=store,
                storage_root_path=storage_root_path,
                relative_path=candidate.relative_path,
            )
            updated = store.get_media_asset_by_path(candidate.relative_path)
            if updated is not None:
                updated_items.append(_to_admin_catalog_item(updated))

        succeeded_count = sum(1 for item in updated_items if item.extraction_status == "succeeded")
        failed_count = sum(1 for item in updated_items if item.extraction_status == "failed")
        return AdminBackfillExtractionResponse(
            requested_statuses=requested_statuses,
            selected_count=len(candidates),
            processed_count=len(updated_items),
            succeeded_count=succeeded_count,
            failed_count=failed_count,
            items=updated_items,
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
