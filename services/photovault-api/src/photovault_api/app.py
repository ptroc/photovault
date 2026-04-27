"""Server-side API skeleton for photovault."""
import hashlib
import io
import logging
import mimetypes
import os
import secrets
import shutil
import subprocess
import tempfile
import time
import traceback
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from PIL import Image, UnidentifiedImageError
from pydantic import BaseModel, Field

from photovault_api.state_store import (
    CatalogBackfillRunRecord,
    ClientHeartbeatRecord,
    ClientRecord,
    InMemoryUploadStateStore,
    PostgresUploadStateStore,
    StorageIndexRunRecord,
    StorageSummary,
    UploadStateStore,
)
from .models import (
    AdminBackfillCatalogRequest,
    AdminBackfillCatalogResponse,
    AdminCatalogAssetResponse,
    AdminCatalogFolderItem,
    AdminCatalogFoldersResponse,
    AdminCatalogItem,
    AdminCatalogListResponse,
    AdminCatalogOrganizationRequest,
    AdminCatalogOrganizationResponse,
    AdminCatalogRejectQueueItem,
    AdminCatalogRejectRequest,
    AdminCatalogRejectQueueResponse,
    AdminCatalogRejectResponse,
    AdminCatalogRejectUnmarkResponse,
    AdminClientActionResponse,
    AdminClientListResponse,
    AdminFileItem,
    AdminFileListResponse,
    AdminLatestCatalogBackfillRunsResponse,
    AdminOverviewResponse,
    AdminRetryExtractionRequest,
    AdminRetryExtractionResponse,
    AdminRetryPreviewRequest,
    AdminRetryPreviewResponse,
    BootstrapEnrollRequest,
    BootstrapEnrollResponse,
    ClientEnrollmentStatus,
    ClientHeartbeatRequest,
    ClientHeartbeatResponse,
    ClientPresenceStatus,
    ClientWorkloadStatus,
    DuplicateShaGroupItem,
    DuplicateShaGroupListResponse,
    HandshakeDecision,
    HandshakeFileResult,
    HandshakeFileRequest,
    IndexStorageResponse,
    LatestIndexRunEnvelope,
    LatestIndexRunResponse,
    MetadataHandshakeRequest,
    MetadataHandshakeResponse,
    PathConflictItem,
    PathConflictListResponse,
    UploadContentResponse,
    VerifyRequest,
    VerifyResponse,
)
from .media import (
    _compute_sha256,
    _iter_storage_files,
    _catalog_origin_for_source_kind,
    _media_type_for_relative_path,
    _preview_capability_for_relative_path,
    _preview_max_size,
    _resolve_preview_max_long_edge,
    _resolve_preview_suffix_set,
    _sanitize_component,
    _normalize_exif_iso_speed,
    _normalize_exif_rational,
    _extract_media_metadata,
    _ALLOWED_EXTRACTION_STATUS,
    _ALLOWED_MEDIA_TYPE,
    _ALLOWED_ORIGIN_KIND,
    _ALLOWED_PREVIEW_CAPABILITY,
    _ALLOWED_PREVIEW_STATUS,
    _DEFAULT_PREVIEW_MAX_LONG_EDGE,
    _HEARTBEAT_ONLINE_MAX_AGE_SECONDS,
    _CLIENT_LIST_SCAN_MAX,
    _CLIENT_LIST_SCAN_PAGE_SIZE,
    _MEDIA_TYPE_SUFFIXES,
    _PREVIEWABLE_SUFFIXES,
    _PREVIEW_HEIC_SUFFIXES,
    _PREVIEW_RASTER_SUFFIXES,
    _PREVIEW_RAW_SUFFIXES,
)
from .storage_ops import (
    _heartbeat_presence_status,
    _list_clients_for_admin_view,
    _normalize_catalog_folder_prefix,
    _parse_boolean_filter,
    _presence_sort_rank,
    _require_approved_client,
    _to_admin_catalog_item,
    _to_admin_client_item,
    _to_backfill_run_summary,
    _validate_backfill_target_statuses,
    _validate_catalog_filter_selection,
    _workload_sort_rank,
)


APP_LOGGER = logging.getLogger("photovault-api.app")

# I/O helpers kept here so tests can monkeypatch via app_module.*
def _find_executable(name: str) -> str | None:
    return shutil.which(name)


def _run_external_command(command: list[str]) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(command, check=False, capture_output=True)


def _open_rgb_image(path: Path) -> Image.Image:
    with Image.open(path) as image:
        image.load()
        return image.convert("RGB")


def _open_rgb_image_from_bytes(payload: bytes) -> Image.Image:
    with Image.open(io.BytesIO(payload)) as image:
        image.load()
        return image.convert("RGB")


def _decode_process_stderr(stderr: bytes) -> str:
    decoded = stderr.decode("utf-8", errors="ignore").strip()
    return decoded or "no error detail"


def _render_heic_preview_source(path: Path) -> Image.Image:
    try:
        return _open_rgb_image(path)
    except (UnidentifiedImageError, OSError, SyntaxError, ValueError):
        pass

    converter_path = _find_executable("heif-convert")
    if converter_path is None:
        raise ValueError(
            "HEIC preview backend unavailable: install heif-convert or enable HEIF support in Pillow"
        )

    with tempfile.TemporaryDirectory(prefix="photovault-heic-preview-") as temp_dir:
        converted_path = Path(temp_dir) / "preview.jpg"
        result = _run_external_command([converter_path, str(path), str(converted_path)])
        if result.returncode != 0:
            raise ValueError(
                "HEIC preview backend failed: "
                f"{_decode_process_stderr(result.stderr)}"
            )
        if not converted_path.is_file():
            raise ValueError("HEIC preview backend failed: converter did not produce preview output")
        try:
            return _open_rgb_image(converted_path)
        except (UnidentifiedImageError, OSError, SyntaxError, ValueError) as exc:
            raise ValueError(f"HEIC preview backend produced invalid preview data: {exc}") from exc


def _extract_raw_embedded_preview_bytes(path: Path) -> bytes:
    extractor_path = _find_executable("exiftool")
    if extractor_path is None:
        raise ValueError("RAW embedded preview unavailable: exiftool is not installed")

    observed_errors: list[str] = []
    for tag_name in _RAW_EMBEDDED_PREVIEW_TAGS:
        result = _run_external_command([extractor_path, "-b", f"-{tag_name}", str(path)])
        if result.returncode == 0 and result.stdout:
            return result.stdout
        stderr_text = _decode_process_stderr(result.stderr)
        if result.returncode != 0 and stderr_text != "no error detail":
            observed_errors.append(f"{tag_name}: {stderr_text}")

    if observed_errors:
        raise ValueError("RAW embedded preview unavailable: " + "; ".join(observed_errors))
    raise ValueError("RAW embedded preview unavailable: no embedded preview data found")


def _render_raw_preview_source_via_libraw(path: Path) -> Image.Image:
    try:
        import rawpy  # type: ignore[import-untyped]
    except ImportError as exc:
        raise ValueError(f"libraw fallback unavailable: rawpy is not installed: {exc}") from exc

    try:
        with rawpy.imread(str(path)) as raw:
            rgb = raw.postprocess(use_camera_wb=True, output_bps=8)
    except (rawpy.LibRawError, OSError, ValueError) as exc:
        raise ValueError(f"libraw fallback failed: {exc}") from exc

    try:
        return Image.fromarray(rgb, mode="RGB")
    except (TypeError, ValueError) as exc:
        raise ValueError(f"libraw fallback produced invalid preview data: {exc}") from exc


def _render_raw_preview_source(path: Path) -> Image.Image:
    embedded_preview_error_detail: str | None = None
    try:
        preview_bytes = _extract_raw_embedded_preview_bytes(path)
        try:
            return _open_rgb_image_from_bytes(preview_bytes)
        except (UnidentifiedImageError, OSError, SyntaxError, ValueError) as exc:
            embedded_preview_error_detail = f"RAW embedded preview data is invalid: {exc}"
    except (OSError, ValueError) as exc:
        embedded_preview_error_detail = f"RAW embedded preview extraction failed: {exc}"

    try:
        return _render_raw_preview_source_via_libraw(path)
    except ValueError as exc:
        if embedded_preview_error_detail is None:
            raise ValueError(f"RAW libraw fallback failed: {exc}") from exc
        raise ValueError(f"{embedded_preview_error_detail}; RAW libraw fallback failed: {exc}") from exc


def _render_preview_source(path: Path) -> Image.Image:
    file_suffix = path.suffix.lower()
    if file_suffix in _PREVIEW_RASTER_SUFFIXES:
        try:
            return _open_rgb_image(path)
        except (UnidentifiedImageError, OSError, SyntaxError, ValueError) as exc:
            raise ValueError(f"invalid media content for preview: {exc}") from exc
    if file_suffix in _PREVIEW_HEIC_SUFFIXES:
        return _render_heic_preview_source(path)
    if file_suffix in _PREVIEW_RAW_SUFFIXES:
        return _render_raw_preview_source(path)
    raise ValueError(f"unsupported media format for preview: {file_suffix or 'unknown'}")


def _media_type_for_relative_path(relative_path: str) -> str:
    lowered = relative_path.lower()
    for media_type, suffixes in _MEDIA_TYPE_SUFFIXES.items():
        if lowered.endswith(suffixes):
            return media_type
    return "other"


def _preview_capability_for_relative_path(relative_path: str) -> str:
    lowered = relative_path.lower()
    if lowered.endswith(tuple(_PREVIEWABLE_SUFFIXES)):
        return "previewable"
    return "not_previewable"


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
            exposure_time_s=None,
            f_number=None,
            iso_speed=None,
            focal_length_mm=None,
            focal_length_35mm_mm=None,
            recorded_at_utc=now,
        )
        return

    exposure_time_raw = metadata["exposure_time_s"]
    f_number_raw = metadata["f_number"]
    iso_raw = metadata["iso_speed"]
    focal_length_raw = metadata["focal_length_mm"]
    focal_length_35mm_raw = metadata["focal_length_35mm_mm"]

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
        exposure_time_s=(
            float(exposure_time_raw) if isinstance(exposure_time_raw, (int, float)) else None
        ),
        f_number=float(f_number_raw) if isinstance(f_number_raw, (int, float)) else None,
        iso_speed=int(iso_raw) if isinstance(iso_raw, int) and not isinstance(iso_raw, bool) else None,
        focal_length_mm=(
            float(focal_length_raw) if isinstance(focal_length_raw, (int, float)) else None
        ),
        focal_length_35mm_mm=(
            int(focal_length_35mm_raw)
            if isinstance(focal_length_35mm_raw, int) and not isinstance(focal_length_35mm_raw, bool)
            else None
        ),
        recorded_at_utc=now,
    )


def _preview_relative_cache_path(
    *,
    relative_path: str,
    sha256_hex: str,
    preview_max_long_edge: int,
) -> str:
    source_path = Path(relative_path)
    stem = source_path.stem or "asset"
    parent = source_path.parent.as_posix()
    filename = f"{stem}__{sha256_hex[:12]}__w{preview_max_long_edge}.jpg"
    if parent and parent != ".":
        return f"{parent}/{filename}"
    return filename


def _attempt_preview_generation(
    *,
    store: UploadStateStore,
    storage_root_path: Path,
    preview_cache_root_path: Path,
    preview_max_long_edge: int,
    preview_passthrough_suffixes: frozenset[str],
    preview_placeholder_suffixes: frozenset[str],
    relative_path: str,
) -> None:
    now = datetime.now(UTC).isoformat()
    store.ensure_media_asset_preview_row(relative_path=relative_path, recorded_at_utc=now)
    asset = store.get_media_asset_by_path(relative_path)
    if asset is None:
        return
    asset_path = storage_root_path / relative_path
    file_suffix = Path(relative_path).suffix.lower()

    if file_suffix in preview_passthrough_suffixes:
        store.upsert_media_asset_preview(
            relative_path=relative_path,
            preview_status="succeeded",
            preview_relative_path=None,
            attempted_at_utc=now,
            succeeded_at_utc=now,
            failed_at_utc=None,
            failure_detail=None,
            recorded_at_utc=now,
        )
        return

    if file_suffix in preview_placeholder_suffixes:
        store.upsert_media_asset_preview(
            relative_path=relative_path,
            preview_status="failed",
            preview_relative_path=None,
            attempted_at_utc=now,
            succeeded_at_utc=None,
            failed_at_utc=now,
            failure_detail=f"preview generation skipped by configuration for suffix: {file_suffix}",
            recorded_at_utc=now,
        )
        return

    preview_relative_path = _preview_relative_cache_path(
        relative_path=relative_path,
        sha256_hex=asset.sha256_hex,
        preview_max_long_edge=preview_max_long_edge,
    )
    preview_path = preview_cache_root_path / preview_relative_path
    preview_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        if not preview_path.exists():
            with _render_preview_source(asset_path) as preview_image:
                preview_image.thumbnail(
                    _preview_max_size(preview_max_long_edge),
                    Image.Resampling.LANCZOS,
                )
                preview_image.save(preview_path, format="JPEG", quality=85, optimize=True)
    except ValueError as exc:
        store.upsert_media_asset_preview(
            relative_path=relative_path,
            preview_status="failed",
            preview_relative_path=None,
            attempted_at_utc=now,
            succeeded_at_utc=None,
            failed_at_utc=now,
            failure_detail=f"preview generation failed: {exc}",
            recorded_at_utc=now,
        )
        return

    store.upsert_media_asset_preview(
        relative_path=relative_path,
        preview_status="succeeded",
        preview_relative_path=preview_relative_path,
        attempted_at_utc=now,
        succeeded_at_utc=now,
        failed_at_utc=None,
        failure_detail=None,
        recorded_at_utc=now,
    )



def create_app(
    initial_known_sha256: set[str] | None = None,
    *,
    state_store: UploadStateStore | None = None,
    database_url: str | None = None,
    storage_root: str | Path | None = None,
    preview_max_long_edge: int | None = None,
    preview_passthrough_suffixes: list[str] | set[str] | tuple[str, ...] | None = None,
    preview_placeholder_suffixes: list[str] | set[str] | tuple[str, ...] | None = None,
    bootstrap_token: str | None = None,
) -> FastAPI:
    resolved_storage_root = storage_root or os.getenv("PHOTOVAULT_API_STORAGE_ROOT")
    if not resolved_storage_root:
        raise RuntimeError("PHOTOVAULT_API_STORAGE_ROOT must be set")
    storage_root_path = Path(resolved_storage_root).expanduser().resolve()
    resolved_preview_cache_root = (
        os.getenv("PHOTOVAULT_API_PREVIEW_CACHE_ROOT")
        or str(storage_root_path.parent / ".photovault_preview_cache")
    )
    preview_cache_root_path = Path(resolved_preview_cache_root).expanduser().resolve()
    resolved_preview_max_long_edge = _resolve_preview_max_long_edge(
        preview_max_long_edge
        if preview_max_long_edge is not None
        else os.getenv("PHOTOVAULT_API_PREVIEW_MAX_LONG_EDGE")
    )
    resolved_preview_passthrough_suffixes = _resolve_preview_suffix_set(
        preview_passthrough_suffixes
        if preview_passthrough_suffixes is not None
        else os.getenv("PHOTOVAULT_API_PREVIEW_PASSTHROUGH_SUFFIXES"),
        env_name="PHOTOVAULT_API_PREVIEW_PASSTHROUGH_SUFFIXES",
    )
    resolved_preview_placeholder_suffixes = _resolve_preview_suffix_set(
        preview_placeholder_suffixes
        if preview_placeholder_suffixes is not None
        else os.getenv("PHOTOVAULT_API_PREVIEW_PLACEHOLDER_SUFFIXES"),
        env_name="PHOTOVAULT_API_PREVIEW_PLACEHOLDER_SUFFIXES",
    )
    temp_root = storage_root_path / ".temp_uploads"
    temp_root.mkdir(parents=True, exist_ok=True)
    preview_cache_root_path.mkdir(parents=True, exist_ok=True)

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
    app.state.preview_cache_root = preview_cache_root_path
    app.state.preview_max_long_edge = resolved_preview_max_long_edge
    app.state.preview_passthrough_suffixes = resolved_preview_passthrough_suffixes
    app.state.preview_placeholder_suffixes = resolved_preview_placeholder_suffixes
    app.state.bootstrap_token = bootstrap_token or os.getenv("PHOTOVAULT_API_BOOTSTRAP_TOKEN", "")

    def _extract_error_message(detail: object) -> str:
        if isinstance(detail, str):
            return detail
        if isinstance(detail, dict):
            message = detail.get("message")
            if isinstance(message, str) and message.strip():
                return message
        return str(detail)

    def _request_id_from_request(request: Request) -> str:
        request_id = getattr(request.state, "request_id", None)
        if isinstance(request_id, str) and request_id.strip():
            return request_id
        return secrets.token_hex(8)

    @app.middleware("http")
    async def log_http_requests(request: Request, call_next):
        request_id = secrets.token_hex(8)
        request.state.request_id = request_id
        started_at_utc = datetime.now(UTC).isoformat()
        started_monotonic = time.perf_counter()
        method = request.method
        path = request.url.path
        try:
            response = await call_next(request)
        except Exception as exc:
            duration_ms = (time.perf_counter() - started_monotonic) * 1000.0
            APP_LOGGER.exception(
                "request timestamp=%s method=%s path=%s status_code=%s duration_ms=%.2f request_id=%s",
                started_at_utc,
                method,
                path,
                500,
                duration_ms,
                request_id,
                exc_info=exc,
            )
            raise
        duration_ms = (time.perf_counter() - started_monotonic) * 1000.0
        APP_LOGGER.info(
            "request timestamp=%s method=%s path=%s status_code=%s duration_ms=%.2f request_id=%s",
            started_at_utc,
            method,
            path,
            response.status_code,
            duration_ms,
            request_id,
        )
        response.headers["x-request-id"] = request_id
        return response

    @app.exception_handler(HTTPException)
    async def handle_http_exception(request: Request, exc: HTTPException):
        request_id = _request_id_from_request(request)
        headers = dict(exc.headers) if exc.headers is not None else {}
        headers["x-request-id"] = request_id
        if exc.status_code < 500:
            return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail}, headers=headers)

        timestamp_utc = datetime.now(UTC).isoformat()
        traceback_lines = traceback.format_exception(type(exc), exc, exc.__traceback__)
        enriched_detail: dict[str, object] = {
            "request_id": request_id,
            "timestamp_utc": timestamp_utc,
            "message": _extract_error_message(exc.detail),
            "traceback": traceback_lines,
        }
        if isinstance(exc.detail, dict):
            enriched_detail = {**exc.detail, **enriched_detail}
        else:
            enriched_detail["error_detail"] = exc.detail

        APP_LOGGER.error(
            "http_5xx timestamp=%s method=%s path=%s status_code=%s request_id=%s message=%s",
            timestamp_utc,
            request.method,
            request.url.path,
            exc.status_code,
            request_id,
            enriched_detail["message"],
            exc_info=exc,
        )
        return JSONResponse(status_code=exc.status_code, content={"detail": enriched_detail}, headers=headers)

    @app.exception_handler(Exception)
    async def handle_unexpected_exception(request: Request, exc: Exception):
        request_id = _request_id_from_request(request)
        timestamp_utc = datetime.now(UTC).isoformat()
        traceback_lines = traceback.format_exception(type(exc), exc, exc.__traceback__)
        detail = {
            "request_id": request_id,
            "timestamp_utc": timestamp_utc,
            "message": str(exc) or exc.__class__.__name__,
            "traceback": traceback_lines,
            "exception_type": exc.__class__.__name__,
        }
        APP_LOGGER.exception(
            "unhandled_exception timestamp=%s method=%s path=%s status_code=%s request_id=%s message=%s",
            timestamp_utc,
            request.method,
            request.url.path,
            500,
            request_id,
            detail["message"],
            exc_info=exc,
        )
        return JSONResponse(status_code=500, content={"detail": detail}, headers={"x-request-id": request_id})

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

    @app.post("/v1/client/heartbeat", response_model=ClientHeartbeatResponse)
    def client_heartbeat(payload: ClientHeartbeatRequest, request: Request) -> ClientHeartbeatResponse:
        store: UploadStateStore = app.state.upload_state_store
        client = _require_approved_client(request, store)
        updated = store.upsert_client_heartbeat(
            client_id=client.client_id,
            last_seen_at_utc=payload.last_seen_at_utc.astimezone(UTC).isoformat(),
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
                payload.retry_backoff.next_retry_at_utc.astimezone(UTC).isoformat()
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
                payload.recent_error.created_at_utc.astimezone(UTC).isoformat()
                if payload.recent_error is not None
                else None
            ),
            updated_at_utc=datetime.now(UTC).isoformat(),
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

        # Phase 3.C: check for tombstoned SHA before checking known_sha256.
        # This prevents re-upload of deleted content.
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
        presence_status: str | None = Query(default=None),
        workload_status: str | None = Query(default=None),
        enrollment_status: str | None = Query(default=None),
        sort_by: str = Query(default="last_seen"),
        sort_order: str = Query(default="desc"),
    ) -> AdminClientListResponse:
        allowed_presence = {status.value for status in ClientPresenceStatus}
        if presence_status is not None and presence_status not in allowed_presence:
            raise HTTPException(status_code=400, detail="invalid presence_status filter")
        allowed_workload = {status.value for status in ClientWorkloadStatus}
        if workload_status is not None and workload_status not in allowed_workload:
            raise HTTPException(status_code=400, detail="invalid workload_status filter")
        allowed_enrollment = {status.value for status in ClientEnrollmentStatus}
        if enrollment_status is not None and enrollment_status not in allowed_enrollment:
            raise HTTPException(status_code=400, detail="invalid enrollment_status filter")
        if sort_by not in {"last_seen", "presence_status", "workload_status", "client_id"}:
            raise HTTPException(status_code=400, detail="invalid sort_by value")
        if sort_order not in {"asc", "desc"}:
            raise HTTPException(status_code=400, detail="invalid sort_order value")

        store: UploadStateStore = app.state.upload_state_store
        now_utc = datetime.now(UTC)
        client_items = [
            _to_admin_client_item(
                client,
                heartbeat=store.get_client_heartbeat(client.client_id),
                now_utc=now_utc,
            )
            for client in _list_clients_for_admin_view(store)
        ]

        if presence_status is not None:
            client_items = [
                item for item in client_items if item.heartbeat_presence_status == presence_status
            ]
        if workload_status is not None:
            client_items = [
                item
                for item in client_items
                if (
                    item.heartbeat_workload_status is not None
                    and item.heartbeat_workload_status.value == workload_status
                )
            ]
        if enrollment_status is not None:
            client_items = [
                item for item in client_items if item.enrollment_status.value == enrollment_status
            ]

        if sort_by == "presence_status":
            client_items.sort(
                key=lambda item: (
                    _presence_sort_rank(item.heartbeat_presence_status),
                    item.heartbeat_last_seen_at_utc or "",
                    item.client_id,
                ),
                reverse=(sort_order == "desc"),
            )
        elif sort_by == "workload_status":
            client_items.sort(
                key=lambda item: (
                    _workload_sort_rank(
                        item.heartbeat_workload_status.value
                        if item.heartbeat_workload_status is not None
                        else None
                    ),
                    item.heartbeat_last_seen_at_utc or "",
                    item.client_id,
                ),
                reverse=(sort_order == "desc"),
            )
        elif sort_by == "client_id":
            client_items.sort(key=lambda item: item.client_id, reverse=(sort_order == "desc"))
        else:
            client_items.sort(
                key=lambda item: (
                    1 if item.heartbeat_last_seen_at_utc is not None else 0,
                    item.heartbeat_last_seen_at_utc or "",
                    item.client_id,
                ),
                reverse=(sort_order == "desc"),
            )

        total = len(client_items)
        paged_items = client_items[offset : offset + limit]
        return AdminClientListResponse(
            total=total,
            limit=limit,
            offset=offset,
            items=paged_items,
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
        return AdminClientActionResponse(
            item=_to_admin_client_item(
                approved,
                heartbeat=store.get_client_heartbeat(approved.client_id),
                now_utc=datetime.now(UTC),
            )
        )

    @app.post("/v1/admin/clients/{client_id}/revoke", response_model=AdminClientActionResponse)
    def admin_revoke_client(client_id: str) -> AdminClientActionResponse:
        store: UploadStateStore = app.state.upload_state_store
        revoked = store.revoke_client(client_id=client_id, revoked_at_utc=datetime.now(UTC).isoformat())
        if revoked is None:
            raise HTTPException(status_code=404, detail="client not found")
        return AdminClientActionResponse(
            item=_to_admin_client_item(
                revoked,
                heartbeat=store.get_client_heartbeat(revoked.client_id),
                now_utc=datetime.now(UTC),
            )
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

    @app.get("/v1/admin/catalog", response_model=AdminCatalogListResponse)
    def admin_catalog(
        limit: int = Query(default=50, ge=1, le=200),
        offset: int = Query(default=0, ge=0),
        extraction_status: str | None = Query(default=None),
        preview_status: str | None = Query(default=None),
        origin_kind: str | None = Query(default=None),
        media_type: str | None = Query(default=None),
        preview_capability: str | None = Query(default=None),
        is_favorite: str | None = Query(default=None),
        is_archived: str | None = Query(default=None),
        cataloged_since_utc: str | None = Query(default=None),
        cataloged_before_utc: str | None = Query(default=None),
        relative_path_prefix: str | None = Query(default=None),
    ) -> AdminCatalogListResponse:
        _validate_catalog_filter_selection(
            extraction_status=extraction_status,
            preview_status=preview_status,
            origin_kind=origin_kind,
            media_type=media_type,
            preview_capability=preview_capability,
        )
        is_favorite_filter = _parse_boolean_filter(is_favorite, field_name="is_favorite")
        is_archived_filter = _parse_boolean_filter(is_archived, field_name="is_archived")
        normalized_prefix = _normalize_catalog_folder_prefix(relative_path_prefix)

        store: UploadStateStore = app.state.upload_state_store
        total, records = store.list_media_assets(
            limit=limit,
            offset=offset,
            extraction_status=extraction_status,
            preview_status=preview_status,
            origin_kind=origin_kind,
            media_type=media_type,
            preview_capability=preview_capability,
            is_favorite=is_favorite_filter,
            is_archived=is_archived_filter,
            cataloged_since_utc=cataloged_since_utc,
            cataloged_before_utc=cataloged_before_utc,
            relative_path_prefix=normalized_prefix,
        )
        # Pull the reject queue in one call and intersect per-item so each
        # row's ``is_rejected`` flag is populated without a per-row query.
        # The queue is expected to be small (single-operator v1); the hard
        # cap of 10_000 keeps this safe if the operator lets it grow.
        _, reject_rows = store.list_catalog_rejects(limit=10_000, offset=0)
        rejected_paths = frozenset(r.relative_path for r in reject_rows)
        return AdminCatalogListResponse(
            total=total,
            limit=limit,
            offset=offset,
            items=[
                _to_admin_catalog_item(
                    record,
                    is_rejected=(record.relative_path in rejected_paths),
                )
                for record in records
            ],
        )

    @app.get(
        "/v1/admin/catalog/folders",
        response_model=AdminCatalogFoldersResponse,
    )
    def admin_catalog_folders() -> AdminCatalogFoldersResponse:
        store: UploadStateStore = app.state.upload_state_store
        rows = store.list_media_asset_folders()
        return AdminCatalogFoldersResponse(
            folders=[
                AdminCatalogFolderItem(
                    path=path,
                    depth=depth,
                    direct_count=direct_count,
                    total_count=total_count,
                )
                for (path, depth, direct_count, total_count) in rows
            ]
        )

    @app.get("/v1/admin/catalog/asset", response_model=AdminCatalogAssetResponse)
    def admin_catalog_asset(
        relative_path: str = Query(min_length=1),
    ) -> AdminCatalogAssetResponse:
        store: UploadStateStore = app.state.upload_state_store
        record = store.get_media_asset_by_path(relative_path)
        if record is None:
            raise HTTPException(status_code=404, detail="catalog asset not found")
        is_rejected = store.is_catalog_reject(relative_path)
        return AdminCatalogAssetResponse(
            item=_to_admin_catalog_item(record, is_rejected=is_rejected)
        )

    @app.post(
        "/v1/admin/catalog/favorite/mark",
        response_model=AdminCatalogOrganizationResponse,
    )
    def admin_mark_catalog_favorite(
        payload: AdminCatalogOrganizationRequest,
    ) -> AdminCatalogOrganizationResponse:
        store: UploadStateStore = app.state.upload_state_store
        updated = store.set_media_asset_favorite(
            relative_path=payload.relative_path,
            is_favorite=True,
            updated_at_utc=datetime.now(UTC).isoformat(),
        )
        if updated is None:
            raise HTTPException(status_code=404, detail="catalog asset not found")
        return AdminCatalogOrganizationResponse(item=_to_admin_catalog_item(updated))

    @app.post(
        "/v1/admin/catalog/favorite/unmark",
        response_model=AdminCatalogOrganizationResponse,
    )
    def admin_unmark_catalog_favorite(
        payload: AdminCatalogOrganizationRequest,
    ) -> AdminCatalogOrganizationResponse:
        store: UploadStateStore = app.state.upload_state_store
        updated = store.set_media_asset_favorite(
            relative_path=payload.relative_path,
            is_favorite=False,
            updated_at_utc=datetime.now(UTC).isoformat(),
        )
        if updated is None:
            raise HTTPException(status_code=404, detail="catalog asset not found")
        return AdminCatalogOrganizationResponse(item=_to_admin_catalog_item(updated))

    @app.post(
        "/v1/admin/catalog/archive/mark",
        response_model=AdminCatalogOrganizationResponse,
    )
    def admin_mark_catalog_archived(
        payload: AdminCatalogOrganizationRequest,
    ) -> AdminCatalogOrganizationResponse:
        store: UploadStateStore = app.state.upload_state_store
        updated = store.set_media_asset_archived(
            relative_path=payload.relative_path,
            is_archived=True,
            updated_at_utc=datetime.now(UTC).isoformat(),
        )
        if updated is None:
            raise HTTPException(status_code=404, detail="catalog asset not found")
        return AdminCatalogOrganizationResponse(item=_to_admin_catalog_item(updated))

    @app.post(
        "/v1/admin/catalog/archive/unmark",
        response_model=AdminCatalogOrganizationResponse,
    )
    def admin_unmark_catalog_archived(
        payload: AdminCatalogOrganizationRequest,
    ) -> AdminCatalogOrganizationResponse:
        store: UploadStateStore = app.state.upload_state_store
        updated = store.set_media_asset_archived(
            relative_path=payload.relative_path,
            is_archived=False,
            updated_at_utc=datetime.now(UTC).isoformat(),
        )
        if updated is None:
            raise HTTPException(status_code=404, detail="catalog asset not found")
        return AdminCatalogOrganizationResponse(item=_to_admin_catalog_item(updated))

    # ---------- Phase 3.B: reject queue -------------------------------------

    def _require_safe_relative_path(relative_path: str) -> str:
        """Validate that ``relative_path`` is a catalog-safe forward-slash path.

        Rejects leading slash, backslash, empty segments, and ``.``/``..`` segments.
        Returns the trimmed path. Raises HTTP 400 otherwise.
        """

        value = (relative_path or "").strip()
        if value == "" or value.startswith("/") or "\\" in value:
            raise HTTPException(status_code=400, detail="invalid relative_path")
        trimmed = value.strip("/")
        if trimmed == "":
            raise HTTPException(status_code=400, detail="invalid relative_path")
        for segment in trimmed.split("/"):
            if segment in ("", ".", ".."):
                raise HTTPException(status_code=400, detail="invalid relative_path")
        return trimmed

    @app.post(
        "/v1/admin/catalog/reject",
        response_model=AdminCatalogRejectResponse,
    )
    def admin_mark_catalog_reject(
        payload: AdminCatalogRejectRequest,
    ) -> AdminCatalogRejectResponse:
        store: UploadStateStore = app.state.upload_state_store
        safe_path = _require_safe_relative_path(payload.relative_path)
        record = store.add_catalog_reject(
            relative_path=safe_path,
            marked_at_utc=datetime.now(UTC).isoformat(),
            marked_reason=payload.marked_reason,
        )
        if record is None:
            raise HTTPException(status_code=404, detail="catalog asset not found")
        return AdminCatalogRejectResponse(
            relative_path=record.relative_path,
            sha256_hex=record.sha256_hex,
            marked_at_utc=record.marked_at_utc,
            marked_reason=record.marked_reason,
            is_rejected=True,
        )

    @app.post(
        "/v1/admin/catalog/reject/unmark",
        response_model=AdminCatalogRejectUnmarkResponse,
    )
    def admin_unmark_catalog_reject(
        payload: AdminCatalogOrganizationRequest,
    ) -> AdminCatalogRejectUnmarkResponse:
        """Idempotent unmark. Returns ``is_rejected=False`` whether or not the
        row was present. We intentionally do not 404 on a missing queue row —
        a double-unmark from two concurrent reviewers should be a no-op, not
        an error.
        """

        store: UploadStateStore = app.state.upload_state_store
        safe_path = _require_safe_relative_path(payload.relative_path)
        store.remove_catalog_reject(safe_path)
        return AdminCatalogRejectUnmarkResponse(
            relative_path=safe_path, is_rejected=False
        )

    @app.get(
        "/v1/admin/catalog/rejects",
        response_model=AdminCatalogRejectQueueResponse,
    )
    def admin_list_catalog_rejects(
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ) -> AdminCatalogRejectQueueResponse:
        store: UploadStateStore = app.state.upload_state_store
        total, rejects = store.list_catalog_rejects(limit=limit, offset=offset)
        items: list[AdminCatalogRejectQueueItem] = []
        for rejected in rejects:
            asset = store.get_media_asset_by_path(rejected.relative_path)
            catalog_item = (
                _to_admin_catalog_item(asset, is_rejected=True) if asset is not None else None
            )
            items.append(
                AdminCatalogRejectQueueItem(
                    relative_path=rejected.relative_path,
                    sha256_hex=rejected.sha256_hex,
                    marked_at_utc=rejected.marked_at_utc,
                    marked_reason=rejected.marked_reason,
                    item=catalog_item,
                )
            )
        return AdminCatalogRejectQueueResponse(
            total=total, limit=limit, offset=offset, items=items
        )

    # ---------- Phase 3.C: execute delete + tombstones ----------------------

    _TRASH_RETENTION_DAYS = 14

    class ExecuteRejectsRequest(BaseModel):
        relative_paths: list[str] | None = Field(default=None, max_length=10000)

    class ExecuteRejectsResponse(BaseModel):
        executed: list[str]
        skipped: list[str]

    class ClientTombstoneReportItem(BaseModel):
        sha256_hex: str
        relative_path: str
        trashed_at_utc: str

    class ClientTombstoneReportRequest(BaseModel):
        sha256_hex: list[str] = Field(min_length=0, max_length=500)

    class ClientTombstoneReportResponse(BaseModel):
        tombstoned: list[ClientTombstoneReportItem]

    @app.post("/v1/admin/catalog/rejects/execute", response_model=ExecuteRejectsResponse)
    def admin_execute_rejects(payload: ExecuteRejectsRequest) -> ExecuteRejectsResponse:
        store: UploadStateStore = app.state.upload_state_store
        executed: list[str] = []
        skipped: list[str] = []
        started_at_utc = datetime.now(UTC).isoformat()

        # Determine which paths to execute: payload list or entire queue.
        if payload.relative_paths:
            target_paths = payload.relative_paths
        else:
            _, all_rejected = store.list_catalog_rejects(limit=100000, offset=0)
            target_paths = [row.relative_path for row in all_rejected]
        APP_LOGGER.info(
            "admin_reject_execute_started timestamp=%s requested_paths=%s targets=%s",
            started_at_utc,
            len(payload.relative_paths or []),
            len(target_paths),
        )

        now = datetime.now(UTC).isoformat()
        for relative_path in target_paths:
            safe_path = _require_safe_relative_path(relative_path)
            # Find the rejected row for this path.
            rejects_total, rejects_batch = store.list_catalog_rejects(limit=100000, offset=0)
            matching_reject = next(
                (r for r in rejects_batch if r.relative_path == safe_path), None
            )
            if matching_reject is None:
                skipped.append(safe_path)
                continue

            # Move file to trash.
            source_path = storage_root_path / safe_path
            trashed_at = datetime.fromisoformat(now)
            trash_year = f"{trashed_at.year:04d}"
            trash_month = f"{trashed_at.month:02d}"
            trash_day = f"{trashed_at.day:02d}"
            sha_prefix = matching_reject.sha256_hex[:12]
            trash_relative_path = f".trash/{trash_year}/{trash_month}/{trash_day}/{sha_prefix}/{safe_path}"
            trash_path = storage_root_path / trash_relative_path

            try:
                if source_path.exists():
                    trash_path.parent.mkdir(parents=True, exist_ok=True)
                    try:
                        os.replace(source_path, trash_path)
                    except OSError:
                        # Fallback for cross-filesystem: copy + unlink + fsync.
                        import shutil as shutil_module
                        shutil_module.copy2(source_path, trash_path)
                        source_path.unlink()
                        trash_path.parent.mkdir(parents=True, exist_ok=True)

                # Record tombstone.
                size_bytes = trash_path.stat().st_size if trash_path.exists() else 0
                store.add_tombstone(
                    relative_path=safe_path,
                    sha256_hex=matching_reject.sha256_hex,
                    trashed_at_utc=now,
                    marked_reason=matching_reject.marked_reason,
                    trash_relative_path=trash_relative_path,
                    original_size_bytes=size_bytes,
                )

                # Remove the stored-file row; duplicates are computed from
                # api_stored_files, and FK cascade removes dependent catalog rows.
                # Fallback to media-asset-only cleanup for legacy/inconsistent
                # states where the stored-file row is already absent.
                if not store.delete_stored_file(safe_path):
                    store.delete_media_asset(safe_path)
                executed.append(safe_path)
            except OSError as exc:
                APP_LOGGER.warning(
                    "admin_reject_execute_item_failed timestamp=%s relative_path=%s reason=%s",
                    datetime.now(UTC).isoformat(),
                    safe_path,
                    str(exc),
                )
                skipped.append(safe_path)

        APP_LOGGER.info(
            "admin_reject_execute_finished timestamp=%s executed=%s skipped=%s skipped_examples=%s",
            datetime.now(UTC).isoformat(),
            len(executed),
            len(skipped),
            skipped[:10],
        )
        return ExecuteRejectsResponse(executed=executed, skipped=skipped)

    @app.post("/v1/client/tombstone-report", response_model=ClientTombstoneReportResponse)
    def client_tombstone_report(
        payload: ClientTombstoneReportRequest, request: Request
    ) -> ClientTombstoneReportResponse:
        store: UploadStateStore = app.state.upload_state_store
        _require_approved_client(request, store)

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

    # ---------- Phase 3.D: tombstone list + restore -------------------------

    class TombstoneListItem(BaseModel):
        relative_path: str
        sha256_hex: str
        trashed_at_utc: str
        marked_reason: str | None
        trash_relative_path: str
        original_size_bytes: int
        age_days: int
        days_until_purge: int

    class TombstoneListResponse(BaseModel):
        total: int
        limit: int
        offset: int
        items: list[TombstoneListItem]

    class TombstoneRestoreRequest(BaseModel):
        relative_path: str = Field(min_length=1)

    class TombstoneRestoreResponse(BaseModel):
        restored: bool
        relative_path: str
        sha256_hex: str
        restored_at_utc: str

    @app.get("/v1/admin/catalog/tombstones", response_model=TombstoneListResponse)
    def admin_list_tombstones(
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
        older_than_days: int | None = Query(default=None, ge=0),
    ) -> TombstoneListResponse:
        """List tombstoned assets, sorted oldest-first.

        ``older_than_days`` filters to only tombstones whose ``trashed_at_utc``
        is at least that many days ago — used by the UI to highlight items about
        to be purged and by the purge worker.
        """
        store: UploadStateStore = app.state.upload_state_store
        total, tombstones = store.list_tombstones(
            limit=limit, offset=offset, older_than_days=older_than_days
        )
        now = datetime.now(UTC)
        items: list[TombstoneListItem] = []
        for ts in tombstones:
            trashed_at = datetime.fromisoformat(ts.trashed_at_utc)
            if trashed_at.tzinfo is None:
                trashed_at = trashed_at.replace(tzinfo=UTC)
            age_days = max(0, (now - trashed_at).days)
            days_until_purge = max(0, _TRASH_RETENTION_DAYS - age_days)
            items.append(
                TombstoneListItem(
                    relative_path=ts.relative_path,
                    sha256_hex=ts.sha256_hex,
                    trashed_at_utc=ts.trashed_at_utc,
                    marked_reason=ts.marked_reason,
                    trash_relative_path=ts.trash_relative_path,
                    original_size_bytes=ts.original_size_bytes,
                    age_days=age_days,
                    days_until_purge=days_until_purge,
                )
            )
        return TombstoneListResponse(total=total, limit=limit, offset=offset, items=items)

    @app.post("/v1/admin/catalog/tombstones/restore", response_model=TombstoneRestoreResponse)
    def admin_restore_tombstone(payload: TombstoneRestoreRequest) -> TombstoneRestoreResponse:
        """Restore a soft-deleted asset from .trash/ back to its original path.

        Atomic per-path sequence:
        1. Look up tombstone (404 if absent).
        2. Move file from <storage_root>/<trash_relative_path> back to
           <storage_root>/<relative_path>. Returns 409 with code
           ``trash_gone`` if the physical file is missing — the tombstone is
           intentionally left intact so the operator knows the file is lost.
        3. Defensively delete any stale api_media_assets row, then re-insert
           with origin_kind='restored' and observed_at_utc=now.
        4. Remove the tombstone row.
        """
        store: UploadStateStore = app.state.upload_state_store
        safe_path = _require_safe_relative_path(payload.relative_path)

        # 1. Read tombstone (404 if absent).
        tombstone = store.get_tombstone_by_path(safe_path)
        if tombstone is None:
            raise HTTPException(status_code=404, detail="tombstone not found")

        # 2. Move file back from trash.
        trash_path = storage_root_path / tombstone.trash_relative_path
        dest_path = storage_root_path / safe_path

        if not trash_path.is_file():
            raise HTTPException(
                status_code=409,
                detail={"code": "trash_gone", "relative_path": safe_path},
            )

        dest_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.replace(trash_path, dest_path)
        except OSError:
            # Cross-filesystem fallback: copy + fsync + unlink.
            shutil.copy2(str(trash_path), str(dest_path))
            try:
                with dest_path.open("rb") as _fh:
                    os.fsync(_fh.fileno())
            except OSError:
                pass
            trash_path.unlink(missing_ok=True)

        # 3. Defensively clean up any existing api_media_assets row, then
        #    re-insert so the asset reappears in the catalog. Extraction/preview
        #    rows are intentionally NOT recreated — they rebuild on demand via
        #    the existing backfill paths.
        now_utc = datetime.now(UTC).isoformat()
        store.delete_media_asset(safe_path)
        store.upsert_media_asset(
            relative_path=safe_path,
            sha256_hex=tombstone.sha256_hex,
            size_bytes=tombstone.original_size_bytes,
            origin_kind="restored",
            observed_at_utc=now_utc,
        )

        # 4. Remove the tombstone row so subsequent uploads are accepted.
        store.remove_tombstone(safe_path)

        return TombstoneRestoreResponse(
            restored=True,
            relative_path=safe_path,
            sha256_hex=tombstone.sha256_hex,
            restored_at_utc=now_utc,
        )

    @app.post("/v1/admin/catalog/preview/retry", response_model=AdminRetryPreviewResponse)
    def admin_retry_catalog_preview(payload: AdminRetryPreviewRequest) -> AdminRetryPreviewResponse:
        store: UploadStateStore = app.state.upload_state_store
        existing = store.get_media_asset_by_path(payload.relative_path)
        if existing is None:
            raise HTTPException(status_code=404, detail="catalog asset not found")

        _attempt_preview_generation(
            store=store,
            storage_root_path=storage_root_path,
            preview_cache_root_path=preview_cache_root_path,
            preview_max_long_edge=resolved_preview_max_long_edge,
            preview_passthrough_suffixes=resolved_preview_passthrough_suffixes,
            preview_placeholder_suffixes=resolved_preview_placeholder_suffixes,
            relative_path=payload.relative_path,
        )
        updated = store.get_media_asset_by_path(payload.relative_path)
        if updated is None:
            raise HTTPException(status_code=404, detail="catalog asset not found after preview retry")
        return AdminRetryPreviewResponse(item=_to_admin_catalog_item(updated))

    @app.get("/v1/admin/catalog/preview")
    def admin_catalog_preview_file(relative_path: str = Query(min_length=1)) -> FileResponse:
        store: UploadStateStore = app.state.upload_state_store
        record = store.get_media_asset_by_path(relative_path)
        if record is None:
            raise HTTPException(status_code=404, detail="catalog asset not found")
        if record.preview_status != "succeeded":
            raise HTTPException(status_code=404, detail="preview not available")

        if not record.preview_relative_path:
            source_path = (storage_root_path / record.relative_path).resolve()
            try:
                source_path.relative_to(storage_root_path)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail="invalid source path") from exc
            if not source_path.is_file():
                raise HTTPException(status_code=404, detail="source file missing")
            media_type = mimetypes.guess_type(str(source_path.name))[0] or "application/octet-stream"
            return FileResponse(source_path, media_type=media_type)

        preview_path = (preview_cache_root_path / record.preview_relative_path).resolve()
        try:
            preview_path.relative_to(preview_cache_root_path)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="invalid preview path") from exc
        if not preview_path.is_file():
            raise HTTPException(status_code=404, detail="preview file missing")
        return FileResponse(preview_path, media_type="image/jpeg")

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

    @app.post("/v1/admin/catalog/extraction/backfill", response_model=AdminBackfillCatalogResponse)
    def admin_backfill_catalog_extraction(
        payload: AdminBackfillCatalogRequest,
    ) -> AdminBackfillCatalogResponse:
        started_at_utc = datetime.now(UTC).isoformat()
        APP_LOGGER.info(
            "admin_extraction_backfill_started timestamp=%s target_statuses=%s limit=%s origin_kind=%s media_type=%s preview_capability=%s",
            started_at_utc,
            payload.target_statuses,
            payload.limit,
            payload.origin_kind,
            payload.media_type,
            payload.preview_capability,
        )
        _validate_catalog_filter_selection(
            origin_kind=payload.origin_kind,
            media_type=payload.media_type,
            preview_capability=payload.preview_capability,
        )
        requested_statuses = _validate_backfill_target_statuses(
            target_statuses=payload.target_statuses,
            allowed_statuses={"pending", "failed"},
        )

        store: UploadStateStore = app.state.upload_state_store
        candidates = store.list_media_assets_for_extraction(
            extraction_statuses=requested_statuses,
            limit=payload.limit,
            origin_kind=payload.origin_kind,
            media_type=payload.media_type,
            preview_capability=payload.preview_capability,
            cataloged_since_utc=payload.cataloged_since_utc,
            cataloged_before_utc=payload.cataloged_before_utc,
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
        remaining_pending_count, _ = store.list_media_assets(
            limit=1,
            offset=0,
            extraction_status="pending",
            origin_kind=payload.origin_kind,
            media_type=payload.media_type,
            preview_capability=payload.preview_capability,
            cataloged_since_utc=payload.cataloged_since_utc,
            cataloged_before_utc=payload.cataloged_before_utc,
        )
        remaining_failed_count, _ = store.list_media_assets(
            limit=1,
            offset=0,
            extraction_status="failed",
            origin_kind=payload.origin_kind,
            media_type=payload.media_type,
            preview_capability=payload.preview_capability,
            cataloged_since_utc=payload.cataloged_since_utc,
            cataloged_before_utc=payload.cataloged_before_utc,
        )
        run_record = CatalogBackfillRunRecord(
            backfill_kind="extraction",
            requested_statuses=tuple(requested_statuses),
            limit_count=payload.limit,
            filter_origin_kind=payload.origin_kind,
            filter_media_type=payload.media_type,
            filter_preview_capability=payload.preview_capability,
            filter_cataloged_since_utc=payload.cataloged_since_utc,
            filter_cataloged_before_utc=payload.cataloged_before_utc,
            selected_count=len(candidates),
            processed_count=len(updated_items),
            succeeded_count=succeeded_count,
            failed_count=failed_count,
            remaining_pending_count=remaining_pending_count,
            remaining_failed_count=remaining_failed_count,
            completed_at_utc=datetime.now(UTC).isoformat(),
        )
        store.record_catalog_backfill_run(run_record)
        failed_details = [
            f"{item.relative_path}: {item.extraction_failure_detail}"
            for item in updated_items
            if item.extraction_status == "failed" and item.extraction_failure_detail is not None
        ]
        APP_LOGGER.info(
            "admin_extraction_backfill_finished timestamp=%s selected=%s processed=%s succeeded=%s failed=%s failure_examples=%s",
            datetime.now(UTC).isoformat(),
            len(candidates),
            len(updated_items),
            succeeded_count,
            failed_count,
            failed_details[:10],
        )
        return AdminBackfillCatalogResponse(
            run=_to_backfill_run_summary(run_record),
            items=updated_items,
        )

    @app.post("/v1/admin/catalog/preview/backfill", response_model=AdminBackfillCatalogResponse)
    def admin_backfill_catalog_preview(
        payload: AdminBackfillCatalogRequest,
    ) -> AdminBackfillCatalogResponse:
        started_at_utc = datetime.now(UTC).isoformat()
        APP_LOGGER.info(
            "admin_preview_backfill_started timestamp=%s target_statuses=%s limit=%s origin_kind=%s media_type=%s preview_capability=%s",
            started_at_utc,
            payload.target_statuses,
            payload.limit,
            payload.origin_kind,
            payload.media_type,
            payload.preview_capability,
        )
        effective_preview_capability = payload.preview_capability or "previewable"
        _validate_catalog_filter_selection(
            origin_kind=payload.origin_kind,
            media_type=payload.media_type,
            preview_capability=effective_preview_capability,
        )
        requested_statuses = _validate_backfill_target_statuses(
            target_statuses=payload.target_statuses,
            allowed_statuses={"pending", "failed"},
        )

        store: UploadStateStore = app.state.upload_state_store
        candidates = store.list_media_assets_for_preview(
            preview_statuses=requested_statuses,
            limit=payload.limit,
            origin_kind=payload.origin_kind,
            media_type=payload.media_type,
            preview_capability=effective_preview_capability,
            cataloged_since_utc=payload.cataloged_since_utc,
            cataloged_before_utc=payload.cataloged_before_utc,
        )
        updated_items: list[AdminCatalogItem] = []
        for candidate in candidates:
            _attempt_preview_generation(
                store=store,
                storage_root_path=storage_root_path,
                preview_cache_root_path=preview_cache_root_path,
                preview_max_long_edge=resolved_preview_max_long_edge,
                preview_passthrough_suffixes=resolved_preview_passthrough_suffixes,
                preview_placeholder_suffixes=resolved_preview_placeholder_suffixes,
                relative_path=candidate.relative_path,
            )
            updated = store.get_media_asset_by_path(candidate.relative_path)
            if updated is not None:
                updated_items.append(_to_admin_catalog_item(updated))

        succeeded_count = sum(1 for item in updated_items if item.preview_status == "succeeded")
        failed_count = sum(1 for item in updated_items if item.preview_status == "failed")
        remaining_pending_count, _ = store.list_media_assets(
            limit=1,
            offset=0,
            preview_status="pending",
            origin_kind=payload.origin_kind,
            media_type=payload.media_type,
            preview_capability=effective_preview_capability,
            cataloged_since_utc=payload.cataloged_since_utc,
            cataloged_before_utc=payload.cataloged_before_utc,
        )
        remaining_failed_count, _ = store.list_media_assets(
            limit=1,
            offset=0,
            preview_status="failed",
            origin_kind=payload.origin_kind,
            media_type=payload.media_type,
            preview_capability=effective_preview_capability,
            cataloged_since_utc=payload.cataloged_since_utc,
            cataloged_before_utc=payload.cataloged_before_utc,
        )
        run_record = CatalogBackfillRunRecord(
            backfill_kind="preview",
            requested_statuses=tuple(requested_statuses),
            limit_count=payload.limit,
            filter_origin_kind=payload.origin_kind,
            filter_media_type=payload.media_type,
            filter_preview_capability=effective_preview_capability,
            filter_cataloged_since_utc=payload.cataloged_since_utc,
            filter_cataloged_before_utc=payload.cataloged_before_utc,
            selected_count=len(candidates),
            processed_count=len(updated_items),
            succeeded_count=succeeded_count,
            failed_count=failed_count,
            remaining_pending_count=remaining_pending_count,
            remaining_failed_count=remaining_failed_count,
            completed_at_utc=datetime.now(UTC).isoformat(),
        )
        store.record_catalog_backfill_run(run_record)
        failed_details = [
            f"{item.relative_path}: {item.preview_failure_detail}"
            for item in updated_items
            if item.preview_status == "failed" and item.preview_failure_detail is not None
        ]
        APP_LOGGER.info(
            "admin_preview_backfill_finished timestamp=%s selected=%s processed=%s succeeded=%s failed=%s failure_examples=%s",
            datetime.now(UTC).isoformat(),
            len(candidates),
            len(updated_items),
            succeeded_count,
            failed_count,
            failed_details[:10],
        )
        return AdminBackfillCatalogResponse(
            run=_to_backfill_run_summary(run_record),
            items=updated_items,
        )

    @app.get("/v1/admin/catalog/backfill/latest", response_model=AdminLatestCatalogBackfillRunsResponse)
    def admin_latest_catalog_backfill_runs() -> AdminLatestCatalogBackfillRunsResponse:
        store: UploadStateStore = app.state.upload_state_store
        extraction_run = store.get_latest_catalog_backfill_run("extraction")
        preview_run = store.get_latest_catalog_backfill_run("preview")
        return AdminLatestCatalogBackfillRunsResponse(
            extraction_run=(
                _to_backfill_run_summary(extraction_run) if extraction_run is not None else None
            ),
            preview_run=_to_backfill_run_summary(preview_run) if preview_run is not None else None,
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
