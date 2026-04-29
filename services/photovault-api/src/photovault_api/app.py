"""Server-side API skeleton for photovault."""
import io
import logging
import os
import secrets
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
from datetime import UTC, datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from PIL import Image, UnidentifiedImageError

from photovault_api.state_store import (
    ClientHeartbeatRecord,
    InMemoryUploadStateStore,
    PostgresUploadStateStore,
    UploadStateStore,
)

from .admin_routes import register_admin_routes
from .media import (
    _MEDIA_TYPE_SUFFIXES,
    _PREVIEW_HEIC_SUFFIXES,
    _PREVIEW_RASTER_SUFFIXES,
    _PREVIEW_RAW_SUFFIXES,
    _PREVIEWABLE_SUFFIXES,
    _RAW_EMBEDDED_PREVIEW_TAGS,
    _catalog_origin_for_source_kind,
    _compute_sha256,
    _extract_media_metadata,
    _iter_storage_files,
    _normalize_exif_iso_speed,
    _normalize_exif_rational,
    _resolve_preview_max_long_edge,
    _resolve_preview_suffix_set,
    _sanitize_component,
)
from .media_preview import (
    attempt_media_extraction,
    attempt_preview_generation,
    preview_relative_cache_path,
    upsert_storage_and_catalog_record,
)
from .routes_client_upload import register_client_upload_routes
from .storage_ops import (
    _heartbeat_presence_status,
    _require_approved_client,
)

APP_LOGGER = logging.getLogger("photovault-api.app")

_APP_COMPAT_EXPORTS = (
    os,
    ClientHeartbeatRecord,
    _compute_sha256,
    _heartbeat_presence_status,
    _iter_storage_files,
    _normalize_exif_iso_speed,
    _normalize_exif_rational,
    _require_approved_client,
    _sanitize_component,
)

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
    upsert_storage_and_catalog_record(
        store=store,
        catalog_origin_for_source_kind=_catalog_origin_for_source_kind,
        relative_path=relative_path,
        sha256_hex=sha256_hex,
        size_bytes=size_bytes,
        source_kind=source_kind,
        seen_at_utc=seen_at_utc,
        provenance_job_name=provenance_job_name,
        provenance_original_filename=provenance_original_filename,
    )


def _attempt_media_extraction(
    *,
    store: UploadStateStore,
    storage_root_path: Path,
    relative_path: str,
) -> None:
    attempt_media_extraction(
        store=store,
        storage_root_path=storage_root_path,
        relative_path=relative_path,
        extract_media_metadata=_extract_media_metadata,
    )


def _preview_relative_cache_path(
    *,
    relative_path: str,
    sha256_hex: str,
    preview_max_long_edge: int,
) -> str:
    return preview_relative_cache_path(
        relative_path=relative_path,
        sha256_hex=sha256_hex,
        preview_max_long_edge=preview_max_long_edge,
    )


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
    attempt_preview_generation(
        store=store,
        storage_root_path=storage_root_path,
        preview_cache_root_path=preview_cache_root_path,
        preview_max_long_edge=preview_max_long_edge,
        preview_passthrough_suffixes=preview_passthrough_suffixes,
        preview_placeholder_suffixes=preview_placeholder_suffixes,
        relative_path=relative_path,
        render_preview_source=_render_preview_source,
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
    register_client_upload_routes(app, sys.modules[__name__])

    register_admin_routes(app, sys.modules[__name__])

    return app
