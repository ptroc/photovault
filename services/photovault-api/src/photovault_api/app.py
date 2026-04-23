"""Server-side API skeleton for photovault."""

import hashlib
import io
import math
import os
import re
import secrets
import shutil
import subprocess
import tempfile
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse
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
    media_type: str
    preview_capability: str
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
    preview_status: str
    preview_relative_path: str | None
    preview_last_attempted_at_utc: str | None
    preview_last_succeeded_at_utc: str | None
    preview_last_failed_at_utc: str | None
    preview_failure_detail: str | None
    capture_timestamp_utc: str | None
    camera_make: str | None
    camera_model: str | None
    image_width: int | None
    image_height: int | None
    orientation: int | None
    lens_model: str | None
    exposure_time_s: float | None = None
    f_number: float | None = None
    iso_speed: int | None = None
    focal_length_mm: float | None = None
    focal_length_35mm_mm: int | None = None
    is_favorite: bool = False
    is_archived: bool = False
    is_rejected: bool = False


class AdminCatalogListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[AdminCatalogItem]


class AdminRetryExtractionRequest(BaseModel):
    relative_path: str = Field(min_length=1)


class AdminRetryExtractionResponse(BaseModel):
    item: AdminCatalogItem


class AdminRetryPreviewRequest(BaseModel):
    relative_path: str = Field(min_length=1)


class AdminRetryPreviewResponse(BaseModel):
    item: AdminCatalogItem


class AdminCatalogAssetResponse(BaseModel):
    item: AdminCatalogItem


class AdminCatalogFolderItem(BaseModel):
    """One folder in the catalog folder index.

    `path` is the forward-slash-joined directory portion of the asset's
    relative_path, e.g. ``2026/04/Job_A`` for an asset at
    ``2026/04/Job_A/IMG_0001.jpg``. `depth` is the number of path
    segments (``2026`` → 1, ``2026/04`` → 2, ``2026/04/Job_A`` → 3).
    `direct_count` is the number of assets whose folder exactly equals
    ``path``; `total_count` includes assets in subfolders. Clients use
    this to render a folder tree and show counts at every depth.
    """

    path: str
    depth: int
    direct_count: int
    total_count: int


class AdminCatalogFoldersResponse(BaseModel):
    folders: list[AdminCatalogFolderItem]


class AdminCatalogOrganizationRequest(BaseModel):
    relative_path: str = Field(min_length=1)


class AdminCatalogOrganizationResponse(BaseModel):
    item: AdminCatalogItem


# ----- Phase 3.B: reject queue models -------------------------------------
class AdminCatalogRejectRequest(BaseModel):
    relative_path: str = Field(min_length=1)
    marked_reason: str | None = Field(default=None, max_length=500)


class AdminCatalogRejectResponse(BaseModel):
    relative_path: str
    sha256_hex: str
    marked_at_utc: str
    marked_reason: str | None = None
    is_rejected: bool = True


class AdminCatalogRejectUnmarkResponse(BaseModel):
    relative_path: str
    is_rejected: bool = False


class AdminCatalogRejectQueueItem(BaseModel):
    """A reject-queue row plus the matching catalog item.

    The UI needs thumbnails + filenames to let the reviewer restore specific
    items, so we always ship the catalog item in-band. If a catalog row has
    disappeared under the queue (should not happen outside of a Phase 3.C
    delete), the item is ``None`` and the UI falls back to the bare relative
    path rendered from the queue row.
    """

    relative_path: str
    sha256_hex: str
    marked_at_utc: str
    marked_reason: str | None = None
    item: AdminCatalogItem | None = None


class AdminCatalogRejectQueueResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[AdminCatalogRejectQueueItem]


class AdminBackfillCatalogRequest(BaseModel):
    target_statuses: list[str] = Field(default_factory=lambda: ["pending", "failed"], min_length=1)
    limit: int = Field(default=100, ge=1, le=500)
    origin_kind: str | None = None
    media_type: str | None = None
    preview_capability: str | None = None
    cataloged_since_utc: str | None = None
    cataloged_before_utc: str | None = None


class AdminCatalogBackfillRunSummary(BaseModel):
    backfill_kind: str
    requested_statuses: list[str]
    limit: int
    origin_kind: str | None
    media_type: str | None
    preview_capability: str | None
    cataloged_since_utc: str | None
    cataloged_before_utc: str | None
    selected_count: int
    processed_count: int
    succeeded_count: int
    failed_count: int
    remaining_pending_count: int
    remaining_failed_count: int
    completed_at_utc: str


class AdminBackfillCatalogResponse(BaseModel):
    run: AdminCatalogBackfillRunSummary
    items: list[AdminCatalogItem]


class AdminLatestCatalogBackfillRunsResponse(BaseModel):
    extraction_run: AdminCatalogBackfillRunSummary | None
    preview_run: AdminCatalogBackfillRunSummary | None


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


class ClientWorkloadStatus(StrEnum):
    IDLE = "idle"
    WORKING = "working"
    WAITING = "waiting"
    BLOCKED = "blocked"


class ClientPresenceStatus(StrEnum):
    ONLINE = "online"
    STALE = "stale"
    UNKNOWN = "unknown"


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
    heartbeat_last_seen_at_utc: str | None
    heartbeat_presence_status: str
    heartbeat_daemon_state: str | None
    heartbeat_workload_status: ClientWorkloadStatus | None
    heartbeat_active_job_summary: str | None
    heartbeat_retry_backoff_summary: str | None
    heartbeat_auth_block_reason: str | None
    heartbeat_recent_error_summary: str | None


class AdminClientListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[AdminClientItem]


class AdminClientActionResponse(BaseModel):
    item: AdminClientItem


class HeartbeatActiveJobSummary(BaseModel):
    job_id: int = Field(ge=1)
    media_label: str | None = Field(default=None, max_length=255)
    job_status: str = Field(min_length=1, max_length=128)
    ready_to_upload: int = Field(default=0, ge=0)
    uploaded: int = Field(default=0, ge=0)
    retrying: int = Field(default=0, ge=0)
    total_files: int | None = Field(default=None, ge=0)
    non_terminal_files: int | None = Field(default=None, ge=0)
    error_files: int | None = Field(default=None, ge=0)
    blocking_reason: str | None = Field(default=None, max_length=256)


class HeartbeatRetryBackoffSummary(BaseModel):
    pending_count: int = Field(ge=0)
    next_retry_at_utc: datetime | None = None
    reason: str | None = Field(default=None, max_length=512)


class HeartbeatRecentErrorSummary(BaseModel):
    category: str = Field(min_length=1, max_length=128)
    message: str = Field(min_length=1, max_length=512)
    created_at_utc: datetime


class ClientHeartbeatRequest(BaseModel):
    last_seen_at_utc: datetime
    daemon_state: str = Field(min_length=1, max_length=128)
    workload_status: ClientWorkloadStatus
    active_job: HeartbeatActiveJobSummary | None = None
    retry_backoff: HeartbeatRetryBackoffSummary | None = None
    auth_block_reason: str | None = Field(default=None, max_length=128)
    recent_error: HeartbeatRecentErrorSummary | None = None


class ClientHeartbeatResponse(BaseModel):
    status: str
    client_id: str
    last_seen_at_utc: str
    daemon_state: str
    workload_status: ClientWorkloadStatus


_HEARTBEAT_ONLINE_MAX_AGE_SECONDS = 90
_CLIENT_LIST_SCAN_MAX = 5000
_CLIENT_LIST_SCAN_PAGE_SIZE = 200
_PREVIEW_MAX_SIZE = (1024, 1024)
_PREVIEW_RASTER_SUFFIXES = {".png", ".jpg", ".jpeg"}
_PREVIEW_HEIC_SUFFIXES = {".heic", ".heif"}
_PREVIEW_RAW_SUFFIXES = {
    ".arw",
    ".cr2",
    ".cr3",
    ".dng",
    ".nef",
    ".orf",
    ".raf",
    ".rw2",
}
_MEDIA_TYPE_SUFFIXES: dict[str, tuple[str, ...]] = {
    "jpeg": tuple(sorted(_PREVIEW_RASTER_SUFFIXES - {".png"})),
    "png": (".png",),
    "heic": tuple(sorted(_PREVIEW_HEIC_SUFFIXES)),
    "raw": tuple(sorted(_PREVIEW_RAW_SUFFIXES)),
    "video": (".avi", ".m4v", ".mkv", ".mov", ".mp4", ".mts"),
}
_PREVIEWABLE_SUFFIXES = frozenset(
    suffix
    for media_type in ("jpeg", "png", "heic", "raw")
    for suffix in _MEDIA_TYPE_SUFFIXES[media_type]
)
_RAW_EMBEDDED_PREVIEW_TAGS = ("PreviewImage", "JpgFromRaw", "OtherImage", "ThumbnailImage")
_ALLOWED_EXTRACTION_STATUS = {"pending", "succeeded", "failed"}
_ALLOWED_PREVIEW_STATUS = {"pending", "succeeded", "failed"}
_ALLOWED_ORIGIN_KIND = {"uploaded", "indexed"}
_ALLOWED_MEDIA_TYPE = {"jpeg", "png", "heic", "raw", "video", "other"}
_ALLOWED_PREVIEW_CAPABILITY = {"previewable", "not_previewable"}


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


def _normalize_exif_rational(value: object) -> float | None:
    """Coerce EXIF numeric-ish values (int/float/IFDRational/str) to float.

    Pillow returns IFDRational for EXIF rational tags. It supports float()
    conversion but guards against zero denominators by raising. We handle
    that defensively so a corrupt tag never aborts extraction.
    """
    if isinstance(value, bool):  # bool is an int subclass — exclude explicitly
        return None
    if isinstance(value, (int, float)):
        result = float(value)
        if math.isfinite(result):
            return result
        return None
    # Pillow IFDRational + anything with __float__ (duck-typed to stay
    # resilient to library changes).
    if hasattr(value, "__float__"):
        try:
            result = float(value)
        except (ZeroDivisionError, ValueError, TypeError):
            return None
        if math.isfinite(result):
            return result
        return None
    if isinstance(value, tuple) and len(value) == 2:
        numerator, denominator = value
        try:
            numerator_f = float(numerator)
            denominator_f = float(denominator)
        except (TypeError, ValueError):
            return None
        if denominator_f == 0:
            return None
        result = numerator_f / denominator_f
        if math.isfinite(result):
            return result
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        # Accept "1/200", "0.005", "2.8", etc.
        if "/" in stripped:
            parts = stripped.split("/", 1)
            try:
                numerator_f = float(parts[0])
                denominator_f = float(parts[1])
            except ValueError:
                return None
            if denominator_f == 0:
                return None
            result = numerator_f / denominator_f
        else:
            try:
                result = float(stripped)
            except ValueError:
                return None
        if math.isfinite(result):
            return result
        return None
    return None


def _normalize_exif_iso_speed(value: object) -> int | None:
    """EXIF ISOSpeedRatings (tag 34855) is often a tuple like (400,) or a
    scalar int. This normalizes both to a single integer, picking the first
    positive entry for tuples/lists.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, (tuple, list)):
        for candidate in value:
            if isinstance(candidate, bool):
                continue
            if isinstance(candidate, int) and candidate > 0:
                return candidate
            if isinstance(candidate, str):
                stripped = candidate.strip()
                if stripped.isdigit():
                    parsed = int(stripped)
                    if parsed > 0:
                        return parsed
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            parsed = int(stripped)
            return parsed if parsed > 0 else None
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


def _extract_media_metadata(path: Path) -> dict[str, str | int | float | None]:
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

    # Exposure-related EXIF tags. See Phase 3.A plan in
    # docs/proposals/server_ui_catalog_improvements.md §9 for rationale
    # (ExposureTime preferred over ShutterSpeedValue; ISOSpeedRatings may
    # be a tuple; 35mm equivalent focal length is a plain integer).
    return {
        "capture_timestamp_utc": _extract_capture_timestamp_utc(exif_map),
        "camera_make": _normalize_exif_text(exif_map.get(271)),
        "camera_model": _normalize_exif_text(exif_map.get(272)),
        "image_width": int(width),
        "image_height": int(height),
        "orientation": _normalize_exif_int(exif_map.get(274)),
        "lens_model": _normalize_exif_text(exif_map.get(42036)),
        "exposure_time_s": _normalize_exif_rational(exif_map.get(33434)),
        "f_number": _normalize_exif_rational(exif_map.get(33437)),
        "iso_speed": _normalize_exif_iso_speed(exif_map.get(34855)),
        "focal_length_mm": _normalize_exif_rational(exif_map.get(37386)),
        "focal_length_35mm_mm": _normalize_exif_int(exif_map.get(41989)),
    }


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


def _render_raw_preview_source(path: Path) -> Image.Image:
    try:
        preview_bytes = _extract_raw_embedded_preview_bytes(path)
    except OSError as exc:
        raise ValueError(f"RAW embedded preview extraction failed: {exc}") from exc

    try:
        return _open_rgb_image_from_bytes(preview_bytes)
    except (UnidentifiedImageError, OSError, SyntaxError, ValueError) as exc:
        raise ValueError(f"RAW embedded preview data is invalid: {exc}") from exc


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


def _preview_relative_cache_path(*, relative_path: str, sha256_hex: str) -> str:
    source_path = Path(relative_path)
    stem = source_path.stem or "asset"
    parent = source_path.parent.as_posix()
    filename = f"{stem}__{sha256_hex[:12]}__w1024.jpg"
    if parent and parent != ".":
        return f"{parent}/{filename}"
    return filename


def _attempt_preview_generation(
    *,
    store: UploadStateStore,
    storage_root_path: Path,
    preview_cache_root_path: Path,
    relative_path: str,
) -> None:
    now = datetime.now(UTC).isoformat()
    store.ensure_media_asset_preview_row(relative_path=relative_path, recorded_at_utc=now)
    asset = store.get_media_asset_by_path(relative_path)
    if asset is None:
        return
    asset_path = storage_root_path / relative_path

    preview_relative_path = _preview_relative_cache_path(
        relative_path=relative_path,
        sha256_hex=asset.sha256_hex,
    )
    preview_path = preview_cache_root_path / preview_relative_path
    preview_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        if not preview_path.exists():
            with _render_preview_source(asset_path) as preview_image:
                preview_image.thumbnail(_PREVIEW_MAX_SIZE, Image.Resampling.LANCZOS)
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
    resolved_preview_cache_root = (
        os.getenv("PHOTOVAULT_API_PREVIEW_CACHE_ROOT")
        or str(storage_root_path.parent / ".photovault_preview_cache")
    )
    preview_cache_root_path = Path(resolved_preview_cache_root).expanduser().resolve()
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
        if record.preview_status != "succeeded" or not record.preview_relative_path:
            raise HTTPException(status_code=404, detail="preview not available")

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
        return AdminBackfillCatalogResponse(
            run=_to_backfill_run_summary(run_record),
            items=updated_items,
        )

    @app.post("/v1/admin/catalog/preview/backfill", response_model=AdminBackfillCatalogResponse)
    def admin_backfill_catalog_preview(
        payload: AdminBackfillCatalogRequest,
    ) -> AdminBackfillCatalogResponse:
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
