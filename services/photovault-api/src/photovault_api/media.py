"""Media metadata extraction and preview rendering helpers for photovault-api."""
from __future__ import annotations

import hashlib
import io
import math
import re
import shutil
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from PIL import Image, UnidentifiedImageError




_HEARTBEAT_ONLINE_MAX_AGE_SECONDS = 90
_CLIENT_LIST_SCAN_MAX = 5000
_CLIENT_LIST_SCAN_PAGE_SIZE = 200
_DEFAULT_PREVIEW_MAX_LONG_EDGE = 1024
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


def _resolve_preview_max_long_edge(raw_value: str | int | None) -> int:
    if raw_value is None:
        return _DEFAULT_PREVIEW_MAX_LONG_EDGE
    if isinstance(raw_value, bool):
        raise RuntimeError("PHOTOVAULT_API_PREVIEW_MAX_LONG_EDGE must be a positive integer")
    if isinstance(raw_value, int):
        parsed = raw_value
    else:
        stripped = raw_value.strip()
        if not stripped:
            return _DEFAULT_PREVIEW_MAX_LONG_EDGE
        try:
            parsed = int(stripped)
        except ValueError as exc:
            raise RuntimeError("PHOTOVAULT_API_PREVIEW_MAX_LONG_EDGE must be a positive integer") from exc
    if parsed <= 0:
        raise RuntimeError("PHOTOVAULT_API_PREVIEW_MAX_LONG_EDGE must be a positive integer")
    return parsed


def _preview_max_size(max_long_edge: int) -> tuple[int, int]:
    return (max_long_edge, max_long_edge)


def _normalize_preview_suffix_token(raw_value: str, *, env_name: str) -> str:
    token = raw_value.strip().lower()
    if not token:
        raise RuntimeError(
            f"{env_name} must be a comma-separated list of file suffixes like '.jpg,.png'"
        )
    normalized = token if token.startswith(".") else f".{token}"
    if len(normalized) <= 1:
        raise RuntimeError(
            f"{env_name} must be a comma-separated list of file suffixes like '.jpg,.png'"
        )
    if "/" in normalized or "\\" in normalized or "," in normalized or " " in normalized:
        raise RuntimeError(
            f"{env_name} must be a comma-separated list of file suffixes like '.jpg,.png'"
        )
    return normalized


def _resolve_preview_suffix_set(
    raw_value: str | list[str] | set[str] | tuple[str, ...] | None,
    *,
    env_name: str,
) -> frozenset[str]:
    if raw_value is None:
        return frozenset()

    tokens: list[str]
    if isinstance(raw_value, str):
        if not raw_value.strip():
            return frozenset()
        tokens = [token for token in raw_value.split(",") if token.strip()]
    else:
        tokens = [str(token) for token in raw_value]

    normalized: set[str] = set()
    for token in tokens:
        normalized.add(_normalize_preview_suffix_token(token, env_name=env_name))
    return frozenset(normalized)


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
