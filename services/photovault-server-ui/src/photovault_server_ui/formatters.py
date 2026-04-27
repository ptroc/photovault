"""Template helpers and data formatters for photovault-server-ui."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable
from urllib.error import URLError
from urllib.parse import parse_qsl, urlencode

from flask import request

ApiFetcher = Callable[[str, dict[str, str]], dict[str, Any]]


def _format_size_bytes(value: int) -> str:
    if value < 1024:
        return f"{value} B"
    if value < 1024 * 1024:
        return f"{value / 1024:.1f} KiB"
    if value < 1024 * 1024 * 1024:
        return f"{value / (1024 * 1024):.1f} MiB"
    return f"{value / (1024 * 1024 * 1024):.1f} GiB"


def _catalog_metadata_summary(item: dict[str, Any]) -> str:
    metadata_bits: list[str] = []
    capture = item.get("capture_timestamp_utc")
    if capture:
        metadata_bits.append(f"captured {_format_timestamp_inline(str(capture))}")
    make = (item.get("camera_make") or "").strip()
    model = (item.get("camera_model") or "").strip()
    if make or model:
        metadata_bits.append("camera " + " ".join([part for part in [make, model] if part]))
    lens_model = (item.get("lens_model") or "").strip()
    if lens_model:
        metadata_bits.append(f"lens {lens_model}")
    width = item.get("image_width")
    height = item.get("image_height")
    if width is not None and height is not None:
        metadata_bits.append(f"{width}x{height}")
    orientation = item.get("orientation")
    if orientation is not None:
        metadata_bits.append(f"orientation {orientation}")
    return " | ".join(metadata_bits)


def _format_shutter_speed(exposure_time_s: float | None) -> str | None:
    """Render an EXIF exposure time as a human-readable shutter speed.

    Sub-second exposures are shown as 1/N (rounded to the nearest integer
    denominator, which is what cameras actually record). One second and above
    are shown as a trimmed float with an "s" suffix. Returns None if the
    input is missing or non-positive.
    """
    if exposure_time_s is None:
        return None
    try:
        value = float(exposure_time_s)
    except (TypeError, ValueError):
        return None
    if not value or value <= 0:
        return None
    if value < 1.0:
        denominator = round(1.0 / value)
        if denominator < 1:
            denominator = 1
        return f"1/{denominator} s"
    # 1.0s and up: trim trailing zeros for readability (e.g. "2 s" not "2.0 s").
    rendered = f"{value:g}"
    return f"{rendered} s"


def _format_exposure_summary(item: dict[str, Any]) -> str:
    """Build a compact "1/200 s · f/2.8 · ISO 400 · 50 mm" summary string."""
    bits: list[str] = []
    shutter = _format_shutter_speed(item.get("exposure_time_s"))
    if shutter:
        bits.append(shutter)
    f_number = item.get("f_number")
    if f_number is not None:
        try:
            f_val = float(f_number)
        except (TypeError, ValueError):
            f_val = None
        if f_val and f_val > 0:
            bits.append(f"f/{f_val:g}")
    iso_speed = item.get("iso_speed")
    if iso_speed is not None:
        try:
            iso_val = int(iso_speed)
        except (TypeError, ValueError):
            iso_val = None
        if iso_val and iso_val > 0:
            bits.append(f"ISO {iso_val}")
    focal = item.get("focal_length_mm")
    focal_35 = item.get("focal_length_35mm_mm")
    focal_part: str | None = None
    if focal is not None:
        try:
            focal_val = float(focal)
        except (TypeError, ValueError):
            focal_val = None
        if focal_val and focal_val > 0:
            focal_part = f"{focal_val:g} mm"
    if focal_35 is not None:
        try:
            focal_35_val = int(focal_35)
        except (TypeError, ValueError):
            focal_35_val = None
        if focal_35_val and focal_35_val > 0:
            if focal_part:
                focal_part = f"{focal_part} ({focal_35_val} mm eq.)"
            else:
                focal_part = f"{focal_35_val} mm eq."
    if focal_part:
        bits.append(focal_part)
    return " \u00b7 ".join(bits)


def _format_sha_for_display(value: str | None, chunk_size: int = 8) -> str:
    if not value:
        return "n/a"
    return " ".join(value[index : index + chunk_size] for index in range(0, len(value), chunk_size))


def _timestamp_parts(value: str | None) -> dict[str, str] | None:
    raw_value = (value or "").strip()
    if not raw_value:
        return None
    try:
        parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError:
        if "T" not in raw_value:
            return {"date": raw_value, "time": ""}
        date_part, time_part = raw_value.split("T", 1)
        return {"date": date_part, "time": time_part}

    date_part = parsed.date().isoformat()
    time_part = parsed.strftime("%H:%M:%S")
    if parsed.tzinfo is not None:
        offset = parsed.utcoffset()
        if offset == timezone.utc.utcoffset(parsed):
            time_part = f"{time_part} UTC"
        else:
            offset_text = parsed.strftime("%z")
            if offset_text:
                offset_text = f"{offset_text[:3]}:{offset_text[3:]}"
                time_part = f"{time_part} {offset_text}"
    return {"date": date_part, "time": time_part}


def _format_timestamp_inline(value: str | None) -> str:
    parts = _timestamp_parts(value)
    if not parts:
        return "n/a"
    if not parts["time"]:
        return parts["date"]
    return f"{parts['date']} {parts['time']}"


def _count_client_summary(items: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "online": sum(1 for item in items if item.get("heartbeat_presence_status") == "online"),
        "stale": sum(1 for item in items if item.get("heartbeat_presence_status") == "stale"),
        "pending": sum(1 for item in items if item.get("enrollment_status") == "pending"),
        "working": sum(1 for item in items if item.get("heartbeat_workload_status") == "working"),
        "blocked": sum(1 for item in items if item.get("heartbeat_workload_status") == "blocked"),
    }


def _catalog_query_state_from_values(values: dict[str, str]) -> dict[str, str]:
    keys = (
        "extraction_status",
        "preview_status",
        "origin_kind",
        "media_type",
        "preview_capability",
        "is_favorite",
        "is_archived",
        "cataloged_since_utc",
        "cataloged_before_utc",
    )
    state: dict[str, str] = {}
    for key in keys:
        value = values.get(key, "").strip()
        if value:
            state[key] = value
    return state


def _catalog_query_state_from_args() -> dict[str, str]:
    values = {key: request.args.get(key, "") for key in request.args.keys()}
    return _catalog_query_state_from_values(values)


def _catalog_query_state_from_form() -> dict[str, str]:
    # If the client sent a single consolidated `return_query` field (new form
    # pattern), prefer it over reconstructing from the individual filter keys.
    # This keeps action templates slim while remaining compatible with callers
    # that still post the individual keys.
    return_query = request.form.get("return_query", "").strip()
    if return_query:
        parsed = dict(parse_qsl(return_query, keep_blank_values=False))
        return _catalog_query_state_from_values(parsed)
    values = {key: request.form.get(key, "") for key in request.form.keys()}
    return _catalog_query_state_from_values(values)


def _local_to_utc_iso(local_value: str) -> str:
    """Convert a browser `datetime-local` string ("YYYY-MM-DDTHH:MM[:SS]") to
    a UTC ISO-8601 string with an explicit offset. Returns empty string on
    missing input; returns the input unchanged if it already carries a
    timezone indicator (so bookmarked UTC URLs still work)."""
    value = (local_value or "").strip()
    if not value:
        return ""
    if value.endswith("Z") or "+" in value or "-" in value[10:]:
        return value
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.isoformat()


def _utc_iso_to_local(utc_value: str) -> str:
    """Best-effort render a stored UTC ISO-8601 string as a value suitable for
    `<input type="datetime-local">` (i.e., "YYYY-MM-DDTHH:MM"). Returns empty
    string on missing or unparseable input."""
    value = (utc_value or "").strip()
    if not value:
        return ""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return ""
    return parsed.strftime("%Y-%m-%dT%H:%M")


def _decorate_catalog_item(item: dict[str, Any]) -> dict[str, Any]:
    """Add derived display fields used by the catalog templates."""
    size_bytes = int(item.get("size_bytes", 0))
    item["size_human"] = _format_size_bytes(size_bytes)
    item["metadata_summary"] = _catalog_metadata_summary(item)
    item["exposure_summary"] = _format_exposure_summary(item)
    shutter = _format_shutter_speed(item.get("exposure_time_s"))
    item["shutter_speed_display"] = shutter or ""
    # Normalize is_rejected in-place so templates can trust a plain bool.
    item["is_rejected"] = bool(item.get("is_rejected", False))
    preview_status = str(item.get("preview_status") or "pending")
    if preview_status == "succeeded":
        item["preview_summary"] = "Preview available"
    elif preview_status == "failed":
        item["preview_summary"] = "Preview failed"
    else:
        item["preview_summary"] = "Preview pending"
    preview_relative_path = str(item.get("preview_relative_path") or "").strip()
    preview_cache_root = (
        os.getenv("PHOTOVAULT_SERVER_UI_PREVIEW_CACHE_ROOT", "").strip()
        or os.getenv("PHOTOVAULT_API_PREVIEW_CACHE_ROOT", "").strip()
    )
    if preview_cache_root and preview_relative_path:
        preview_root_path = Path(preview_cache_root).expanduser()
        preview_relative_posix = PurePosixPath(preview_relative_path)
        item["preview_full_path"] = str(preview_root_path.joinpath(*preview_relative_posix.parts))
    else:
        item["preview_full_path"] = None
    item["filename"] = PurePosixPath(str(item.get("relative_path", ""))).name
    sha_hex = str(item.get("sha256_hex") or "")
    item["sha256_display"] = _format_sha_for_display(sha_hex)
    # Stable, short, DOM-id-safe handle for HTMX swap targets. Using the
    # first 16 hex chars keeps the full SHA256 out of the HTML while still
    # being collision-free within any realistic catalog page.
    if sha_hex:
        item["card_id"] = sha_hex[:16]
    else:
        fallback_name = PurePosixPath(str(item.get("relative_path", ""))).name
        item["card_id"] = fallback_name.replace(".", "-")
    return item


def _fallback_catalog_asset(relative_path: str) -> dict[str, Any]:
    item = {
        "relative_path": relative_path,
        "sha256_hex": "",
        "size_bytes": 0,
        "media_type": "unknown",
        "preview_capability": "not_previewable",
        "origin_kind": "indexed",
        "last_observed_origin_kind": "indexed",
        "provenance_job_name": None,
        "provenance_original_filename": PurePosixPath(relative_path).name,
        "first_cataloged_at_utc": None,
        "last_cataloged_at_utc": None,
        "extraction_status": "unknown",
        "extraction_last_attempted_at_utc": None,
        "extraction_last_succeeded_at_utc": None,
        "extraction_last_failed_at_utc": None,
        "extraction_failure_detail": None,
        "preview_status": "pending",
        "preview_relative_path": None,
        "preview_last_attempted_at_utc": None,
        "preview_last_succeeded_at_utc": None,
        "preview_last_failed_at_utc": None,
        "preview_failure_detail": None,
        "is_favorite": False,
        "is_archived": False,
        "is_rejected": False,
        "capture_timestamp_utc": None,
        "camera_make": None,
        "camera_model": None,
        "image_width": None,
        "image_height": None,
        "orientation": None,
        "lens_model": None,
    }
    return _decorate_catalog_item(item)


def _fetch_catalog_asset_for_display(
    fetcher: ApiFetcher, relative_path: str
) -> dict[str, Any]:
    try:
        payload = fetcher("/v1/admin/catalog/asset", {"relative_path": relative_path})
    except (URLError, TimeoutError, ValueError):
        return _fallback_catalog_asset(relative_path)
    item = dict(payload.get("item") or {})
    if not item:
        return _fallback_catalog_asset(relative_path)
    return _decorate_catalog_item(item)

