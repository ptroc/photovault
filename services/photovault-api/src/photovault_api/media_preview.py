"""Preview and extraction helper implementations for photovault-api."""

from datetime import UTC, datetime
from pathlib import Path

from PIL import Image

from .state_store import UploadStateStore


def upsert_storage_and_catalog_record(
    *,
    store: UploadStateStore,
    catalog_origin_for_source_kind,
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
        origin_kind=catalog_origin_for_source_kind(source_kind),
        observed_at_utc=seen_at_utc,
        provenance_job_name=provenance_job_name,
        provenance_original_filename=provenance_original_filename,
    )


def attempt_media_extraction(
    *,
    store: UploadStateStore,
    storage_root_path: Path,
    relative_path: str,
    extract_media_metadata,
) -> None:
    now = datetime.now(UTC).isoformat()
    store.ensure_media_asset_extraction_row(relative_path=relative_path, recorded_at_utc=now)
    asset_path = storage_root_path / relative_path
    try:
        metadata = extract_media_metadata(asset_path)
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


def preview_relative_cache_path(
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


def attempt_preview_generation(
    *,
    store: UploadStateStore,
    storage_root_path: Path,
    preview_cache_root_path: Path,
    preview_max_long_edge: int,
    preview_passthrough_suffixes: frozenset[str],
    preview_placeholder_suffixes: frozenset[str],
    relative_path: str,
    render_preview_source,
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

    preview_relative_path = preview_relative_cache_path(
        relative_path=relative_path,
        sha256_hex=asset.sha256_hex,
        preview_max_long_edge=preview_max_long_edge,
    )
    preview_path = preview_cache_root_path / preview_relative_path
    preview_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        if not preview_path.exists():
            with render_preview_source(asset_path) as preview_image:
                preview_image.thumbnail(
                    (preview_max_long_edge, preview_max_long_edge),
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
