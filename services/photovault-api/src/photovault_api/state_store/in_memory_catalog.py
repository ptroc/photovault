"""Catalog helpers for InMemoryUploadStateStore."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from .records import (
    CatalogBackfillRunRecord,
    DuplicateShaGroup,
    MediaAssetRecord,
    MediaExtractionRecord,
    MediaPreviewRecord,
    PathConflictRecord,
    RejectedAssetRecord,
    StorageIndexRunRecord,
    StorageSummary,
    StoredFileRecord,
    TombstoneRecord,
    _media_type_for_path,
    _preview_capability_for_path,
)


def upsert_media_asset(
    self,
    *,
    relative_path: str,
    sha256_hex: str,
    size_bytes: int,
    origin_kind: str,
    observed_at_utc: str,
    provenance_job_name: str | None = None,
    provenance_original_filename: str | None = None,
) -> None:
    with self._lock:
        existing = self.media_assets.get(relative_path)
        first_cataloged = existing.first_cataloged_at_utc if existing is not None else observed_at_utc
        self.media_assets[relative_path] = MediaAssetRecord(
            relative_path=relative_path,
            sha256_hex=sha256_hex,
            size_bytes=size_bytes,
            origin_kind=existing.origin_kind if existing is not None else origin_kind,
            last_observed_origin_kind=origin_kind,
            provenance_job_name=(
                provenance_job_name
                if provenance_job_name is not None
                else (existing.provenance_job_name if existing is not None else None)
            ),
            provenance_original_filename=(
                provenance_original_filename
                if provenance_original_filename is not None
                else (existing.provenance_original_filename if existing is not None else None)
            ),
            first_cataloged_at_utc=first_cataloged,
            last_cataloged_at_utc=observed_at_utc,
            extraction_status=existing.extraction_status if existing is not None else "pending",
            extraction_last_attempted_at_utc=(
                existing.extraction_last_attempted_at_utc if existing is not None else None
            ),
            extraction_last_succeeded_at_utc=(
                existing.extraction_last_succeeded_at_utc if existing is not None else None
            ),
            extraction_last_failed_at_utc=(
                existing.extraction_last_failed_at_utc if existing is not None else None
            ),
            extraction_failure_detail=(
                existing.extraction_failure_detail if existing is not None else None
            ),
            preview_status=existing.preview_status if existing is not None else "pending",
            preview_relative_path=existing.preview_relative_path if existing is not None else None,
            preview_last_attempted_at_utc=(
                existing.preview_last_attempted_at_utc if existing is not None else None
            ),
            preview_last_succeeded_at_utc=(
                existing.preview_last_succeeded_at_utc if existing is not None else None
            ),
            preview_last_failed_at_utc=(
                existing.preview_last_failed_at_utc if existing is not None else None
            ),
            preview_failure_detail=existing.preview_failure_detail if existing is not None else None,
            capture_timestamp_utc=existing.capture_timestamp_utc if existing is not None else None,
            camera_make=existing.camera_make if existing is not None else None,
            camera_model=existing.camera_model if existing is not None else None,
            image_width=existing.image_width if existing is not None else None,
            image_height=existing.image_height if existing is not None else None,
            orientation=existing.orientation if existing is not None else None,
            lens_model=existing.lens_model if existing is not None else None,
            is_favorite=existing.is_favorite if existing is not None else False,
            is_archived=existing.is_archived if existing is not None else False,
        )
        self.media_asset_extractions.setdefault(
            relative_path,
            MediaExtractionRecord(
                relative_path=relative_path,
                extraction_status="pending",
                extraction_last_attempted_at_utc=None,
                extraction_last_succeeded_at_utc=None,
                extraction_last_failed_at_utc=None,
                extraction_failure_detail=None,
                capture_timestamp_utc=None,
                camera_make=None,
                camera_model=None,
                image_width=None,
                image_height=None,
                orientation=None,
                lens_model=None,
            ),
        )
        self.media_asset_previews.setdefault(
            relative_path,
            MediaPreviewRecord(
                relative_path=relative_path,
                preview_status="pending",
                preview_relative_path=None,
                preview_last_attempted_at_utc=None,
                preview_last_succeeded_at_utc=None,
                preview_last_failed_at_utc=None,
                preview_failure_detail=None,
            ),
        )

def list_media_assets(
    self,
    *,
    limit: int,
    offset: int,
    extraction_status: str | None = None,
    preview_status: str | None = None,
    origin_kind: str | None = None,
    media_type: str | None = None,
    preview_capability: str | None = None,
    is_favorite: bool | None = None,
    is_archived: bool | None = None,
    cataloged_since_utc: str | None = None,
    cataloged_before_utc: str | None = None,
    relative_path_prefix: str | None = None,
) -> tuple[int, list[MediaAssetRecord]]:
    with self._lock:
        ordered = sorted(self.media_assets.values(), key=lambda item: item.relative_path)
        ordered = sorted(ordered, key=lambda item: item.last_cataloged_at_utc, reverse=True)
        if extraction_status is not None:
            ordered = [item for item in ordered if item.extraction_status == extraction_status]
        if preview_status is not None:
            ordered = [item for item in ordered if item.preview_status == preview_status]
        if origin_kind is not None:
            ordered = [item for item in ordered if item.origin_kind == origin_kind]
        if media_type is not None:
            ordered = [
                item for item in ordered if _media_type_for_path(item.relative_path) == media_type
            ]
        if preview_capability is not None:
            ordered = [
                item
                for item in ordered
                if _preview_capability_for_path(item.relative_path) == preview_capability
            ]
        if is_favorite is not None:
            ordered = [item for item in ordered if item.is_favorite == is_favorite]
        if is_archived is not None:
            ordered = [item for item in ordered if item.is_archived == is_archived]
        if cataloged_since_utc is not None:
            ordered = [item for item in ordered if item.last_cataloged_at_utc >= cataloged_since_utc]
        if cataloged_before_utc is not None:
            ordered = [item for item in ordered if item.last_cataloged_at_utc <= cataloged_before_utc]
        if relative_path_prefix:
            # Match both direct folder (prefix/child) and the folder itself.
            # We normalize by stripping trailing '/' so the operator can
            # pass either form and get the same result.
            normalized = relative_path_prefix.rstrip("/") + "/"
            ordered = [
                item for item in ordered if item.relative_path.startswith(normalized)
            ]
        total = len(ordered)
        return total, ordered[offset : offset + limit]

def list_media_asset_folders(self) -> list[tuple[str, int, int, int]]:
    # Aggregate folders from the in-memory asset dict. "Folder" is the
    # directory portion of relative_path; depth is the number of
    # path segments. direct_count counts assets whose folder equals
    # this path exactly; total_count includes assets in sub-folders.
    with self._lock:
        direct: dict[str, int] = {}
        total: dict[str, int] = {}
        for record in self.media_assets.values():
            parts = record.relative_path.split("/")
            if len(parts) <= 1:
                continue
            folder_parts = parts[:-1]
            direct_path = "/".join(folder_parts)
            direct[direct_path] = direct.get(direct_path, 0) + 1
            for depth in range(1, len(folder_parts) + 1):
                ancestor = "/".join(folder_parts[:depth])
                total[ancestor] = total.get(ancestor, 0) + 1
        rows: list[tuple[str, int, int, int]] = []
        for path in sorted(total.keys()):
            depth = path.count("/") + 1
            rows.append((path, depth, direct.get(path, 0), total[path]))
        return rows

def get_media_asset_by_path(self, relative_path: str) -> MediaAssetRecord | None:
    with self._lock:
        return self.media_assets.get(relative_path)

def set_media_asset_favorite(
    self, *, relative_path: str, is_favorite: bool, updated_at_utc: str
) -> MediaAssetRecord | None:
    del updated_at_utc
    with self._lock:
        existing = self.media_assets.get(relative_path)
        if existing is None:
            return None
        updated = MediaAssetRecord(
            relative_path=existing.relative_path,
            sha256_hex=existing.sha256_hex,
            size_bytes=existing.size_bytes,
            origin_kind=existing.origin_kind,
            last_observed_origin_kind=existing.last_observed_origin_kind,
            provenance_job_name=existing.provenance_job_name,
            provenance_original_filename=existing.provenance_original_filename,
            first_cataloged_at_utc=existing.first_cataloged_at_utc,
            last_cataloged_at_utc=existing.last_cataloged_at_utc,
            extraction_status=existing.extraction_status,
            extraction_last_attempted_at_utc=existing.extraction_last_attempted_at_utc,
            extraction_last_succeeded_at_utc=existing.extraction_last_succeeded_at_utc,
            extraction_last_failed_at_utc=existing.extraction_last_failed_at_utc,
            extraction_failure_detail=existing.extraction_failure_detail,
            preview_status=existing.preview_status,
            preview_relative_path=existing.preview_relative_path,
            preview_last_attempted_at_utc=existing.preview_last_attempted_at_utc,
            preview_last_succeeded_at_utc=existing.preview_last_succeeded_at_utc,
            preview_last_failed_at_utc=existing.preview_last_failed_at_utc,
            preview_failure_detail=existing.preview_failure_detail,
            capture_timestamp_utc=existing.capture_timestamp_utc,
            camera_make=existing.camera_make,
            camera_model=existing.camera_model,
            image_width=existing.image_width,
            image_height=existing.image_height,
            orientation=existing.orientation,
            lens_model=existing.lens_model,
            exposure_time_s=existing.exposure_time_s,
            f_number=existing.f_number,
            iso_speed=existing.iso_speed,
            focal_length_mm=existing.focal_length_mm,
            focal_length_35mm_mm=existing.focal_length_35mm_mm,
            is_favorite=is_favorite,
            is_archived=existing.is_archived,
        )
        self.media_assets[relative_path] = updated
        return updated

def set_media_asset_archived(
    self, *, relative_path: str, is_archived: bool, updated_at_utc: str
) -> MediaAssetRecord | None:
    del updated_at_utc
    with self._lock:
        existing = self.media_assets.get(relative_path)
        if existing is None:
            return None
        updated = MediaAssetRecord(
            relative_path=existing.relative_path,
            sha256_hex=existing.sha256_hex,
            size_bytes=existing.size_bytes,
            origin_kind=existing.origin_kind,
            last_observed_origin_kind=existing.last_observed_origin_kind,
            provenance_job_name=existing.provenance_job_name,
            provenance_original_filename=existing.provenance_original_filename,
            first_cataloged_at_utc=existing.first_cataloged_at_utc,
            last_cataloged_at_utc=existing.last_cataloged_at_utc,
            extraction_status=existing.extraction_status,
            extraction_last_attempted_at_utc=existing.extraction_last_attempted_at_utc,
            extraction_last_succeeded_at_utc=existing.extraction_last_succeeded_at_utc,
            extraction_last_failed_at_utc=existing.extraction_last_failed_at_utc,
            extraction_failure_detail=existing.extraction_failure_detail,
            preview_status=existing.preview_status,
            preview_relative_path=existing.preview_relative_path,
            preview_last_attempted_at_utc=existing.preview_last_attempted_at_utc,
            preview_last_succeeded_at_utc=existing.preview_last_succeeded_at_utc,
            preview_last_failed_at_utc=existing.preview_last_failed_at_utc,
            preview_failure_detail=existing.preview_failure_detail,
            capture_timestamp_utc=existing.capture_timestamp_utc,
            camera_make=existing.camera_make,
            camera_model=existing.camera_model,
            image_width=existing.image_width,
            image_height=existing.image_height,
            orientation=existing.orientation,
            lens_model=existing.lens_model,
            exposure_time_s=existing.exposure_time_s,
            f_number=existing.f_number,
            iso_speed=existing.iso_speed,
            focal_length_mm=existing.focal_length_mm,
            focal_length_35mm_mm=existing.focal_length_35mm_mm,
            is_favorite=existing.is_favorite,
            is_archived=is_archived,
        )
        self.media_assets[relative_path] = updated
        return updated

# -------- Phase 3.B: reject queue -----------------------------------
def add_catalog_reject(
    self,
    *,
    relative_path: str,
    marked_at_utc: str,
    marked_reason: str | None = None,
) -> RejectedAssetRecord | None:
    with self._lock:
        asset = self.media_assets.get(relative_path)
        if asset is None:
            return None
        existing = self.media_asset_rejects.get(relative_path)
        # Idempotent: keep the first-marked timestamp on repeated adds so
        # the UI can show "marked since". Allow reason to be refreshed.
        first_marked = (
            existing.marked_at_utc if existing is not None else marked_at_utc
        )
        if marked_reason is not None:
            effective_reason: str | None = marked_reason
        elif existing is not None:
            effective_reason = existing.marked_reason
        else:
            effective_reason = None
        record = RejectedAssetRecord(
            relative_path=relative_path,
            sha256_hex=asset.sha256_hex,
            marked_at_utc=first_marked,
            marked_reason=effective_reason,
        )
        self.media_asset_rejects[relative_path] = record
        return record

def remove_catalog_reject(self, relative_path: str) -> bool:
    with self._lock:
        return self.media_asset_rejects.pop(relative_path, None) is not None

def is_catalog_reject(self, relative_path: str) -> bool:
    with self._lock:
        return relative_path in self.media_asset_rejects

def count_catalog_rejects(self) -> int:
    with self._lock:
        return len(self.media_asset_rejects)

def list_catalog_rejects(
    self, *, limit: int, offset: int
) -> tuple[int, list[RejectedAssetRecord]]:
    with self._lock:
        rows = sorted(
            self.media_asset_rejects.values(),
            key=lambda record: (record.marked_at_utc, record.relative_path),
        )
        total = len(rows)
        if limit <= 0:
            return total, []
        if offset < 0:
            offset = 0
        return total, rows[offset : offset + limit]

# -------- Phase 3.C: tombstones -----------------------------------
def add_tombstone(
    self,
    *,
    relative_path: str,
    sha256_hex: str,
    trashed_at_utc: str,
    marked_reason: str | None,
    trash_relative_path: str,
    original_size_bytes: int,
) -> TombstoneRecord:
    with self._lock:
        record = TombstoneRecord(
            relative_path=relative_path,
            sha256_hex=sha256_hex,
            trashed_at_utc=trashed_at_utc,
            marked_reason=marked_reason,
            trash_relative_path=trash_relative_path,
            original_size_bytes=original_size_bytes,
        )
        self.tombstones[sha256_hex] = record
        return record

def is_sha_tombstoned(self, sha256_hex: str) -> bool:
    with self._lock:
        return sha256_hex in self.tombstones

def list_sha_tombstones(self, shas: list[str]) -> list[TombstoneRecord]:
    with self._lock:
        return [self.tombstones[sha] for sha in shas if sha in self.tombstones]

def get_tombstone_by_path(self, relative_path: str) -> TombstoneRecord | None:
    with self._lock:
        for record in self.tombstones.values():
            if record.relative_path == relative_path:
                return record
        return None

def remove_tombstone(self, relative_path: str) -> bool:
    with self._lock:
        for sha, record in list(self.tombstones.items()):
            if record.relative_path == relative_path:
                del self.tombstones[sha]
                return True
        return False

def list_tombstones(
    self,
    *,
    limit: int,
    offset: int,
    older_than_days: int | None = None,
) -> tuple[int, list[TombstoneRecord]]:
    with self._lock:
        rows = sorted(self.tombstones.values(), key=lambda r: r.trashed_at_utc)
        if older_than_days is not None:
            cutoff = (datetime.now(UTC) - timedelta(days=older_than_days)).isoformat()
            rows = [r for r in rows if r.trashed_at_utc <= cutoff]
        total = len(rows)
        return total, rows[offset : offset + limit]

def purge_tombstones(
    self,
    *,
    older_than_days: int,
    max_batch: int,
) -> list[TombstoneRecord]:
    with self._lock:
        cutoff = (datetime.now(UTC) - timedelta(days=older_than_days)).isoformat()
        candidates = sorted(
            [r for r in self.tombstones.values() if r.trashed_at_utc <= cutoff],
            key=lambda r: r.trashed_at_utc,
        )[:max_batch]
        for record in candidates:
            self.tombstones.pop(record.sha256_hex, None)
        return candidates

def delete_media_asset(self, relative_path: str) -> bool:
    """Remove the media asset row and its dependents from the in-memory store.

    Mirrors ON DELETE CASCADE behaviour from Postgres:
    - api_media_assets row removed
    - api_media_asset_extractions row removed (if present)
    - api_media_asset_previews row removed (if present)
    - api_catalog_reject_queue row removed (if present, via cascade from assets)
    """
    with self._lock:
        if relative_path not in self.media_assets:
            return False
        del self.media_assets[relative_path]
        self.media_asset_extractions.pop(relative_path, None)
        self.media_asset_previews.pop(relative_path, None)
        self.media_asset_rejects.pop(relative_path, None)
        return True

def _filter_assets_for_backfill(
    self,
    *,
    assets: list[MediaAssetRecord],
    statuses: set[str],
    status_field: str,
    origin_kind: str | None,
    media_type: str | None,
    preview_capability: str | None,
    cataloged_since_utc: str | None,
    cataloged_before_utc: str | None,
) -> list[MediaAssetRecord]:
    filtered = [item for item in assets if str(getattr(item, status_field)) in statuses]
    if origin_kind is not None:
        filtered = [item for item in filtered if item.origin_kind == origin_kind]
    if media_type is not None:
        filtered = [item for item in filtered if _media_type_for_path(item.relative_path) == media_type]
    if preview_capability is not None:
        filtered = [
            item
            for item in filtered
            if _preview_capability_for_path(item.relative_path) == preview_capability
        ]
    if cataloged_since_utc is not None:
        filtered = [item for item in filtered if item.last_cataloged_at_utc >= cataloged_since_utc]
    if cataloged_before_utc is not None:
        filtered = [item for item in filtered if item.last_cataloged_at_utc <= cataloged_before_utc]
    return filtered

def list_media_assets_for_extraction(
    self,
    *,
    extraction_statuses: list[str],
    limit: int,
    origin_kind: str | None = None,
    media_type: str | None = None,
    preview_capability: str | None = None,
    cataloged_since_utc: str | None = None,
    cataloged_before_utc: str | None = None,
) -> list[MediaAssetRecord]:
    if limit <= 0 or not extraction_statuses:
        return []
    status_filter = set(extraction_statuses)
    with self._lock:
        ordered = sorted(self.media_assets.values(), key=lambda item: item.relative_path)
        ordered = sorted(ordered, key=lambda item: item.last_cataloged_at_utc, reverse=True)
        filtered = self._filter_assets_for_backfill(
            assets=ordered,
            statuses=status_filter,
            status_field="extraction_status",
            origin_kind=origin_kind,
            media_type=media_type,
            preview_capability=preview_capability,
            cataloged_since_utc=cataloged_since_utc,
            cataloged_before_utc=cataloged_before_utc,
        )
        return filtered[:limit]

def list_media_assets_for_preview(
    self,
    *,
    preview_statuses: list[str],
    limit: int,
    origin_kind: str | None = None,
    media_type: str | None = None,
    preview_capability: str | None = None,
    cataloged_since_utc: str | None = None,
    cataloged_before_utc: str | None = None,
) -> list[MediaAssetRecord]:
    if limit <= 0 or not preview_statuses:
        return []
    status_filter = set(preview_statuses)
    with self._lock:
        ordered = sorted(self.media_assets.values(), key=lambda item: item.relative_path)
        ordered = sorted(ordered, key=lambda item: item.last_cataloged_at_utc, reverse=True)
        filtered = self._filter_assets_for_backfill(
            assets=ordered,
            statuses=status_filter,
            status_field="preview_status",
            origin_kind=origin_kind,
            media_type=media_type,
            preview_capability=preview_capability,
            cataloged_since_utc=cataloged_since_utc,
            cataloged_before_utc=cataloged_before_utc,
        )
        return filtered[:limit]

def ensure_media_asset_extraction_row(self, *, relative_path: str, recorded_at_utc: str) -> None:
    del recorded_at_utc
    with self._lock:
        self.media_asset_extractions.setdefault(
            relative_path,
            MediaExtractionRecord(
                relative_path=relative_path,
                extraction_status="pending",
                extraction_last_attempted_at_utc=None,
                extraction_last_succeeded_at_utc=None,
                extraction_last_failed_at_utc=None,
                extraction_failure_detail=None,
                capture_timestamp_utc=None,
                camera_make=None,
                camera_model=None,
                image_width=None,
                image_height=None,
                orientation=None,
                lens_model=None,
            ),
        )

def upsert_media_asset_extraction(
    self,
    *,
    relative_path: str,
    extraction_status: str,
    attempted_at_utc: str | None,
    succeeded_at_utc: str | None,
    failed_at_utc: str | None,
    failure_detail: str | None,
    capture_timestamp_utc: str | None,
    camera_make: str | None,
    camera_model: str | None,
    image_width: int | None,
    image_height: int | None,
    orientation: int | None,
    lens_model: str | None,
    exposure_time_s: float | None = None,
    f_number: float | None = None,
    iso_speed: int | None = None,
    focal_length_mm: float | None = None,
    focal_length_35mm_mm: int | None = None,
    recorded_at_utc: str,
) -> None:
    del recorded_at_utc
    with self._lock:
        self.media_asset_extractions[relative_path] = MediaExtractionRecord(
            relative_path=relative_path,
            extraction_status=extraction_status,
            extraction_last_attempted_at_utc=attempted_at_utc,
            extraction_last_succeeded_at_utc=succeeded_at_utc,
            extraction_last_failed_at_utc=failed_at_utc,
            extraction_failure_detail=failure_detail,
            capture_timestamp_utc=capture_timestamp_utc,
            camera_make=camera_make,
            camera_model=camera_model,
            image_width=image_width,
            image_height=image_height,
            orientation=orientation,
            lens_model=lens_model,
            exposure_time_s=exposure_time_s,
            f_number=f_number,
            iso_speed=iso_speed,
            focal_length_mm=focal_length_mm,
            focal_length_35mm_mm=focal_length_35mm_mm,
        )
        existing_asset = self.media_assets.get(relative_path)
        if existing_asset is None:
            return
        self.media_assets[relative_path] = MediaAssetRecord(
            relative_path=existing_asset.relative_path,
            sha256_hex=existing_asset.sha256_hex,
            size_bytes=existing_asset.size_bytes,
            origin_kind=existing_asset.origin_kind,
            last_observed_origin_kind=existing_asset.last_observed_origin_kind,
            provenance_job_name=existing_asset.provenance_job_name,
            provenance_original_filename=existing_asset.provenance_original_filename,
            first_cataloged_at_utc=existing_asset.first_cataloged_at_utc,
            last_cataloged_at_utc=existing_asset.last_cataloged_at_utc,
            extraction_status=extraction_status,
            extraction_last_attempted_at_utc=attempted_at_utc,
            extraction_last_succeeded_at_utc=succeeded_at_utc,
            extraction_last_failed_at_utc=failed_at_utc,
            extraction_failure_detail=failure_detail,
            preview_status=existing_asset.preview_status,
            preview_relative_path=existing_asset.preview_relative_path,
            preview_last_attempted_at_utc=existing_asset.preview_last_attempted_at_utc,
            preview_last_succeeded_at_utc=existing_asset.preview_last_succeeded_at_utc,
            preview_last_failed_at_utc=existing_asset.preview_last_failed_at_utc,
            preview_failure_detail=existing_asset.preview_failure_detail,
            capture_timestamp_utc=capture_timestamp_utc,
            camera_make=camera_make,
            camera_model=camera_model,
            image_width=image_width,
            image_height=image_height,
            orientation=orientation,
            lens_model=lens_model,
            exposure_time_s=exposure_time_s,
            f_number=f_number,
            iso_speed=iso_speed,
            focal_length_mm=focal_length_mm,
            focal_length_35mm_mm=focal_length_35mm_mm,
            is_favorite=existing_asset.is_favorite,
            is_archived=existing_asset.is_archived,
        )

def ensure_media_asset_preview_row(self, *, relative_path: str, recorded_at_utc: str) -> None:
    del recorded_at_utc
    with self._lock:
        self.media_asset_previews.setdefault(
            relative_path,
            MediaPreviewRecord(
                relative_path=relative_path,
                preview_status="pending",
                preview_relative_path=None,
                preview_last_attempted_at_utc=None,
                preview_last_succeeded_at_utc=None,
                preview_last_failed_at_utc=None,
                preview_failure_detail=None,
            ),
        )

def upsert_media_asset_preview(
    self,
    *,
    relative_path: str,
    preview_status: str,
    preview_relative_path: str | None,
    attempted_at_utc: str | None,
    succeeded_at_utc: str | None,
    failed_at_utc: str | None,
    failure_detail: str | None,
    recorded_at_utc: str,
) -> None:
    del recorded_at_utc
    with self._lock:
        self.media_asset_previews[relative_path] = MediaPreviewRecord(
            relative_path=relative_path,
            preview_status=preview_status,
            preview_relative_path=preview_relative_path,
            preview_last_attempted_at_utc=attempted_at_utc,
            preview_last_succeeded_at_utc=succeeded_at_utc,
            preview_last_failed_at_utc=failed_at_utc,
            preview_failure_detail=failure_detail,
        )
        existing_asset = self.media_assets.get(relative_path)
        if existing_asset is None:
            return
        self.media_assets[relative_path] = MediaAssetRecord(
            relative_path=existing_asset.relative_path,
            sha256_hex=existing_asset.sha256_hex,
            size_bytes=existing_asset.size_bytes,
            origin_kind=existing_asset.origin_kind,
            last_observed_origin_kind=existing_asset.last_observed_origin_kind,
            provenance_job_name=existing_asset.provenance_job_name,
            provenance_original_filename=existing_asset.provenance_original_filename,
            first_cataloged_at_utc=existing_asset.first_cataloged_at_utc,
            last_cataloged_at_utc=existing_asset.last_cataloged_at_utc,
            extraction_status=existing_asset.extraction_status,
            extraction_last_attempted_at_utc=existing_asset.extraction_last_attempted_at_utc,
            extraction_last_succeeded_at_utc=existing_asset.extraction_last_succeeded_at_utc,
            extraction_last_failed_at_utc=existing_asset.extraction_last_failed_at_utc,
            extraction_failure_detail=existing_asset.extraction_failure_detail,
            preview_status=preview_status,
            preview_relative_path=preview_relative_path,
            preview_last_attempted_at_utc=attempted_at_utc,
            preview_last_succeeded_at_utc=succeeded_at_utc,
            preview_last_failed_at_utc=failed_at_utc,
            preview_failure_detail=failure_detail,
            capture_timestamp_utc=existing_asset.capture_timestamp_utc,
            camera_make=existing_asset.camera_make,
            camera_model=existing_asset.camera_model,
            image_width=existing_asset.image_width,
            image_height=existing_asset.image_height,
            orientation=existing_asset.orientation,
            lens_model=existing_asset.lens_model,
            exposure_time_s=existing_asset.exposure_time_s,
            f_number=existing_asset.f_number,
            iso_speed=existing_asset.iso_speed,
            focal_length_mm=existing_asset.focal_length_mm,
            focal_length_35mm_mm=existing_asset.focal_length_35mm_mm,
            is_favorite=existing_asset.is_favorite,
            is_archived=existing_asset.is_archived,
        )

def list_duplicate_sha_groups(
    self, *, limit: int, offset: int
) -> tuple[int, list[DuplicateShaGroup]]:
    with self._lock:
        grouped: dict[str, list[StoredFileRecord]] = {}
        for record in self.stored_files.values():
            grouped.setdefault(record.sha256_hex, []).append(record)
        groups = [
            DuplicateShaGroup(
                sha256_hex=sha256_hex,
                file_count=len(records),
                first_seen_at_utc=min(record.first_seen_at_utc for record in records),
                last_seen_at_utc=max(record.last_seen_at_utc for record in records),
                relative_paths=tuple(sorted(record.relative_path for record in records)),
            )
            for sha256_hex, records in grouped.items()
            if len(records) > 1
        ]
        ordered = sorted(
            groups,
            key=lambda item: (-item.file_count, item.last_seen_at_utc, item.sha256_hex),
            reverse=False,
        )
        total = len(ordered)
        return total, ordered[offset : offset + limit]

def record_path_conflict(
    self,
    *,
    relative_path: str,
    previous_sha256_hex: str,
    current_sha256_hex: str,
    detected_at_utc: str,
) -> None:
    with self._lock:
        self.path_conflicts.append(
            PathConflictRecord(
                relative_path=relative_path,
                previous_sha256_hex=previous_sha256_hex,
                current_sha256_hex=current_sha256_hex,
                detected_at_utc=detected_at_utc,
            )
        )

def list_path_conflicts(self, *, limit: int, offset: int) -> tuple[int, list[PathConflictRecord]]:
    with self._lock:
        ordered = sorted(
            self.path_conflicts,
            key=lambda item: (item.detected_at_utc, item.relative_path),
            reverse=True,
        )
        total = len(ordered)
        return total, ordered[offset : offset + limit]

def record_storage_index_run(self, record: StorageIndexRunRecord) -> None:
    with self._lock:
        self.latest_index_run = record

def get_latest_storage_index_run(self) -> StorageIndexRunRecord | None:
    with self._lock:
        return self.latest_index_run

def record_catalog_backfill_run(self, record: CatalogBackfillRunRecord) -> None:
    with self._lock:
        self.latest_catalog_backfill_runs[record.backfill_kind] = record

def get_latest_catalog_backfill_run(self, backfill_kind: str) -> CatalogBackfillRunRecord | None:
    with self._lock:
        return self.latest_catalog_backfill_runs.get(backfill_kind)

def summarize_storage(self) -> StorageSummary:
    now = datetime.now(UTC)
    threshold = now - timedelta(hours=24)
    with self._lock:
        records = list(self.stored_files.values())
        duplicate_file_paths = len(records) - len({record.sha256_hex for record in records})
        indexed_records = [record for record in records if record.source_kind == "index_scan"]
        uploaded_records = [record for record in records if record.source_kind == "upload_verify"]
        recent_indexed = 0
        recent_uploaded = 0
        last_indexed: str | None = None
        last_uploaded: str | None = None

        for record in indexed_records:
            try:
                seen_at = datetime.fromisoformat(record.last_seen_at_utc)
            except ValueError:
                continue
            if seen_at >= threshold:
                recent_indexed += 1
            if last_indexed is None or record.last_seen_at_utc > last_indexed:
                last_indexed = record.last_seen_at_utc

        for record in uploaded_records:
            try:
                seen_at = datetime.fromisoformat(record.last_seen_at_utc)
            except ValueError:
                continue
            if seen_at >= threshold:
                recent_uploaded += 1
            if last_uploaded is None or record.last_seen_at_utc > last_uploaded:
                last_uploaded = record.last_seen_at_utc

        return StorageSummary(
            total_known_sha256=len(self.known_sha256),
            total_stored_files=len(records),
            indexed_files=len(indexed_records),
            uploaded_files=len(uploaded_records),
            duplicate_file_paths=duplicate_file_paths,
            recent_indexed_files_24h=recent_indexed,
            recent_uploaded_files_24h=recent_uploaded,
            last_indexed_at_utc=last_indexed,
            last_uploaded_at_utc=last_uploaded,
        )
