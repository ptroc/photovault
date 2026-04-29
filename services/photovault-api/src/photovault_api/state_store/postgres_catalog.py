"""Catalog helpers for PostgresUploadStateStore."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from .records import (
    _MEDIA_TYPE_SUFFIXES,
    _PREVIEWABLE_SUFFIXES,
    CatalogBackfillRunRecord,
    DuplicateShaGroup,
    MediaAssetRecord,
    PathConflictRecord,
    RejectedAssetRecord,
    StorageIndexRunRecord,
    StorageSummary,
    TombstoneRecord,
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
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO api_media_assets (
                    relative_path,
                    sha256_hex,
                    size_bytes,
                    origin_kind,
                    last_observed_origin_kind,
                    provenance_job_name,
                    provenance_original_filename,
                    first_cataloged_at_utc,
                    last_cataloged_at_utc,
                    is_favorite,
                    is_archived
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE, FALSE)
                ON CONFLICT (relative_path) DO UPDATE
                SET sha256_hex = EXCLUDED.sha256_hex,
                    size_bytes = EXCLUDED.size_bytes,
                    last_observed_origin_kind = EXCLUDED.last_observed_origin_kind,
                    provenance_job_name = COALESCE(
                        EXCLUDED.provenance_job_name,
                        api_media_assets.provenance_job_name
                    ),
                    provenance_original_filename = COALESCE(
                        EXCLUDED.provenance_original_filename,
                        api_media_assets.provenance_original_filename
                    ),
                    last_cataloged_at_utc = EXCLUDED.last_cataloged_at_utc,
                    is_favorite = api_media_assets.is_favorite,
                    is_archived = api_media_assets.is_archived;
                """,
                (
                    relative_path,
                    sha256_hex,
                    size_bytes,
                    origin_kind,
                    origin_kind,
                    provenance_job_name,
                    provenance_original_filename,
                    observed_at_utc,
                    observed_at_utc,
                ),
            )
        conn.commit()

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
    where_clauses = []
    params: list[object] = []
    if extraction_status is not None:
        where_clauses.append("COALESCE(me.extraction_status, 'pending') = %s")
        params.append(extraction_status)
    if preview_status is not None:
        where_clauses.append("COALESCE(mp.preview_status, 'pending') = %s")
        params.append(preview_status)
    if origin_kind is not None:
        where_clauses.append("ma.origin_kind = %s")
        params.append(origin_kind)
    if is_favorite is not None:
        where_clauses.append("ma.is_favorite = %s")
        params.append(is_favorite)
    if is_archived is not None:
        where_clauses.append("ma.is_archived = %s")
        params.append(is_archived)
    if relative_path_prefix:
        normalized_prefix = relative_path_prefix.rstrip("/") + "/"
        where_clauses.append("ma.relative_path LIKE %s")
        # Escape SQL LIKE wildcards in the prefix so "%" and "_" in folder
        # names cannot match unrelated paths.
        escaped = (
            normalized_prefix.replace("\\", "\\\\")
            .replace("%", "\\%")
            .replace("_", "\\_")
        )
        params.append(escaped + "%")
    if media_type is not None:
        media_type_suffixes = _MEDIA_TYPE_SUFFIXES.get(media_type)
        if media_type_suffixes is not None:
            like_clauses = ["LOWER(ma.relative_path) LIKE %s" for _ in media_type_suffixes]
            where_clauses.append("(" + " OR ".join(like_clauses) + ")")
            params.extend([f"%{suffix}" for suffix in media_type_suffixes])
        else:
            all_suffixes = [suffix for values in _MEDIA_TYPE_SUFFIXES.values() for suffix in values]
            not_like_clauses = ["LOWER(ma.relative_path) NOT LIKE %s" for _ in all_suffixes]
            where_clauses.append("(" + " AND ".join(not_like_clauses) + ")")
            params.extend([f"%{suffix}" for suffix in all_suffixes])
    if preview_capability is not None:
        previewable_suffixes = tuple(sorted(_PREVIEWABLE_SUFFIXES))
        if preview_capability == "previewable":
            like_clauses = ["LOWER(ma.relative_path) LIKE %s" for _ in previewable_suffixes]
            where_clauses.append("(" + " OR ".join(like_clauses) + ")")
            params.extend([f"%{suffix}" for suffix in previewable_suffixes])
        else:
            not_like_clauses = ["LOWER(ma.relative_path) NOT LIKE %s" for _ in previewable_suffixes]
            where_clauses.append("(" + " AND ".join(not_like_clauses) + ")")
            params.extend([f"%{suffix}" for suffix in previewable_suffixes])
    if cataloged_since_utc is not None:
        where_clauses.append("ma.last_cataloged_at_utc >= %s")
        params.append(cataloged_since_utc)
    if cataloged_before_utc is not None:
        where_clauses.append("ma.last_cataloged_at_utc <= %s")
        params.append(cataloged_before_utc)
    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM api_media_assets ma
                LEFT JOIN api_media_asset_extractions me
                    ON me.relative_path = ma.relative_path
                LEFT JOIN api_media_asset_previews mp
                    ON mp.relative_path = ma.relative_path
                {where_sql};
                """,
                tuple(params),
            )
            count_row = cur.fetchone()
            total = int(count_row[0]) if count_row is not None else 0
            cur.execute(
                f"""
                SELECT
                    ma.relative_path,
                    ma.sha256_hex,
                    ma.size_bytes,
                    ma.origin_kind,
                    ma.last_observed_origin_kind,
                    ma.provenance_job_name,
                    ma.provenance_original_filename,
                    ma.first_cataloged_at_utc,
                    ma.last_cataloged_at_utc,
                    ma.is_favorite,
                    ma.is_archived,
                    COALESCE(me.extraction_status, 'pending') AS extraction_status,
                    me.last_attempted_at_utc,
                    me.last_succeeded_at_utc,
                    me.last_failed_at_utc,
                    me.failure_detail,
                    COALESCE(mp.preview_status, 'pending') AS preview_status,
                    mp.preview_relative_path,
                    mp.last_attempted_at_utc,
                    mp.last_succeeded_at_utc,
                    mp.last_failed_at_utc,
                    mp.failure_detail,
                    me.capture_timestamp_utc,
                    me.camera_make,
                    me.camera_model,
                    me.image_width,
                    me.image_height,
                    me.orientation,
                    me.lens_model,
                    me.exposure_time_s,
                    me.f_number,
                    me.iso_speed,
                    me.focal_length_mm,
                    me.focal_length_35mm_mm
                FROM api_media_assets ma
                LEFT JOIN api_media_asset_extractions me
                    ON me.relative_path = ma.relative_path
                LEFT JOIN api_media_asset_previews mp
                    ON mp.relative_path = ma.relative_path
                {where_sql}
                ORDER BY ma.last_cataloged_at_utc DESC, ma.relative_path ASC
                LIMIT %s
                OFFSET %s;
                """,
                tuple([*params, limit, offset]),
            )
            rows = cur.fetchall()
            records = [
                MediaAssetRecord(
                    relative_path=str(row[0]),
                    sha256_hex=str(row[1]),
                    size_bytes=int(row[2]),
                    origin_kind=str(row[3]),
                    last_observed_origin_kind=str(row[4]),
                    provenance_job_name=str(row[5]) if row[5] is not None else None,
                    provenance_original_filename=str(row[6]) if row[6] is not None else None,
                    first_cataloged_at_utc=str(row[7]),
                    last_cataloged_at_utc=str(row[8]),
                    is_favorite=bool(row[9]),
                    is_archived=bool(row[10]),
                    extraction_status=str(row[11]),
                    extraction_last_attempted_at_utc=str(row[12]) if row[12] is not None else None,
                    extraction_last_succeeded_at_utc=str(row[13]) if row[13] is not None else None,
                    extraction_last_failed_at_utc=str(row[14]) if row[14] is not None else None,
                    extraction_failure_detail=str(row[15]) if row[15] is not None else None,
                    preview_status=str(row[16]),
                    preview_relative_path=str(row[17]) if row[17] is not None else None,
                    preview_last_attempted_at_utc=str(row[18]) if row[18] is not None else None,
                    preview_last_succeeded_at_utc=str(row[19]) if row[19] is not None else None,
                    preview_last_failed_at_utc=str(row[20]) if row[20] is not None else None,
                    preview_failure_detail=str(row[21]) if row[21] is not None else None,
                    capture_timestamp_utc=str(row[22]) if row[22] is not None else None,
                    camera_make=str(row[23]) if row[23] is not None else None,
                    camera_model=str(row[24]) if row[24] is not None else None,
                    image_width=int(row[25]) if row[25] is not None else None,
                    image_height=int(row[26]) if row[26] is not None else None,
                    orientation=int(row[27]) if row[27] is not None else None,
                    lens_model=str(row[28]) if row[28] is not None else None,
                    exposure_time_s=float(row[29]) if row[29] is not None else None,
                    f_number=float(row[30]) if row[30] is not None else None,
                    iso_speed=int(row[31]) if row[31] is not None else None,
                    focal_length_mm=float(row[32]) if row[32] is not None else None,
                    focal_length_35mm_mm=int(row[33]) if row[33] is not None else None,
                )
                for row in rows
            ]
            return total, records

def list_media_asset_folders(self) -> list[tuple[str, int, int, int]]:
    # Aggregate folder rows from api_media_assets. We compute direct and
    # total counts per ancestor folder by expanding each asset's path into
    # all of its ancestor folder prefixes (depth 1..N-1), then summing.
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH path_parts AS (
                    SELECT
                        relative_path,
                        string_to_array(relative_path, '/') AS parts
                    FROM api_media_assets
                ),
                ancestors AS (
                    SELECT
                        relative_path,
                        array_length(parts, 1) AS num_parts,
                        d AS depth,
                        array_to_string(parts[1:d], '/') AS folder_path
                    FROM path_parts,
                         generate_series(1, COALESCE(array_length(parts, 1), 1) - 1) AS d
                    WHERE array_length(parts, 1) > 1
                )
                SELECT
                    folder_path,
                    depth,
                    SUM(CASE WHEN depth = num_parts - 1 THEN 1 ELSE 0 END)::bigint AS direct_count,
                    COUNT(*)::bigint AS total_count
                FROM ancestors
                GROUP BY folder_path, depth
                ORDER BY folder_path ASC;
                """
            )
            rows = cur.fetchall() or []
            return [
                (str(row[0]), int(row[1]), int(row[2]), int(row[3]))
                for row in rows
            ]

def get_media_asset_by_path(self, relative_path: str) -> MediaAssetRecord | None:
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    ma.relative_path,
                    ma.sha256_hex,
                    ma.size_bytes,
                    ma.origin_kind,
                    ma.last_observed_origin_kind,
                    ma.provenance_job_name,
                    ma.provenance_original_filename,
                    ma.first_cataloged_at_utc,
                    ma.last_cataloged_at_utc,
                    ma.is_favorite,
                    ma.is_archived,
                    COALESCE(me.extraction_status, 'pending') AS extraction_status,
                    me.last_attempted_at_utc,
                    me.last_succeeded_at_utc,
                    me.last_failed_at_utc,
                    me.failure_detail,
                    COALESCE(mp.preview_status, 'pending') AS preview_status,
                    mp.preview_relative_path,
                    mp.last_attempted_at_utc,
                    mp.last_succeeded_at_utc,
                    mp.last_failed_at_utc,
                    mp.failure_detail,
                    me.capture_timestamp_utc,
                    me.camera_make,
                    me.camera_model,
                    me.image_width,
                    me.image_height,
                    me.orientation,
                    me.lens_model,
                    me.exposure_time_s,
                    me.f_number,
                    me.iso_speed,
                    me.focal_length_mm,
                    me.focal_length_35mm_mm
                FROM api_media_assets ma
                LEFT JOIN api_media_asset_extractions me
                    ON me.relative_path = ma.relative_path
                LEFT JOIN api_media_asset_previews mp
                    ON mp.relative_path = ma.relative_path
                WHERE ma.relative_path = %s
                LIMIT 1;
                """,
                (relative_path,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            return MediaAssetRecord(
                relative_path=str(row[0]),
                sha256_hex=str(row[1]),
                size_bytes=int(row[2]),
                origin_kind=str(row[3]),
                last_observed_origin_kind=str(row[4]),
                provenance_job_name=str(row[5]) if row[5] is not None else None,
                provenance_original_filename=str(row[6]) if row[6] is not None else None,
                first_cataloged_at_utc=str(row[7]),
                last_cataloged_at_utc=str(row[8]),
                is_favorite=bool(row[9]),
                is_archived=bool(row[10]),
                extraction_status=str(row[11]),
                extraction_last_attempted_at_utc=str(row[12]) if row[12] is not None else None,
                extraction_last_succeeded_at_utc=str(row[13]) if row[13] is not None else None,
                extraction_last_failed_at_utc=str(row[14]) if row[14] is not None else None,
                extraction_failure_detail=str(row[15]) if row[15] is not None else None,
                preview_status=str(row[16]),
                preview_relative_path=str(row[17]) if row[17] is not None else None,
                preview_last_attempted_at_utc=str(row[18]) if row[18] is not None else None,
                preview_last_succeeded_at_utc=str(row[19]) if row[19] is not None else None,
                preview_last_failed_at_utc=str(row[20]) if row[20] is not None else None,
                preview_failure_detail=str(row[21]) if row[21] is not None else None,
                capture_timestamp_utc=str(row[22]) if row[22] is not None else None,
                camera_make=str(row[23]) if row[23] is not None else None,
                camera_model=str(row[24]) if row[24] is not None else None,
                image_width=int(row[25]) if row[25] is not None else None,
                image_height=int(row[26]) if row[26] is not None else None,
                orientation=int(row[27]) if row[27] is not None else None,
                lens_model=str(row[28]) if row[28] is not None else None,
                exposure_time_s=float(row[29]) if row[29] is not None else None,
                f_number=float(row[30]) if row[30] is not None else None,
                iso_speed=int(row[31]) if row[31] is not None else None,
                focal_length_mm=float(row[32]) if row[32] is not None else None,
                focal_length_35mm_mm=int(row[33]) if row[33] is not None else None,
            )

def set_media_asset_favorite(
    self, *, relative_path: str, is_favorite: bool, updated_at_utc: str
) -> MediaAssetRecord | None:
    del updated_at_utc
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE api_media_assets
                SET is_favorite = %s
                WHERE relative_path = %s;
                """,
                (is_favorite, relative_path),
            )
            if cur.rowcount <= 0:
                conn.commit()
                return None
        conn.commit()
    return self.get_media_asset_by_path(relative_path)

def set_media_asset_archived(
    self, *, relative_path: str, is_archived: bool, updated_at_utc: str
) -> MediaAssetRecord | None:
    del updated_at_utc
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE api_media_assets
                SET is_archived = %s
                WHERE relative_path = %s;
                """,
                (is_archived, relative_path),
            )
            if cur.rowcount <= 0:
                conn.commit()
                return None
        conn.commit()
    return self.get_media_asset_by_path(relative_path)

# -------- Phase 3.B: reject queue -----------------------------------
def add_catalog_reject(
    self,
    *,
    relative_path: str,
    marked_at_utc: str,
    marked_reason: str | None = None,
) -> RejectedAssetRecord | None:
    with self._connect() as conn:
        with conn.cursor() as cur:
            # Idempotent upsert: keep the first-marked timestamp when the
            # row already exists so "marked since" reflects the original
            # reviewer action. SHA is read inline from api_media_assets.
            cur.execute(
                """
                INSERT INTO api_catalog_reject_queue (
                    relative_path, sha256_hex, marked_at_utc, marked_reason
                )
                SELECT ma.relative_path, ma.sha256_hex, %s, %s
                FROM api_media_assets ma
                WHERE ma.relative_path = %s
                ON CONFLICT (relative_path) DO UPDATE SET
                    marked_reason = COALESCE(
                        EXCLUDED.marked_reason,
                        api_catalog_reject_queue.marked_reason
                    );
                """,
                (marked_at_utc, marked_reason, relative_path),
            )
            if cur.rowcount <= 0:
                conn.commit()
                return None
            cur.execute(
                """
                SELECT relative_path, sha256_hex, marked_at_utc, marked_reason
                FROM api_catalog_reject_queue
                WHERE relative_path = %s;
                """,
                (relative_path,),
            )
            row = cur.fetchone()
        conn.commit()
    if row is None:
        return None
    return RejectedAssetRecord(
        relative_path=str(row[0]),
        sha256_hex=str(row[1]),
        marked_at_utc=str(row[2]),
        marked_reason=str(row[3]) if row[3] is not None else None,
    )

def remove_catalog_reject(self, relative_path: str) -> bool:
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM api_catalog_reject_queue WHERE relative_path = %s;
                """,
                (relative_path,),
            )
            removed = cur.rowcount > 0
        conn.commit()
    return removed

def is_catalog_reject(self, relative_path: str) -> bool:
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM api_catalog_reject_queue WHERE relative_path = %s LIMIT 1;
                """,
                (relative_path,),
            )
            return cur.fetchone() is not None

def count_catalog_rejects(self) -> int:
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM api_catalog_reject_queue;")
            row = cur.fetchone()
            return int(row[0]) if row else 0

def list_catalog_rejects(
    self, *, limit: int, offset: int
) -> tuple[int, list[RejectedAssetRecord]]:
    if limit <= 0:
        total = self.count_catalog_rejects()
        return total, []
    if offset < 0:
        offset = 0
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM api_catalog_reject_queue;")
            row = cur.fetchone()
            total = int(row[0]) if row else 0
            cur.execute(
                """
                SELECT relative_path, sha256_hex, marked_at_utc, marked_reason
                FROM api_catalog_reject_queue
                ORDER BY marked_at_utc ASC, relative_path ASC
                LIMIT %s OFFSET %s;
                """,
                (limit, offset),
            )
            rows = cur.fetchall()
    items = [
        RejectedAssetRecord(
            relative_path=str(row[0]),
            sha256_hex=str(row[1]),
            marked_at_utc=str(row[2]),
            marked_reason=str(row[3]) if row[3] is not None else None,
        )
        for row in rows
    ]
    return total, items

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
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO api_catalog_tombstones (
                    relative_path, sha256_hex, trashed_at_utc, marked_reason,
                    trash_relative_path, original_size_bytes
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (relative_path) DO UPDATE
                SET sha256_hex = EXCLUDED.sha256_hex,
                    trashed_at_utc = EXCLUDED.trashed_at_utc,
                    marked_reason = EXCLUDED.marked_reason,
                    trash_relative_path = EXCLUDED.trash_relative_path,
                    original_size_bytes = EXCLUDED.original_size_bytes;
                """,
                (
                    relative_path,
                    sha256_hex,
                    trashed_at_utc,
                    marked_reason,
                    trash_relative_path,
                    original_size_bytes,
                ),
            )
        conn.commit()
    return TombstoneRecord(
        relative_path=relative_path,
        sha256_hex=sha256_hex,
        trashed_at_utc=trashed_at_utc,
        marked_reason=marked_reason,
        trash_relative_path=trash_relative_path,
        original_size_bytes=original_size_bytes,
    )

def is_sha_tombstoned(self, sha256_hex: str) -> bool:
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM api_catalog_tombstones WHERE sha256_hex = %s LIMIT 1;
                """,
                (sha256_hex,),
            )
            return cur.fetchone() is not None

def list_sha_tombstones(self, shas: list[str]) -> list[TombstoneRecord]:
    if not shas:
        return []
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT relative_path, sha256_hex, trashed_at_utc, marked_reason,
                       trash_relative_path, original_size_bytes
                FROM api_catalog_tombstones
                WHERE sha256_hex = ANY(%s);
                """,
                (shas,),
            )
            rows = cur.fetchall()
    return [
        TombstoneRecord(
            relative_path=str(row[0]),
            sha256_hex=str(row[1]),
            trashed_at_utc=str(row[2]),
            marked_reason=str(row[3]) if row[3] is not None else None,
            trash_relative_path=str(row[4]),
            original_size_bytes=int(row[5]),
        )
        for row in rows
    ]

def remove_tombstone(self, relative_path: str) -> bool:
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM api_catalog_tombstones WHERE relative_path = %s;
                """,
                (relative_path,),
            )
            removed = cur.rowcount > 0
        conn.commit()
    return removed

def get_tombstone_by_path(self, relative_path: str) -> TombstoneRecord | None:
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT relative_path, sha256_hex, trashed_at_utc, marked_reason,
                       trash_relative_path, original_size_bytes
                FROM api_catalog_tombstones
                WHERE relative_path = %s;
                """,
                (relative_path,),
            )
            row = cur.fetchone()
    if row is None:
        return None
    return TombstoneRecord(
        relative_path=str(row[0]),
        sha256_hex=str(row[1]),
        trashed_at_utc=str(row[2]),
        marked_reason=str(row[3]) if row[3] is not None else None,
        trash_relative_path=str(row[4]),
        original_size_bytes=int(row[5]),
    )

def list_tombstones(
    self,
    *,
    limit: int,
    offset: int,
    older_than_days: int | None = None,
) -> tuple[int, list[TombstoneRecord]]:
    where_clauses: list[str] = []
    params: list[object] = []
    if older_than_days is not None:
        cutoff = (datetime.now(UTC) - timedelta(days=older_than_days)).isoformat()
        where_clauses.append("trashed_at_utc <= %s")
        params.append(cutoff)
    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM api_catalog_tombstones {where_sql};",
                params,
            )
            row = cur.fetchone()
            total = int(row[0]) if row else 0
            cur.execute(
                f"""
                SELECT relative_path, sha256_hex, trashed_at_utc, marked_reason,
                       trash_relative_path, original_size_bytes
                FROM api_catalog_tombstones
                {where_sql}
                ORDER BY trashed_at_utc ASC
                LIMIT %s OFFSET %s;
                """,
                [*params, limit, offset],
            )
            rows = cur.fetchall()
    return total, [
        TombstoneRecord(
            relative_path=str(r[0]),
            sha256_hex=str(r[1]),
            trashed_at_utc=str(r[2]),
            marked_reason=str(r[3]) if r[3] is not None else None,
            trash_relative_path=str(r[4]),
            original_size_bytes=int(r[5]),
        )
        for r in rows
    ]

def purge_tombstones(
    self,
    *,
    older_than_days: int,
    max_batch: int,
) -> list[TombstoneRecord]:
    cutoff = (datetime.now(UTC) - timedelta(days=older_than_days)).isoformat()
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT relative_path, sha256_hex, trashed_at_utc, marked_reason,
                       trash_relative_path, original_size_bytes
                FROM api_catalog_tombstones
                WHERE trashed_at_utc <= %s
                ORDER BY trashed_at_utc ASC
                FOR UPDATE SKIP LOCKED
                LIMIT %s;
                """,
                (cutoff, max_batch),
            )
            rows = cur.fetchall()
            if rows:
                relative_paths = [str(r[0]) for r in rows]
                cur.execute(
                    "DELETE FROM api_catalog_tombstones WHERE relative_path = ANY(%s);",
                    (relative_paths,),
                )
        conn.commit()
    return [
        TombstoneRecord(
            relative_path=str(r[0]),
            sha256_hex=str(r[1]),
            trashed_at_utc=str(r[2]),
            marked_reason=str(r[3]) if r[3] is not None else None,
            trash_relative_path=str(r[4]),
            original_size_bytes=int(r[5]),
        )
        for r in rows
    ]

def delete_media_asset(self, relative_path: str) -> bool:
    """Delete the api_media_assets row (ON DELETE CASCADE removes extraction,
    preview, and reject-queue rows automatically).

    Returns True if a row was deleted, False if the path was not found.
    Added in Phase 3.C to support the execute-delete path.
    """
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM api_media_assets WHERE relative_path = %s;
                """,
                (relative_path,),
            )
            deleted = cur.rowcount > 0
        conn.commit()
    return deleted

def _row_to_media_asset_record(self, row: tuple[object, ...]) -> MediaAssetRecord:
    return MediaAssetRecord(
        relative_path=str(row[0]),
        sha256_hex=str(row[1]),
        size_bytes=int(row[2]),
        origin_kind=str(row[3]),
        last_observed_origin_kind=str(row[4]),
        provenance_job_name=str(row[5]) if row[5] is not None else None,
        provenance_original_filename=str(row[6]) if row[6] is not None else None,
        first_cataloged_at_utc=str(row[7]),
        last_cataloged_at_utc=str(row[8]),
        is_favorite=bool(row[9]),
        is_archived=bool(row[10]),
        extraction_status=str(row[11]),
        extraction_last_attempted_at_utc=str(row[12]) if row[12] is not None else None,
        extraction_last_succeeded_at_utc=str(row[13]) if row[13] is not None else None,
        extraction_last_failed_at_utc=str(row[14]) if row[14] is not None else None,
        extraction_failure_detail=str(row[15]) if row[15] is not None else None,
        preview_status=str(row[16]),
        preview_relative_path=str(row[17]) if row[17] is not None else None,
        preview_last_attempted_at_utc=str(row[18]) if row[18] is not None else None,
        preview_last_succeeded_at_utc=str(row[19]) if row[19] is not None else None,
        preview_last_failed_at_utc=str(row[20]) if row[20] is not None else None,
        preview_failure_detail=str(row[21]) if row[21] is not None else None,
        capture_timestamp_utc=str(row[22]) if row[22] is not None else None,
        camera_make=str(row[23]) if row[23] is not None else None,
        camera_model=str(row[24]) if row[24] is not None else None,
        image_width=int(row[25]) if row[25] is not None else None,
        image_height=int(row[26]) if row[26] is not None else None,
        orientation=int(row[27]) if row[27] is not None else None,
        lens_model=str(row[28]) if row[28] is not None else None,
        exposure_time_s=float(row[29]) if len(row) > 29 and row[29] is not None else None,
        f_number=float(row[30]) if len(row) > 30 and row[30] is not None else None,
        iso_speed=int(row[31]) if len(row) > 31 and row[31] is not None else None,
        focal_length_mm=float(row[32]) if len(row) > 32 and row[32] is not None else None,
        focal_length_35mm_mm=int(row[33]) if len(row) > 33 and row[33] is not None else None,
    )

def _list_media_assets_for_backfill(
    self,
    *,
    status_column_sql: str,
    statuses: list[str],
    limit: int,
    origin_kind: str | None,
    media_type: str | None,
    preview_capability: str | None,
    cataloged_since_utc: str | None,
    cataloged_before_utc: str | None,
) -> list[MediaAssetRecord]:
    if limit <= 0 or not statuses:
        return []

    where_clauses = [f"{status_column_sql} = ANY(%s)"]
    params: list[object] = [statuses]
    if origin_kind is not None:
        where_clauses.append("ma.origin_kind = %s")
        params.append(origin_kind)
    if media_type is not None:
        media_type_suffixes = _MEDIA_TYPE_SUFFIXES.get(media_type)
        if media_type_suffixes is not None:
            like_clauses = ["LOWER(ma.relative_path) LIKE %s" for _ in media_type_suffixes]
            where_clauses.append("(" + " OR ".join(like_clauses) + ")")
            params.extend([f"%{suffix}" for suffix in media_type_suffixes])
        else:
            all_suffixes = [suffix for values in _MEDIA_TYPE_SUFFIXES.values() for suffix in values]
            not_like_clauses = ["LOWER(ma.relative_path) NOT LIKE %s" for _ in all_suffixes]
            where_clauses.append("(" + " AND ".join(not_like_clauses) + ")")
            params.extend([f"%{suffix}" for suffix in all_suffixes])
    if preview_capability is not None:
        previewable_suffixes = tuple(sorted(_PREVIEWABLE_SUFFIXES))
        if preview_capability == "previewable":
            like_clauses = ["LOWER(ma.relative_path) LIKE %s" for _ in previewable_suffixes]
            where_clauses.append("(" + " OR ".join(like_clauses) + ")")
            params.extend([f"%{suffix}" for suffix in previewable_suffixes])
        else:
            not_like_clauses = ["LOWER(ma.relative_path) NOT LIKE %s" for _ in previewable_suffixes]
            where_clauses.append("(" + " AND ".join(not_like_clauses) + ")")
            params.extend([f"%{suffix}" for suffix in previewable_suffixes])
    if cataloged_since_utc is not None:
        where_clauses.append("ma.last_cataloged_at_utc >= %s")
        params.append(cataloged_since_utc)
    if cataloged_before_utc is not None:
        where_clauses.append("ma.last_cataloged_at_utc <= %s")
        params.append(cataloged_before_utc)
    where_sql = "WHERE " + " AND ".join(where_clauses)

    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    ma.relative_path,
                    ma.sha256_hex,
                    ma.size_bytes,
                    ma.origin_kind,
                    ma.last_observed_origin_kind,
                    ma.provenance_job_name,
                    ma.provenance_original_filename,
                    ma.first_cataloged_at_utc,
                    ma.last_cataloged_at_utc,
                    ma.is_favorite,
                    ma.is_archived,
                    COALESCE(me.extraction_status, 'pending') AS extraction_status,
                    me.last_attempted_at_utc,
                    me.last_succeeded_at_utc,
                    me.last_failed_at_utc,
                    me.failure_detail,
                    COALESCE(mp.preview_status, 'pending') AS preview_status,
                    mp.preview_relative_path,
                    mp.last_attempted_at_utc,
                    mp.last_succeeded_at_utc,
                    mp.last_failed_at_utc,
                    mp.failure_detail,
                    me.capture_timestamp_utc,
                    me.camera_make,
                    me.camera_model,
                    me.image_width,
                    me.image_height,
                    me.orientation,
                    me.lens_model,
                    me.exposure_time_s,
                    me.f_number,
                    me.iso_speed,
                    me.focal_length_mm,
                    me.focal_length_35mm_mm
                FROM api_media_assets ma
                LEFT JOIN api_media_asset_extractions me
                    ON me.relative_path = ma.relative_path
                LEFT JOIN api_media_asset_previews mp
                    ON mp.relative_path = ma.relative_path
                {where_sql}
                ORDER BY ma.last_cataloged_at_utc DESC, ma.relative_path ASC
                LIMIT %s;
                """,
                tuple([*params, limit]),
            )
            rows = cur.fetchall()
            return [self._row_to_media_asset_record(tuple(row)) for row in rows]

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
    return self._list_media_assets_for_backfill(
        status_column_sql="COALESCE(me.extraction_status, 'pending')",
        statuses=extraction_statuses,
        limit=limit,
        origin_kind=origin_kind,
        media_type=media_type,
        preview_capability=preview_capability,
        cataloged_since_utc=cataloged_since_utc,
        cataloged_before_utc=cataloged_before_utc,
    )

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
    return self._list_media_assets_for_backfill(
        status_column_sql="COALESCE(mp.preview_status, 'pending')",
        statuses=preview_statuses,
        limit=limit,
        origin_kind=origin_kind,
        media_type=media_type,
        preview_capability=preview_capability,
        cataloged_since_utc=cataloged_since_utc,
        cataloged_before_utc=cataloged_before_utc,
    )

def ensure_media_asset_extraction_row(self, *, relative_path: str, recorded_at_utc: str) -> None:
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO api_media_asset_extractions (
                    relative_path,
                    extraction_status,
                    updated_at_utc
                )
                VALUES (%s, 'pending', %s)
                ON CONFLICT (relative_path) DO NOTHING;
                """,
                (relative_path, recorded_at_utc),
            )
        conn.commit()

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
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO api_media_asset_extractions (
                    relative_path,
                    extraction_status,
                    last_attempted_at_utc,
                    last_succeeded_at_utc,
                    last_failed_at_utc,
                    failure_detail,
                    capture_timestamp_utc,
                    camera_make,
                    camera_model,
                    image_width,
                    image_height,
                    orientation,
                    lens_model,
                    exposure_time_s,
                    f_number,
                    iso_speed,
                    focal_length_mm,
                    focal_length_35mm_mm,
                    updated_at_utc
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s
                )
                ON CONFLICT (relative_path) DO UPDATE
                SET extraction_status = EXCLUDED.extraction_status,
                    last_attempted_at_utc = EXCLUDED.last_attempted_at_utc,
                    last_succeeded_at_utc = EXCLUDED.last_succeeded_at_utc,
                    last_failed_at_utc = EXCLUDED.last_failed_at_utc,
                    failure_detail = EXCLUDED.failure_detail,
                    capture_timestamp_utc = EXCLUDED.capture_timestamp_utc,
                    camera_make = EXCLUDED.camera_make,
                    camera_model = EXCLUDED.camera_model,
                    image_width = EXCLUDED.image_width,
                    image_height = EXCLUDED.image_height,
                    orientation = EXCLUDED.orientation,
                    lens_model = EXCLUDED.lens_model,
                    exposure_time_s = EXCLUDED.exposure_time_s,
                    f_number = EXCLUDED.f_number,
                    iso_speed = EXCLUDED.iso_speed,
                    focal_length_mm = EXCLUDED.focal_length_mm,
                    focal_length_35mm_mm = EXCLUDED.focal_length_35mm_mm,
                    updated_at_utc = EXCLUDED.updated_at_utc;
                """,
                (
                    relative_path,
                    extraction_status,
                    attempted_at_utc,
                    succeeded_at_utc,
                    failed_at_utc,
                    failure_detail,
                    capture_timestamp_utc,
                    camera_make,
                    camera_model,
                    image_width,
                    image_height,
                    orientation,
                    lens_model,
                    exposure_time_s,
                    f_number,
                    iso_speed,
                    focal_length_mm,
                    focal_length_35mm_mm,
                    recorded_at_utc,
                ),
            )
        conn.commit()

def ensure_media_asset_preview_row(self, *, relative_path: str, recorded_at_utc: str) -> None:
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO api_media_asset_previews (
                    relative_path,
                    preview_status,
                    updated_at_utc
                )
                VALUES (%s, 'pending', %s)
                ON CONFLICT (relative_path) DO NOTHING;
                """,
                (relative_path, recorded_at_utc),
            )
        conn.commit()

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
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO api_media_asset_previews (
                    relative_path,
                    preview_status,
                    preview_relative_path,
                    last_attempted_at_utc,
                    last_succeeded_at_utc,
                    last_failed_at_utc,
                    failure_detail,
                    updated_at_utc
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (relative_path) DO UPDATE
                SET preview_status = EXCLUDED.preview_status,
                    preview_relative_path = EXCLUDED.preview_relative_path,
                    last_attempted_at_utc = EXCLUDED.last_attempted_at_utc,
                    last_succeeded_at_utc = EXCLUDED.last_succeeded_at_utc,
                    last_failed_at_utc = EXCLUDED.last_failed_at_utc,
                    failure_detail = EXCLUDED.failure_detail,
                    updated_at_utc = EXCLUDED.updated_at_utc;
                """,
                (
                    relative_path,
                    preview_status,
                    preview_relative_path,
                    attempted_at_utc,
                    succeeded_at_utc,
                    failed_at_utc,
                    failure_detail,
                    recorded_at_utc,
                ),
            )
        conn.commit()

def list_duplicate_sha_groups(
    self, *, limit: int, offset: int
) -> tuple[int, list[DuplicateShaGroup]]:
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH duplicate_groups AS (
                    SELECT
                        sha256_hex,
                        COUNT(*) AS file_count,
                        MIN(first_seen_at_utc) AS first_seen_at_utc,
                        MAX(last_seen_at_utc) AS last_seen_at_utc,
                        ARRAY_AGG(relative_path ORDER BY relative_path ASC) AS relative_paths
                    FROM api_stored_files
                    GROUP BY sha256_hex
                    HAVING COUNT(*) > 1
                )
                SELECT COUNT(*) FROM duplicate_groups;
                """
            )
            count_row = cur.fetchone()
            total = int(count_row[0]) if count_row is not None else 0
            cur.execute(
                """
                SELECT
                    sha256_hex,
                    file_count,
                    first_seen_at_utc,
                    last_seen_at_utc,
                    relative_paths
                FROM (
                    SELECT
                        sha256_hex,
                        COUNT(*) AS file_count,
                        MIN(first_seen_at_utc) AS first_seen_at_utc,
                        MAX(last_seen_at_utc) AS last_seen_at_utc,
                        ARRAY_AGG(relative_path ORDER BY relative_path ASC) AS relative_paths
                    FROM api_stored_files
                    GROUP BY sha256_hex
                    HAVING COUNT(*) > 1
                ) duplicate_groups
                ORDER BY file_count DESC, last_seen_at_utc DESC, sha256_hex ASC
                LIMIT %s
                OFFSET %s;
                """,
                (limit, offset),
            )
            rows = cur.fetchall()
            groups = [
                DuplicateShaGroup(
                    sha256_hex=str(row[0]),
                    file_count=int(row[1]),
                    first_seen_at_utc=str(row[2]),
                    last_seen_at_utc=str(row[3]),
                    relative_paths=tuple(str(path) for path in row[4]),
                )
                for row in rows
            ]
            return total, groups

def record_path_conflict(
    self,
    *,
    relative_path: str,
    previous_sha256_hex: str,
    current_sha256_hex: str,
    detected_at_utc: str,
) -> None:
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO api_storage_path_conflicts (
                    relative_path, previous_sha256_hex, current_sha256_hex, detected_at_utc
                )
                VALUES (%s, %s, %s, %s);
                """,
                (relative_path, previous_sha256_hex, current_sha256_hex, detected_at_utc),
            )
        conn.commit()

def list_path_conflicts(self, *, limit: int, offset: int) -> tuple[int, list[PathConflictRecord]]:
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM api_storage_path_conflicts;")
            count_row = cur.fetchone()
            total = int(count_row[0]) if count_row is not None else 0
            cur.execute(
                """
                SELECT relative_path, previous_sha256_hex, current_sha256_hex, detected_at_utc
                FROM api_storage_path_conflicts
                ORDER BY detected_at_utc DESC, relative_path ASC
                LIMIT %s
                OFFSET %s;
                """,
                (limit, offset),
            )
            rows = cur.fetchall()
            records = [
                PathConflictRecord(
                    relative_path=str(row[0]),
                    previous_sha256_hex=str(row[1]),
                    current_sha256_hex=str(row[2]),
                    detected_at_utc=str(row[3]),
                )
                for row in rows
            ]
            return total, records

def record_storage_index_run(self, record: StorageIndexRunRecord) -> None:
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO api_storage_index_runs (
                    singleton_key, scanned_files, indexed_files, new_sha_entries,
                    existing_sha_matches, path_conflicts, errors, completed_at_utc
                )
                VALUES (TRUE, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (singleton_key) DO UPDATE
                SET scanned_files = EXCLUDED.scanned_files,
                    indexed_files = EXCLUDED.indexed_files,
                    new_sha_entries = EXCLUDED.new_sha_entries,
                    existing_sha_matches = EXCLUDED.existing_sha_matches,
                    path_conflicts = EXCLUDED.path_conflicts,
                    errors = EXCLUDED.errors,
                    completed_at_utc = EXCLUDED.completed_at_utc;
                """,
                (
                    record.scanned_files,
                    record.indexed_files,
                    record.new_sha_entries,
                    record.existing_sha_matches,
                    record.path_conflicts,
                    record.errors,
                    record.completed_at_utc,
                ),
            )
        conn.commit()

def get_latest_storage_index_run(self) -> StorageIndexRunRecord | None:
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    scanned_files,
                    indexed_files,
                    new_sha_entries,
                    existing_sha_matches,
                    path_conflicts,
                    errors,
                    completed_at_utc
                FROM api_storage_index_runs
                WHERE singleton_key = TRUE
                LIMIT 1;
                """
            )
            row = cur.fetchone()
            if row is None:
                return None
            return StorageIndexRunRecord(
                scanned_files=int(row[0]),
                indexed_files=int(row[1]),
                new_sha_entries=int(row[2]),
                existing_sha_matches=int(row[3]),
                path_conflicts=int(row[4]),
                errors=int(row[5]),
                completed_at_utc=str(row[6]),
            )

def record_catalog_backfill_run(self, record: CatalogBackfillRunRecord) -> None:
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO api_catalog_backfill_runs (
                    backfill_kind,
                    requested_statuses,
                    selected_count,
                    processed_count,
                    succeeded_count,
                    failed_count,
                    remaining_pending_count,
                    remaining_failed_count,
                    filter_origin_kind,
                    filter_media_type,
                    filter_preview_capability,
                    filter_cataloged_since_utc,
                    filter_cataloged_before_utc,
                    limit_count,
                    completed_at_utc
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (backfill_kind) DO UPDATE
                SET requested_statuses = EXCLUDED.requested_statuses,
                    selected_count = EXCLUDED.selected_count,
                    processed_count = EXCLUDED.processed_count,
                    succeeded_count = EXCLUDED.succeeded_count,
                    failed_count = EXCLUDED.failed_count,
                    remaining_pending_count = EXCLUDED.remaining_pending_count,
                    remaining_failed_count = EXCLUDED.remaining_failed_count,
                    filter_origin_kind = EXCLUDED.filter_origin_kind,
                    filter_media_type = EXCLUDED.filter_media_type,
                    filter_preview_capability = EXCLUDED.filter_preview_capability,
                    filter_cataloged_since_utc = EXCLUDED.filter_cataloged_since_utc,
                    filter_cataloged_before_utc = EXCLUDED.filter_cataloged_before_utc,
                    limit_count = EXCLUDED.limit_count,
                    completed_at_utc = EXCLUDED.completed_at_utc;
                """,
                (
                    record.backfill_kind,
                    list(record.requested_statuses),
                    record.selected_count,
                    record.processed_count,
                    record.succeeded_count,
                    record.failed_count,
                    record.remaining_pending_count,
                    record.remaining_failed_count,
                    record.filter_origin_kind,
                    record.filter_media_type,
                    record.filter_preview_capability,
                    record.filter_cataloged_since_utc,
                    record.filter_cataloged_before_utc,
                    record.limit_count,
                    record.completed_at_utc,
                ),
            )
        conn.commit()

def get_latest_catalog_backfill_run(self, backfill_kind: str) -> CatalogBackfillRunRecord | None:
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    backfill_kind,
                    requested_statuses,
                    selected_count,
                    processed_count,
                    succeeded_count,
                    failed_count,
                    remaining_pending_count,
                    remaining_failed_count,
                    filter_origin_kind,
                    filter_media_type,
                    filter_preview_capability,
                    filter_cataloged_since_utc,
                    filter_cataloged_before_utc,
                    limit_count,
                    completed_at_utc
                FROM api_catalog_backfill_runs
                WHERE backfill_kind = %s
                LIMIT 1;
                """,
                (backfill_kind,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            raw_statuses = row[1]
            statuses: tuple[str, ...]
            if isinstance(raw_statuses, (list, tuple)):
                statuses = tuple(str(status) for status in raw_statuses)
            else:
                statuses = tuple()
            return CatalogBackfillRunRecord(
                backfill_kind=str(row[0]),
                requested_statuses=statuses,
                selected_count=int(row[2]),
                processed_count=int(row[3]),
                succeeded_count=int(row[4]),
                failed_count=int(row[5]),
                remaining_pending_count=int(row[6]),
                remaining_failed_count=int(row[7]),
                filter_origin_kind=str(row[8]) if row[8] is not None else None,
                filter_media_type=str(row[9]) if row[9] is not None else None,
                filter_preview_capability=str(row[10]) if row[10] is not None else None,
                filter_cataloged_since_utc=str(row[11]) if row[11] is not None else None,
                filter_cataloged_before_utc=str(row[12]) if row[12] is not None else None,
                limit_count=int(row[13]),
                completed_at_utc=str(row[14]),
            )

def summarize_storage(self) -> StorageSummary:
    with self._connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM api_known_sha256;")
            known_row = cur.fetchone()
            total_known_sha256 = int(known_row[0]) if known_row is not None else 0

            cur.execute(
                """
                SELECT
                    COUNT(*) AS total_stored_files,
                    COUNT(*) FILTER (WHERE source_kind = 'index_scan') AS indexed_files,
                    COUNT(*) FILTER (WHERE source_kind = 'upload_verify') AS uploaded_files,
                    COUNT(*) - COUNT(DISTINCT sha256_hex) AS duplicate_file_paths
                FROM api_stored_files;
                """
            )
            aggregate_row = cur.fetchone()
            total_stored_files = int(aggregate_row[0]) if aggregate_row is not None else 0
            indexed_files = int(aggregate_row[1]) if aggregate_row is not None else 0
            uploaded_files = int(aggregate_row[2]) if aggregate_row is not None else 0
            duplicate_file_paths = int(aggregate_row[3]) if aggregate_row is not None else 0

            cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (
                        WHERE source_kind = 'index_scan'
                        AND last_seen_at_utc >= %s
                    ) AS recent_indexed_files_24h,
                    COUNT(*) FILTER (
                        WHERE source_kind = 'upload_verify'
                        AND last_seen_at_utc >= %s
                    ) AS recent_uploaded_files_24h,
                    MAX(last_seen_at_utc) FILTER (
                        WHERE source_kind = 'index_scan'
                    ) AS last_indexed_at_utc,
                    MAX(last_seen_at_utc) FILTER (
                        WHERE source_kind = 'upload_verify'
                    ) AS last_uploaded_at_utc
                FROM api_stored_files;
                """,
                (
                    (datetime.now(UTC) - timedelta(hours=24)).isoformat(),
                    (datetime.now(UTC) - timedelta(hours=24)).isoformat(),
                ),
            )
            recent_row = cur.fetchone()
            recent_indexed_files_24h = (
                int(recent_row[0]) if recent_row and recent_row[0] is not None else 0
            )
            recent_uploaded_files_24h = (
                int(recent_row[1]) if recent_row and recent_row[1] is not None else 0
            )
            last_indexed_at_utc = (
                str(recent_row[2]) if recent_row and recent_row[2] is not None else None
            )
            last_uploaded_at_utc = (
                str(recent_row[3]) if recent_row and recent_row[3] is not None else None
            )

            return StorageSummary(
                total_known_sha256=total_known_sha256,
                total_stored_files=total_stored_files,
                indexed_files=indexed_files,
                uploaded_files=uploaded_files,
                duplicate_file_paths=duplicate_file_paths,
                recent_indexed_files_24h=recent_indexed_files_24h,
                recent_uploaded_files_24h=recent_uploaded_files_24h,
                last_indexed_at_utc=last_indexed_at_utc,
                last_uploaded_at_utc=last_uploaded_at_utc,
            )
