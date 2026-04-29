"""Admin route registration for photovault-api."""

import mimetypes
import secrets
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from photovault_api.state_store import CatalogBackfillRunRecord, StorageSummary, UploadStateStore

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
    AdminCatalogRejectQueueResponse,
    AdminCatalogRejectRequest,
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
    ClientEnrollmentStatus,
    ClientPresenceStatus,
    ClientWorkloadStatus,
    DuplicateShaGroupItem,
    DuplicateShaGroupListResponse,
    LatestIndexRunEnvelope,
    LatestIndexRunResponse,
    PathConflictItem,
    PathConflictListResponse,
)
from .storage_ops import (
    _list_clients_for_admin_view,
    _normalize_catalog_folder_prefix,
    _parse_boolean_filter,
    _presence_sort_rank,
    _to_admin_catalog_item,
    _to_admin_client_item,
    _to_backfill_run_summary,
    _validate_backfill_target_statuses,
    _validate_catalog_filter_selection,
    _workload_sort_rank,
)

_TRASH_RETENTION_DAYS = 14


class ExecuteRejectsRequest(BaseModel):
    relative_paths: list[str] | None = Field(default=None, max_length=10000)


class ExecuteRejectsResponse(BaseModel):
    executed: list[str]
    skipped: list[str]


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


def _require_safe_relative_path(relative_path: str) -> str:
    """Validate that ``relative_path`` is a catalog-safe forward-slash path."""

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


def register_admin_routes(app: FastAPI, helpers: Any) -> None:
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
        now_utc = helpers.datetime.now(helpers.UTC)
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
            approved_at_utc=helpers.datetime.now(helpers.UTC).isoformat(),
            auth_token=issued_token,
        )
        if approved is None:
            raise HTTPException(status_code=404, detail="client not found")
        return AdminClientActionResponse(
            item=_to_admin_client_item(
                approved,
                heartbeat=store.get_client_heartbeat(approved.client_id),
                now_utc=helpers.datetime.now(helpers.UTC),
            )
        )

    @app.post("/v1/admin/clients/{client_id}/revoke", response_model=AdminClientActionResponse)
    def admin_revoke_client(client_id: str) -> AdminClientActionResponse:
        store: UploadStateStore = app.state.upload_state_store
        revoked = store.revoke_client(
            client_id=client_id,
            revoked_at_utc=helpers.datetime.now(helpers.UTC).isoformat(),
        )
        if revoked is None:
            raise HTTPException(status_code=404, detail="client not found")
        return AdminClientActionResponse(
            item=_to_admin_client_item(
                revoked,
                heartbeat=store.get_client_heartbeat(revoked.client_id),
                now_utc=helpers.datetime.now(helpers.UTC),
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
        _, reject_rows = store.list_catalog_rejects(limit=10_000, offset=0)
        rejected_paths = frozenset(row.relative_path for row in reject_rows)
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

    @app.get("/v1/admin/catalog/folders", response_model=AdminCatalogFoldersResponse)
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
    def admin_catalog_asset(relative_path: str = Query(min_length=1)) -> AdminCatalogAssetResponse:
        store: UploadStateStore = app.state.upload_state_store
        record = store.get_media_asset_by_path(relative_path)
        if record is None:
            raise HTTPException(status_code=404, detail="catalog asset not found")
        is_rejected = store.is_catalog_reject(relative_path)
        return AdminCatalogAssetResponse(item=_to_admin_catalog_item(record, is_rejected=is_rejected))

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
            updated_at_utc=helpers.datetime.now(helpers.UTC).isoformat(),
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
            updated_at_utc=helpers.datetime.now(helpers.UTC).isoformat(),
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
            updated_at_utc=helpers.datetime.now(helpers.UTC).isoformat(),
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
            updated_at_utc=helpers.datetime.now(helpers.UTC).isoformat(),
        )
        if updated is None:
            raise HTTPException(status_code=404, detail="catalog asset not found")
        return AdminCatalogOrganizationResponse(item=_to_admin_catalog_item(updated))

    @app.post("/v1/admin/catalog/reject", response_model=AdminCatalogRejectResponse)
    def admin_mark_catalog_reject(
        payload: AdminCatalogRejectRequest,
    ) -> AdminCatalogRejectResponse:
        store: UploadStateStore = app.state.upload_state_store
        safe_path = _require_safe_relative_path(payload.relative_path)
        record = store.add_catalog_reject(
            relative_path=safe_path,
            marked_at_utc=helpers.datetime.now(helpers.UTC).isoformat(),
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
        store: UploadStateStore = app.state.upload_state_store
        safe_path = _require_safe_relative_path(payload.relative_path)
        store.remove_catalog_reject(safe_path)
        return AdminCatalogRejectUnmarkResponse(relative_path=safe_path, is_rejected=False)

    @app.get("/v1/admin/catalog/rejects", response_model=AdminCatalogRejectQueueResponse)
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
        return AdminCatalogRejectQueueResponse(total=total, limit=limit, offset=offset, items=items)

    @app.post("/v1/admin/catalog/rejects/execute", response_model=ExecuteRejectsResponse)
    def admin_execute_rejects(payload: ExecuteRejectsRequest) -> ExecuteRejectsResponse:
        store: UploadStateStore = app.state.upload_state_store
        storage_root_path = Path(app.state.storage_root)
        executed: list[str] = []
        skipped: list[str] = []
        started_at_utc = helpers.datetime.now(helpers.UTC).isoformat()

        if payload.relative_paths:
            target_paths = payload.relative_paths
        else:
            _, all_rejected = store.list_catalog_rejects(limit=100000, offset=0)
            target_paths = [row.relative_path for row in all_rejected]
        helpers.APP_LOGGER.info(
            "admin_reject_execute_started timestamp=%s requested_paths=%s targets=%s",
            started_at_utc,
            len(payload.relative_paths or []),
            len(target_paths),
        )

        now = helpers.datetime.now(helpers.UTC).isoformat()
        for relative_path in target_paths:
            safe_path = _require_safe_relative_path(relative_path)
            _, rejects_batch = store.list_catalog_rejects(limit=100000, offset=0)
            matching_reject = next((row for row in rejects_batch if row.relative_path == safe_path), None)
            if matching_reject is None:
                skipped.append(safe_path)
                continue

            source_path = storage_root_path / safe_path
            trashed_at = helpers.datetime.fromisoformat(now)
            trash_year = f"{trashed_at.year:04d}"
            trash_month = f"{trashed_at.month:02d}"
            trash_day = f"{trashed_at.day:02d}"
            sha_prefix = matching_reject.sha256_hex[:12]
            trash_relative_path = (
                f".trash/{trash_year}/{trash_month}/{trash_day}/{sha_prefix}/{safe_path}"
            )
            trash_path = storage_root_path / trash_relative_path

            try:
                if source_path.exists():
                    trash_path.parent.mkdir(parents=True, exist_ok=True)
                    try:
                        helpers.os.replace(source_path, trash_path)
                    except OSError:
                        helpers.shutil.copy2(source_path, trash_path)
                        source_path.unlink()
                        trash_path.parent.mkdir(parents=True, exist_ok=True)

                size_bytes = trash_path.stat().st_size if trash_path.exists() else 0
                store.add_tombstone(
                    relative_path=safe_path,
                    sha256_hex=matching_reject.sha256_hex,
                    trashed_at_utc=now,
                    marked_reason=matching_reject.marked_reason,
                    trash_relative_path=trash_relative_path,
                    original_size_bytes=size_bytes,
                )

                if not store.delete_stored_file(safe_path):
                    store.delete_media_asset(safe_path)
                executed.append(safe_path)
            except OSError as exc:
                helpers.APP_LOGGER.warning(
                    "admin_reject_execute_item_failed timestamp=%s relative_path=%s reason=%s",
                    helpers.datetime.now(helpers.UTC).isoformat(),
                    safe_path,
                    str(exc),
                )
                skipped.append(safe_path)

        helpers.APP_LOGGER.info(
            "admin_reject_execute_finished timestamp=%s executed=%s skipped=%s skipped_examples=%s",
            helpers.datetime.now(helpers.UTC).isoformat(),
            len(executed),
            len(skipped),
            skipped[:10],
        )
        return ExecuteRejectsResponse(executed=executed, skipped=skipped)

    @app.get("/v1/admin/catalog/tombstones", response_model=TombstoneListResponse)
    def admin_list_tombstones(
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
        older_than_days: int | None = Query(default=None, ge=0),
    ) -> TombstoneListResponse:
        store: UploadStateStore = app.state.upload_state_store
        total, tombstones = store.list_tombstones(
            limit=limit,
            offset=offset,
            older_than_days=older_than_days,
        )
        now = helpers.datetime.now(helpers.UTC)
        items: list[TombstoneListItem] = []
        for tombstone in tombstones:
            trashed_at = helpers.datetime.fromisoformat(tombstone.trashed_at_utc)
            if trashed_at.tzinfo is None:
                trashed_at = trashed_at.replace(tzinfo=helpers.UTC)
            age_days = max(0, (now - trashed_at).days)
            days_until_purge = max(0, _TRASH_RETENTION_DAYS - age_days)
            items.append(
                TombstoneListItem(
                    relative_path=tombstone.relative_path,
                    sha256_hex=tombstone.sha256_hex,
                    trashed_at_utc=tombstone.trashed_at_utc,
                    marked_reason=tombstone.marked_reason,
                    trash_relative_path=tombstone.trash_relative_path,
                    original_size_bytes=tombstone.original_size_bytes,
                    age_days=age_days,
                    days_until_purge=days_until_purge,
                )
            )
        return TombstoneListResponse(total=total, limit=limit, offset=offset, items=items)

    @app.post("/v1/admin/catalog/tombstones/restore", response_model=TombstoneRestoreResponse)
    def admin_restore_tombstone(payload: TombstoneRestoreRequest) -> TombstoneRestoreResponse:
        store: UploadStateStore = app.state.upload_state_store
        storage_root_path = Path(app.state.storage_root)
        safe_path = _require_safe_relative_path(payload.relative_path)
        tombstone = store.get_tombstone_by_path(safe_path)
        if tombstone is None:
            raise HTTPException(status_code=404, detail="tombstone not found")

        trash_path = storage_root_path / tombstone.trash_relative_path
        dest_path = storage_root_path / safe_path
        if not trash_path.is_file():
            raise HTTPException(
                status_code=409,
                detail={"code": "trash_gone", "relative_path": safe_path},
            )

        dest_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            helpers.os.replace(trash_path, dest_path)
        except OSError:
            helpers.shutil.copy2(str(trash_path), str(dest_path))
            try:
                with dest_path.open("rb") as file_handle:
                    helpers.os.fsync(file_handle.fileno())
            except OSError:
                pass
            trash_path.unlink(missing_ok=True)

        now_utc = helpers.datetime.now(helpers.UTC).isoformat()
        store.delete_media_asset(safe_path)
        store.upsert_media_asset(
            relative_path=safe_path,
            sha256_hex=tombstone.sha256_hex,
            size_bytes=tombstone.original_size_bytes,
            origin_kind="restored",
            observed_at_utc=now_utc,
        )
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
        storage_root_path = Path(app.state.storage_root)
        preview_cache_root_path = Path(app.state.preview_cache_root)
        existing = store.get_media_asset_by_path(payload.relative_path)
        if existing is None:
            raise HTTPException(status_code=404, detail="catalog asset not found")

        helpers._attempt_preview_generation(
            store=store,
            storage_root_path=storage_root_path,
            preview_cache_root_path=preview_cache_root_path,
            preview_max_long_edge=int(app.state.preview_max_long_edge),
            preview_passthrough_suffixes=frozenset(app.state.preview_passthrough_suffixes),
            preview_placeholder_suffixes=frozenset(app.state.preview_placeholder_suffixes),
            relative_path=payload.relative_path,
        )
        updated = store.get_media_asset_by_path(payload.relative_path)
        if updated is None:
            raise HTTPException(status_code=404, detail="catalog asset not found after preview retry")
        return AdminRetryPreviewResponse(item=_to_admin_catalog_item(updated))

    @app.get("/v1/admin/catalog/preview")
    def admin_catalog_preview_file(
        relative_path: str = Query(min_length=1),
        max_long_edge: int | None = Query(default=None, ge=1),
    ) -> FileResponse:
        store: UploadStateStore = app.state.upload_state_store
        storage_root_path = Path(app.state.storage_root)
        preview_cache_root_path = Path(app.state.preview_cache_root)
        record = store.get_media_asset_by_path(relative_path)
        if record is None:
            raise HTTPException(status_code=404, detail="catalog asset not found")
        if record.preview_status != "succeeded":
            raise HTTPException(status_code=404, detail="preview not available")
        requested_max_long_edge = (
            int(max_long_edge) if max_long_edge is not None else int(app.state.preview_max_long_edge)
        )

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

        preview_relative_path = record.preview_relative_path
        if requested_max_long_edge != int(app.state.preview_max_long_edge):
            try:
                preview_relative_path = helpers._ensure_preview_cache_file(
                    storage_root_path=storage_root_path,
                    preview_cache_root_path=preview_cache_root_path,
                    relative_path=record.relative_path,
                    sha256_hex=record.sha256_hex,
                    preview_max_long_edge=requested_max_long_edge,
                )
            except (OSError, ValueError) as exc:
                raise HTTPException(status_code=404, detail=f"preview unavailable: {exc}") from exc

        preview_path = (preview_cache_root_path / preview_relative_path).resolve()
        try:
            preview_path.relative_to(preview_cache_root_path)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="invalid preview path") from exc
        if not preview_path.is_file():
            raise HTTPException(status_code=404, detail="preview file missing")
        return FileResponse(preview_path, media_type="image/jpeg")

    @app.post("/v1/admin/catalog/extraction/retry", response_model=AdminRetryExtractionResponse)
    def admin_retry_catalog_extraction(
        payload: AdminRetryExtractionRequest,
    ) -> AdminRetryExtractionResponse:
        store: UploadStateStore = app.state.upload_state_store
        storage_root_path = Path(app.state.storage_root)
        existing = store.get_media_asset_by_path(payload.relative_path)
        if existing is None:
            raise HTTPException(status_code=404, detail="catalog asset not found")

        helpers._attempt_media_extraction(
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
        storage_root_path = Path(app.state.storage_root)
        started_at_utc = helpers.datetime.now(helpers.UTC).isoformat()
        helpers.APP_LOGGER.info(
            (
                "admin_extraction_backfill_started timestamp=%s target_statuses=%s limit=%s "
                "origin_kind=%s media_type=%s preview_capability=%s"
            ),
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
            allowed_statuses={"pending", "failed", "succeeded"},
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
            helpers._attempt_media_extraction(
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
            completed_at_utc=helpers.datetime.now(helpers.UTC).isoformat(),
        )
        store.record_catalog_backfill_run(run_record)
        failed_details = [
            f"{item.relative_path}: {item.extraction_failure_detail}"
            for item in updated_items
            if item.extraction_status == "failed" and item.extraction_failure_detail is not None
        ]
        helpers.APP_LOGGER.info(
            (
                "admin_extraction_backfill_finished timestamp=%s selected=%s processed=%s "
                "succeeded=%s failed=%s failure_examples=%s"
            ),
            helpers.datetime.now(helpers.UTC).isoformat(),
            len(candidates),
            len(updated_items),
            succeeded_count,
            failed_count,
            failed_details[:10],
        )
        return AdminBackfillCatalogResponse(run=_to_backfill_run_summary(run_record), items=updated_items)

    @app.post("/v1/admin/catalog/preview/backfill", response_model=AdminBackfillCatalogResponse)
    def admin_backfill_catalog_preview(
        payload: AdminBackfillCatalogRequest,
    ) -> AdminBackfillCatalogResponse:
        storage_root_path = Path(app.state.storage_root)
        preview_cache_root_path = Path(app.state.preview_cache_root)
        started_at_utc = helpers.datetime.now(helpers.UTC).isoformat()
        helpers.APP_LOGGER.info(
            (
                "admin_preview_backfill_started timestamp=%s target_statuses=%s limit=%s "
                "origin_kind=%s media_type=%s preview_capability=%s"
            ),
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
            allowed_statuses={"pending", "failed", "succeeded"},
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
            helpers._attempt_preview_generation(
                store=store,
                storage_root_path=storage_root_path,
                preview_cache_root_path=preview_cache_root_path,
                preview_max_long_edge=int(app.state.preview_max_long_edge),
                preview_passthrough_suffixes=frozenset(app.state.preview_passthrough_suffixes),
                preview_placeholder_suffixes=frozenset(app.state.preview_placeholder_suffixes),
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
            completed_at_utc=helpers.datetime.now(helpers.UTC).isoformat(),
        )
        store.record_catalog_backfill_run(run_record)
        failed_details = [
            f"{item.relative_path}: {item.preview_failure_detail}"
            for item in updated_items
            if item.preview_status == "failed" and item.preview_failure_detail is not None
        ]
        helpers.APP_LOGGER.info(
            (
                "admin_preview_backfill_finished timestamp=%s selected=%s processed=%s "
                "succeeded=%s failed=%s failure_examples=%s"
            ),
            helpers.datetime.now(helpers.UTC).isoformat(),
            len(candidates),
            len(updated_items),
            succeeded_count,
            failed_count,
            failed_details[:10],
        )
        return AdminBackfillCatalogResponse(run=_to_backfill_run_summary(run_record), items=updated_items)

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
