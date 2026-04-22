"""SSR monitoring UI for the photovault server."""

from __future__ import annotations

import json
import os
from pathlib import PurePosixPath
from typing import Any, Callable
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from flask import Flask, Response, redirect, render_template, request, url_for

ApiFetcher = Callable[[str, dict[str, str]], dict[str, Any]]
ApiPoster = Callable[[str, dict[str, Any]], dict[str, Any]]


def _default_api_fetcher(path: str, query: dict[str, str]) -> dict[str, Any]:
    base_url = os.getenv("PHOTOVAULT_SERVER_UI_API_BASE_URL", "http://127.0.0.1:9301")
    query_suffix = f"?{urlencode(query)}" if query else ""
    url = f"{base_url}{path}{query_suffix}"
    req = Request(url=url, method="GET")
    with urlopen(req, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def _default_api_poster(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    base_url = os.getenv("PHOTOVAULT_SERVER_UI_API_BASE_URL", "http://127.0.0.1:9301")
    url = f"{base_url}{path}"
    data = json.dumps(payload).encode("utf-8")
    req = Request(
        url=url,
        method="POST",
        data=data,
        headers={"Content-Type": "application/json"},
    )
    with urlopen(req, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


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
        metadata_bits.append(f"captured {capture}")
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


def _format_sha_for_display(value: str | None, chunk_size: int = 8) -> str:
    if not value:
        return "n/a"
    return " ".join(value[index : index + chunk_size] for index in range(0, len(value), chunk_size))


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
    values = {key: request.form.get(key, "") for key in request.form.keys()}
    return _catalog_query_state_from_values(values)


def create_app(*, api_fetcher: ApiFetcher | None = None, api_poster: ApiPoster | None = None) -> Flask:
    app = Flask(__name__)
    app.jinja_env.globals["format_sha_for_display"] = _format_sha_for_display
    fetcher = api_fetcher or _default_api_fetcher
    poster = api_poster or _default_api_poster
    page_size = 50
    insight_page_size = 25

    def _catalog_action_redirect(
        *,
        relative_path: str,
        page: str,
        query_state: dict[str, str],
        return_to: str,
        action_message: str | None = None,
        action_error: str | None = None,
    ):
        if return_to == "asset" and relative_path:
            return redirect(
                url_for(
                    "catalog_asset_detail",
                    relative_path=relative_path,
                    page=page,
                    action_message=action_message,
                    action_error=action_error,
                    **query_state,
                )
            )
        return redirect(
            url_for(
                "catalog",
                page=page,
                action_message=action_message,
                action_error=action_error,
                **query_state,
            )
        )

    @app.get("/")
    def index() -> str:
        default_overview = {
            "total_known_sha256": 0,
            "total_stored_files": 0,
            "indexed_files": 0,
            "uploaded_files": 0,
            "duplicate_file_paths": 0,
            "recent_indexed_files_24h": 0,
            "recent_uploaded_files_24h": 0,
            "last_indexed_at_utc": None,
            "last_uploaded_at_utc": None,
        }
        default_latest_run = {"latest_run": None}
        error_message: str | None = None
        try:
            overview = fetcher("/v1/admin/overview", {})
            latest_run = fetcher("/v1/admin/latest-index-run", {})
        except (URLError, TimeoutError, ValueError):
            overview = default_overview
            latest_run = default_latest_run
            error_message = "Unable to reach photovault-api overview endpoint."

        return render_template(
            "dashboard.html",
            overview=overview,
            latest_run=latest_run.get("latest_run"),
            error_message=error_message,
            active_page="home",
        )

    @app.get("/files")
    def files() -> str:
        raw_page = request.args.get("page", "1")
        try:
            page = max(1, int(raw_page))
        except ValueError:
            page = 1
        offset = (page - 1) * page_size
        error_message: str | None = None
        try:
            payload = fetcher("/v1/admin/files", {"limit": str(page_size), "offset": str(offset)})
        except (URLError, TimeoutError, ValueError):
            payload = {"total": 0, "limit": page_size, "offset": offset, "items": []}
            error_message = "Unable to reach photovault-api files endpoint."

        total = int(payload.get("total", 0))
        items = list(payload.get("items", []))
        has_previous = page > 1
        has_next = offset + len(items) < total
        start_index = offset + 1 if total > 0 and items else 0
        end_index = offset + len(items)

        for item in items:
            size_bytes = int(item.get("size_bytes", 0))
            item["size_human"] = _format_size_bytes(size_bytes)
            item["sha256_display"] = _format_sha_for_display(str(item.get("sha256_hex") or ""))

        return render_template(
            "files.html",
            files=items,
            page=page,
            page_size=page_size,
            total=total,
            has_previous=has_previous,
            has_next=has_next,
            start_index=start_index,
            end_index=end_index,
            error_message=error_message,
            active_page="files",
        )

    @app.get("/clients")
    def clients() -> str:
        raw_page = request.args.get("page", "1")
        try:
            page = max(1, int(raw_page))
        except ValueError:
            page = 1
        offset = (page - 1) * page_size
        presence_status_filter = request.args.get("presence_status", "").strip()
        workload_status_filter = request.args.get("workload_status", "").strip()
        enrollment_status_filter = request.args.get("enrollment_status", "").strip()
        sort_by = request.args.get("sort_by", "").strip() or "last_seen"
        sort_order = request.args.get("sort_order", "").strip() or "desc"
        clients_query_state = {
            "presence_status": presence_status_filter,
            "workload_status": workload_status_filter,
            "enrollment_status": enrollment_status_filter,
            "sort_by": sort_by,
            "sort_order": sort_order,
        }
        action_message = request.args.get("action_message")
        action_error = request.args.get("action_error")
        error_message: str | None = None
        query: dict[str, str] = {"limit": str(page_size), "offset": str(offset)}
        if presence_status_filter:
            query["presence_status"] = presence_status_filter
        if workload_status_filter:
            query["workload_status"] = workload_status_filter
        if enrollment_status_filter:
            query["enrollment_status"] = enrollment_status_filter
        if request.args.get("sort_by", "").strip():
            query["sort_by"] = sort_by
        if request.args.get("sort_order", "").strip():
            query["sort_order"] = sort_order
        try:
            payload = fetcher("/v1/admin/clients", query)
        except (URLError, TimeoutError, ValueError):
            payload = {"total": 0, "limit": page_size, "offset": offset, "items": []}
            error_message = "Unable to reach photovault-api client registry endpoint."

        total = int(payload.get("total", 0))
        items = list(payload.get("items", []))
        has_previous = page > 1
        has_next = offset + len(items) < total
        start_index = offset + 1 if total > 0 and items else 0
        end_index = offset + len(items)
        previous_url = (
            url_for("clients", page=page - 1, **clients_query_state)
            if has_previous
            else None
        )
        next_url = (
            url_for("clients", page=page + 1, **clients_query_state)
            if has_next
            else None
        )

        return render_template(
            "clients.html",
            clients=items,
            client_summary=_count_client_summary(items),
            page=page,
            page_size=page_size,
            total=total,
            has_previous=has_previous,
            has_next=has_next,
            start_index=start_index,
            end_index=end_index,
            error_message=error_message,
            action_message=action_message,
            action_error=action_error,
            clients_query_state=clients_query_state,
            previous_url=previous_url,
            next_url=next_url,
            active_page="clients",
        )

    @app.post("/clients/actions/approve")
    def approve_client_action():
        client_id = request.form.get("client_id", "").strip()
        page = request.form.get("page", "1")
        clients_query_state = {
            "presence_status": request.form.get("presence_status", "").strip(),
            "workload_status": request.form.get("workload_status", "").strip(),
            "enrollment_status": request.form.get("enrollment_status", "").strip(),
            "sort_by": request.form.get("sort_by", "").strip() or "last_seen",
            "sort_order": request.form.get("sort_order", "").strip() or "desc",
        }
        if not client_id:
            return redirect(
                url_for(
                    "clients",
                    page=page,
                    action_error="Missing client id for approval.",
                    **clients_query_state,
                )
            )
        try:
            poster(f"/v1/admin/clients/{client_id}/approve", {})
        except (URLError, TimeoutError, ValueError):
            return redirect(
                url_for(
                    "clients",
                    page=page,
                    action_error=f"Approval failed for {client_id}.",
                    **clients_query_state,
                )
            )
        return redirect(
            url_for(
                "clients",
                page=page,
                action_message=f"Approved client {client_id}.",
                **clients_query_state,
            )
        )

    @app.post("/clients/actions/revoke")
    def revoke_client_action():
        client_id = request.form.get("client_id", "").strip()
        page = request.form.get("page", "1")
        clients_query_state = {
            "presence_status": request.form.get("presence_status", "").strip(),
            "workload_status": request.form.get("workload_status", "").strip(),
            "enrollment_status": request.form.get("enrollment_status", "").strip(),
            "sort_by": request.form.get("sort_by", "").strip() or "last_seen",
            "sort_order": request.form.get("sort_order", "").strip() or "desc",
        }
        if not client_id:
            return redirect(
                url_for(
                    "clients",
                    page=page,
                    action_error="Missing client id for revocation.",
                    **clients_query_state,
                )
            )
        try:
            poster(f"/v1/admin/clients/{client_id}/revoke", {})
        except (URLError, TimeoutError, ValueError):
            return redirect(
                url_for(
                    "clients",
                    page=page,
                    action_error=f"Revocation failed for {client_id}.",
                    **clients_query_state,
                )
            )
        return redirect(
            url_for(
                "clients",
                page=page,
                action_message=f"Revoked client {client_id}.",
                **clients_query_state,
            )
        )

    @app.get("/duplicates")
    def duplicates() -> str:
        error_message: str | None = None
        try:
            payload = fetcher("/v1/admin/duplicates", {"limit": str(insight_page_size), "offset": "0"})
        except (URLError, TimeoutError, ValueError):
            payload = {"total": 0, "limit": insight_page_size, "offset": 0, "items": []}
            error_message = "Unable to reach photovault-api duplicates endpoint."

        groups = list(payload.get("items", []))
        for group in groups:
            group["sha256_display"] = _format_sha_for_display(str(group.get("sha256_hex") or ""))
        return render_template(
            "duplicates.html",
            groups=groups,
            total=int(payload.get("total", 0)),
            error_message=error_message,
            active_page="duplicates",
        )

    @app.get("/conflicts")
    def conflicts() -> str:
        error_message: str | None = None
        try:
            payload = fetcher("/v1/admin/path-conflicts", {"limit": str(insight_page_size), "offset": "0"})
            latest_run = fetcher("/v1/admin/latest-index-run", {})
        except (URLError, TimeoutError, ValueError):
            payload = {"total": 0, "limit": insight_page_size, "offset": 0, "items": []}
            latest_run = {"latest_run": None}
            error_message = "Unable to reach photovault-api conflict inspection endpoints."

        conflicts_list = list(payload.get("items", []))
        for conflict in conflicts_list:
            conflict["previous_sha256_display"] = _format_sha_for_display(
                str(conflict.get("previous_sha256_hex") or "")
            )
            conflict["current_sha256_display"] = _format_sha_for_display(
                str(conflict.get("current_sha256_hex") or "")
            )

        return render_template(
            "conflicts.html",
            conflicts=conflicts_list,
            total=int(payload.get("total", 0)),
            latest_run=latest_run.get("latest_run"),
            error_message=error_message,
            active_page="conflicts",
        )

    @app.get("/catalog")
    def catalog() -> str:
        raw_page = request.args.get("page", "1")
        try:
            page = max(1, int(raw_page))
        except ValueError:
            page = 1
        offset = (page - 1) * page_size
        extraction_status_filter = request.args.get("extraction_status", "").strip()
        origin_kind_filter = request.args.get("origin_kind", "").strip()
        media_type_filter = request.args.get("media_type", "").strip()
        preview_capability_filter = request.args.get("preview_capability", "").strip()
        preview_status_filter = request.args.get("preview_status", "").strip()
        is_favorite_filter = request.args.get("is_favorite", "").strip()
        is_archived_filter = request.args.get("is_archived", "").strip()
        cataloged_since_filter = request.args.get("cataloged_since_utc", "").strip()
        cataloged_before_filter = request.args.get("cataloged_before_utc", "").strip()
        action_message = request.args.get("action_message")
        action_error = request.args.get("action_error")
        error_message: str | None = None
        latest_backfill_runs: dict[str, Any] = {"extraction_run": None, "preview_run": None}
        query: dict[str, str] = {"limit": str(page_size), "offset": str(offset)}
        if extraction_status_filter:
            query["extraction_status"] = extraction_status_filter
        if origin_kind_filter:
            query["origin_kind"] = origin_kind_filter
        if media_type_filter:
            query["media_type"] = media_type_filter
        if preview_capability_filter:
            query["preview_capability"] = preview_capability_filter
        if preview_status_filter:
            query["preview_status"] = preview_status_filter
        if is_favorite_filter:
            query["is_favorite"] = is_favorite_filter
        if is_archived_filter:
            query["is_archived"] = is_archived_filter
        if cataloged_since_filter:
            query["cataloged_since_utc"] = cataloged_since_filter
        if cataloged_before_filter:
            query["cataloged_before_utc"] = cataloged_before_filter
        try:
            payload = fetcher("/v1/admin/catalog", query)
        except (URLError, TimeoutError, ValueError):
            payload = {"total": 0, "limit": page_size, "offset": offset, "items": []}
            error_message = "Unable to reach photovault-api catalog endpoint."
        try:
            latest_backfill_runs = fetcher("/v1/admin/catalog/backfill/latest", {})
        except (URLError, TimeoutError, ValueError):
            if error_message is None:
                error_message = "Unable to reach photovault-api catalog backfill endpoint."

        total = int(payload.get("total", 0))
        items = list(payload.get("items", []))
        has_previous = page > 1
        has_next = offset + len(items) < total
        start_index = offset + 1 if total > 0 and items else 0
        end_index = offset + len(items)

        for item in items:
            size_bytes = int(item.get("size_bytes", 0))
            item["size_human"] = _format_size_bytes(size_bytes)
            item["metadata_summary"] = _catalog_metadata_summary(item)
            preview_status = str(item.get("preview_status") or "pending")
            if preview_status == "succeeded":
                item["preview_summary"] = "Preview available"
            elif preview_status == "failed":
                item["preview_summary"] = "Preview failed"
            else:
                item["preview_summary"] = "Preview pending"
            item["filename"] = PurePosixPath(str(item.get("relative_path", ""))).name
            item["sha256_display"] = _format_sha_for_display(str(item.get("sha256_hex") or ""))

        filter_query = _catalog_query_state_from_args()
        previous_url = url_for("catalog", page=page - 1, **filter_query) if has_previous else None
        next_url = url_for("catalog", page=page + 1, **filter_query) if has_next else None

        return render_template(
            "catalog.html",
            assets=items,
            page=page,
            page_size=page_size,
            total=total,
            has_previous=has_previous,
            has_next=has_next,
            start_index=start_index,
            end_index=end_index,
            error_message=error_message,
            action_message=action_message,
            action_error=action_error,
            extraction_status_filter=extraction_status_filter,
            origin_kind_filter=origin_kind_filter,
            media_type_filter=media_type_filter,
            preview_capability_filter=preview_capability_filter,
            preview_status_filter=preview_status_filter,
            is_favorite_filter=is_favorite_filter,
            is_archived_filter=is_archived_filter,
            cataloged_since_filter=cataloged_since_filter,
            cataloged_before_filter=cataloged_before_filter,
            catalog_query_state=filter_query,
            latest_backfill_runs=latest_backfill_runs,
            previous_url=previous_url,
            next_url=next_url,
            active_page="catalog",
        )

    @app.get("/catalog/asset")
    def catalog_asset_detail() -> str:
        relative_path = request.args.get("relative_path", "").strip()
        if not relative_path:
            return redirect(url_for("catalog", action_error="Missing catalog relative path for detail view."))

        page = request.args.get("page", "1").strip() or "1"
        extraction_status_filter = request.args.get("extraction_status", "").strip()
        origin_kind_filter = request.args.get("origin_kind", "").strip()
        media_type_filter = request.args.get("media_type", "").strip()
        preview_capability_filter = request.args.get("preview_capability", "").strip()
        preview_status_filter = request.args.get("preview_status", "").strip()
        is_favorite_filter = request.args.get("is_favorite", "").strip()
        is_archived_filter = request.args.get("is_archived", "").strip()
        cataloged_since_filter = request.args.get("cataloged_since_utc", "").strip()
        cataloged_before_filter = request.args.get("cataloged_before_utc", "").strip()
        action_message = request.args.get("action_message")
        action_error = request.args.get("action_error")
        error_message: str | None = None

        try:
            payload = fetcher("/v1/admin/catalog/asset", {"relative_path": relative_path})
        except (URLError, TimeoutError, ValueError):
            payload = {"item": None}
            error_message = "Unable to reach photovault-api catalog detail endpoint."

        item = payload.get("item")
        if item is not None:
            size_bytes = int(item.get("size_bytes", 0))
            item["size_human"] = _format_size_bytes(size_bytes)
            item["metadata_summary"] = _catalog_metadata_summary(item)
            item["filename"] = PurePosixPath(str(item.get("relative_path", ""))).name

        catalog_query_state = _catalog_query_state_from_args()

        return render_template(
            "catalog_asset.html",
            asset=item,
            page=page,
            extraction_status_filter=extraction_status_filter,
            origin_kind_filter=origin_kind_filter,
            media_type_filter=media_type_filter,
            preview_capability_filter=preview_capability_filter,
            preview_status_filter=preview_status_filter,
            is_favorite_filter=is_favorite_filter,
            is_archived_filter=is_archived_filter,
            cataloged_since_filter=cataloged_since_filter,
            cataloged_before_filter=cataloged_before_filter,
            catalog_query_state=catalog_query_state,
            error_message=error_message,
            action_message=action_message,
            action_error=action_error,
            active_page="catalog",
        )

    @app.post("/catalog/actions/favorite/mark")
    def catalog_favorite_mark_action():
        relative_path = request.form.get("relative_path", "").strip()
        page = request.form.get("page", "1").strip() or "1"
        return_to = request.form.get("return_to", "catalog").strip() or "catalog"
        query_state = _catalog_query_state_from_form()
        if not relative_path:
            return _catalog_action_redirect(
                relative_path=relative_path,
                page=page,
                query_state=query_state,
                return_to=return_to,
                action_error="Missing catalog relative path.",
            )
        try:
            poster("/v1/admin/catalog/favorite/mark", {"relative_path": relative_path})
        except (URLError, TimeoutError, ValueError):
            return _catalog_action_redirect(
                relative_path=relative_path,
                page=page,
                query_state=query_state,
                return_to=return_to,
                action_error=f"Failed to mark favorite for {relative_path}.",
            )
        return _catalog_action_redirect(
            relative_path=relative_path,
            page=page,
            query_state=query_state,
            return_to=return_to,
            action_message=f"Marked favorite: {relative_path}.",
        )

    @app.post("/catalog/actions/favorite/unmark")
    def catalog_favorite_unmark_action():
        relative_path = request.form.get("relative_path", "").strip()
        page = request.form.get("page", "1").strip() or "1"
        return_to = request.form.get("return_to", "catalog").strip() or "catalog"
        query_state = _catalog_query_state_from_form()
        if not relative_path:
            return _catalog_action_redirect(
                relative_path=relative_path,
                page=page,
                query_state=query_state,
                return_to=return_to,
                action_error="Missing catalog relative path.",
            )
        try:
            poster("/v1/admin/catalog/favorite/unmark", {"relative_path": relative_path})
        except (URLError, TimeoutError, ValueError):
            return _catalog_action_redirect(
                relative_path=relative_path,
                page=page,
                query_state=query_state,
                return_to=return_to,
                action_error=f"Failed to unmark favorite for {relative_path}.",
            )
        return _catalog_action_redirect(
            relative_path=relative_path,
            page=page,
            query_state=query_state,
            return_to=return_to,
            action_message=f"Unmarked favorite: {relative_path}.",
        )

    @app.post("/catalog/actions/archive/mark")
    def catalog_archive_mark_action():
        relative_path = request.form.get("relative_path", "").strip()
        page = request.form.get("page", "1").strip() or "1"
        return_to = request.form.get("return_to", "catalog").strip() or "catalog"
        query_state = _catalog_query_state_from_form()
        if not relative_path:
            return _catalog_action_redirect(
                relative_path=relative_path,
                page=page,
                query_state=query_state,
                return_to=return_to,
                action_error="Missing catalog relative path.",
            )
        try:
            poster("/v1/admin/catalog/archive/mark", {"relative_path": relative_path})
        except (URLError, TimeoutError, ValueError):
            return _catalog_action_redirect(
                relative_path=relative_path,
                page=page,
                query_state=query_state,
                return_to=return_to,
                action_error=f"Failed to archive {relative_path}.",
            )
        return _catalog_action_redirect(
            relative_path=relative_path,
            page=page,
            query_state=query_state,
            return_to=return_to,
            action_message=f"Archived asset: {relative_path}.",
        )

    @app.post("/catalog/actions/archive/unmark")
    def catalog_archive_unmark_action():
        relative_path = request.form.get("relative_path", "").strip()
        page = request.form.get("page", "1").strip() or "1"
        return_to = request.form.get("return_to", "catalog").strip() or "catalog"
        query_state = _catalog_query_state_from_form()
        if not relative_path:
            return _catalog_action_redirect(
                relative_path=relative_path,
                page=page,
                query_state=query_state,
                return_to=return_to,
                action_error="Missing catalog relative path.",
            )
        try:
            poster("/v1/admin/catalog/archive/unmark", {"relative_path": relative_path})
        except (URLError, TimeoutError, ValueError):
            return _catalog_action_redirect(
                relative_path=relative_path,
                page=page,
                query_state=query_state,
                return_to=return_to,
                action_error=f"Failed to unarchive {relative_path}.",
            )
        return _catalog_action_redirect(
            relative_path=relative_path,
            page=page,
            query_state=query_state,
            return_to=return_to,
            action_message=f"Unarchived asset: {relative_path}.",
        )

    @app.post("/catalog/actions/backfill")
    def catalog_backfill_action():
        page = request.form.get("page", "1").strip() or "1"
        query_state = _catalog_query_state_from_form()
        backfill_kind = request.form.get("backfill_kind", "").strip().lower()
        if backfill_kind not in {"extraction", "preview"}:
            return _catalog_action_redirect(
                relative_path="",
                page=page,
                query_state=query_state,
                return_to="catalog",
                action_error="Unknown backfill kind.",
            )

        raw_limit = request.form.get("limit", "50").strip()
        try:
            limit = int(raw_limit)
        except ValueError:
            return _catalog_action_redirect(
                relative_path="",
                page=page,
                query_state=query_state,
                return_to="catalog",
                action_error="Backfill limit must be a number.",
            )
        limit = min(500, max(1, limit))

        requested_statuses: list[str] = []
        for status in request.form.getlist("target_statuses"):
            normalized = status.strip().lower()
            if normalized in {"pending", "failed"} and normalized not in requested_statuses:
                requested_statuses.append(normalized)
        if not requested_statuses:
            requested_statuses = ["pending", "failed"]

        payload: dict[str, Any] = {
            "target_statuses": requested_statuses,
            "limit": limit,
        }
        for key in (
            "origin_kind",
            "media_type",
            "preview_capability",
            "cataloged_since_utc",
            "cataloged_before_utc",
        ):
            value = query_state.get(key, "").strip()
            if value:
                payload[key] = value

        endpoint = (
            "/v1/admin/catalog/extraction/backfill"
            if backfill_kind == "extraction"
            else "/v1/admin/catalog/preview/backfill"
        )
        try:
            response = poster(endpoint, payload)
            run = response.get("run", {})
        except (URLError, TimeoutError, ValueError):
            return _catalog_action_redirect(
                relative_path="",
                page=page,
                query_state=query_state,
                return_to="catalog",
                action_error=f"{backfill_kind.title()} backfill request failed.",
            )

        return _catalog_action_redirect(
            relative_path="",
            page=page,
            query_state=query_state,
            return_to="catalog",
            action_message=(
                f"{backfill_kind.title()} backfill completed: selected={run.get('selected_count', 0)}, "
                f"succeeded={run.get('succeeded_count', 0)}, failed={run.get('failed_count', 0)}, "
                f"remaining pending={run.get('remaining_pending_count', 0)}, "
                f"remaining failed={run.get('remaining_failed_count', 0)}."
            ),
        )

    @app.get("/catalog/preview")
    def catalog_preview_proxy() -> Response:
        relative_path = request.args.get("relative_path", "").strip()
        if not relative_path:
            return Response("missing relative_path", status=400, mimetype="text/plain")
        base_url = os.getenv("PHOTOVAULT_SERVER_UI_API_BASE_URL", "http://127.0.0.1:9301")
        query_suffix = urlencode({"relative_path": relative_path})
        req = Request(url=f"{base_url}/v1/admin/catalog/preview?{query_suffix}", method="GET")
        try:
            with urlopen(req, timeout=10) as response:
                content = response.read()
                content_type = response.headers.get("Content-Type", "image/jpeg")
                return Response(content, status=200, mimetype=content_type)
        except (URLError, TimeoutError, ValueError):
            return Response("preview unavailable", status=404, mimetype="text/plain")


    return app
