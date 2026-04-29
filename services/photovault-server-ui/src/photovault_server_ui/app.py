"""SSR monitoring UI for the photovault server."""
from __future__ import annotations

import os
from typing import Any
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from flask import Flask, Response, redirect, render_template, request, url_for

from .api_client import ApiFetcher, ApiPoster, _default_api_fetcher, _default_api_poster
from .catalog_pages import (
    build_library_folder_tree,
    catalog_action_redirect,
    render_catalog_page,
    sanitize_library_prefix,
)
from .client_pages import is_hx_request, render_clients_page, render_conflicts_page, render_duplicates_page
from .formatters import (
    _catalog_query_state_from_args,
    _catalog_query_state_from_form,
    _decorate_catalog_item,
    _format_exposure_summary,
    _format_sha_for_display,
    _format_shutter_speed,
    _format_size_bytes,
    _local_to_utc_iso,
    _timestamp_parts,
)

_APP_FORMAT_EXPORTS = (_format_exposure_summary, _format_shutter_speed)

def create_app(*, api_fetcher: ApiFetcher | None = None, api_poster: ApiPoster | None = None) -> Flask:
    app = Flask(__name__)
    app.jinja_env.globals["format_sha_for_display"] = _format_sha_for_display
    app.jinja_env.globals["timestamp_parts"] = _timestamp_parts
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
        return catalog_action_redirect(
            relative_path=relative_path,
            page=page,
            query_state=query_state,
            return_to=return_to,
            action_message=action_message,
            action_error=action_error,
        )

    def _is_hx_request() -> bool:
        return is_hx_request()

    def _render_clients_page(
        *,
        page: int,
        clients_query_state: dict[str, str],
        action_message: str | None = None,
        action_error: str | None = None,
        include_sort_by: bool = False,
        include_sort_order: bool = False,
    ) -> str:
        return render_clients_page(
            fetcher,
            page=page,
            clients_query_state=clients_query_state,
            page_size=page_size,
            action_message=action_message,
            action_error=action_error,
            include_sort_by=include_sort_by,
            include_sort_order=include_sort_order,
        )

    def _render_duplicates_page(
        *,
        action_message: str | None = None,
        action_error: str | None = None,
    ) -> str:
        return render_duplicates_page(
            fetcher,
            insight_page_size=insight_page_size,
            action_message=action_message,
            action_error=action_error,
        )

    def _render_catalog_page(
        *,
        page: int,
        catalog_filters: dict[str, str],
        action_message: str | None = None,
        action_error: str | None = None,
    ) -> str:
        return render_catalog_page(
            fetcher,
            page=page,
            page_size=page_size,
            is_hx_request=_is_hx_request(),
            catalog_filters=catalog_filters,
            action_message=action_message,
            action_error=action_error,
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
        sort_by_raw = request.args.get("sort_by", "").strip()
        sort_order_raw = request.args.get("sort_order", "").strip()
        clients_query_state = {
            "presence_status": request.args.get("presence_status", "").strip(),
            "workload_status": request.args.get("workload_status", "").strip(),
            "enrollment_status": request.args.get("enrollment_status", "").strip(),
            "sort_by": sort_by_raw or "last_seen",
            "sort_order": sort_order_raw or "desc",
        }
        return _render_clients_page(
            page=page,
            clients_query_state=clients_query_state,
            action_message=request.args.get("action_message"),
            action_error=request.args.get("action_error"),
            include_sort_by=bool(sort_by_raw),
            include_sort_order=bool(sort_order_raw),
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
        try:
            page_number = max(1, int(page))
        except ValueError:
            page_number = 1
        if not client_id:
            if _is_hx_request():
                return _render_clients_page(
                    page=page_number,
                    clients_query_state=clients_query_state,
                    action_error="Missing client id for approval.",
                    include_sort_by=True,
                    include_sort_order=True,
                )
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
            if _is_hx_request():
                return _render_clients_page(
                    page=page_number,
                    clients_query_state=clients_query_state,
                    action_error=f"Approval failed for {client_id}.",
                    include_sort_by=True,
                    include_sort_order=True,
                )
            return redirect(
                url_for(
                    "clients",
                    page=page,
                    action_error=f"Approval failed for {client_id}.",
                    **clients_query_state,
                )
            )
        if _is_hx_request():
            return _render_clients_page(
                page=page_number,
                clients_query_state=clients_query_state,
                action_message=f"Approved client {client_id}.",
                include_sort_by=True,
                include_sort_order=True,
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
        try:
            page_number = max(1, int(page))
        except ValueError:
            page_number = 1
        if not client_id:
            if _is_hx_request():
                return _render_clients_page(
                    page=page_number,
                    clients_query_state=clients_query_state,
                    action_error="Missing client id for revocation.",
                    include_sort_by=True,
                    include_sort_order=True,
                )
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
            if _is_hx_request():
                return _render_clients_page(
                    page=page_number,
                    clients_query_state=clients_query_state,
                    action_error=f"Revocation failed for {client_id}.",
                    include_sort_by=True,
                    include_sort_order=True,
                )
            return redirect(
                url_for(
                    "clients",
                    page=page,
                    action_error=f"Revocation failed for {client_id}.",
                    **clients_query_state,
                )
            )
        if _is_hx_request():
            return _render_clients_page(
                page=page_number,
                clients_query_state=clients_query_state,
                action_message=f"Revoked client {client_id}.",
                include_sort_by=True,
                include_sort_order=True,
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
        return _render_duplicates_page(
            action_message=request.args.get("action_message"),
            action_error=request.args.get("action_error"),
        )

    @app.get("/conflicts")
    def conflicts() -> str:
        return render_conflicts_page(fetcher, insight_page_size=insight_page_size)

    @app.get("/catalog")
    def catalog() -> str:
        raw_page = request.args.get("page", "1")
        try:
            page = max(1, int(raw_page))
        except ValueError:
            page = 1
        cataloged_since_filter = request.args.get("cataloged_since_utc", "").strip()
        cataloged_before_filter = request.args.get("cataloged_before_utc", "").strip()
        if not cataloged_since_filter:
            cataloged_since_filter = _local_to_utc_iso(
                request.args.get("cataloged_since_local", "")
            )
        if not cataloged_before_filter:
            cataloged_before_filter = _local_to_utc_iso(
                request.args.get("cataloged_before_local", "")
            )
        catalog_filters = {
            "extraction_status": request.args.get("extraction_status", "").strip(),
            "origin_kind": request.args.get("origin_kind", "").strip(),
            "media_type": request.args.get("media_type", "").strip(),
            "preview_capability": request.args.get("preview_capability", "").strip(),
            "preview_status": request.args.get("preview_status", "").strip(),
            "is_favorite": request.args.get("is_favorite", "").strip(),
            "is_archived": request.args.get("is_archived", "").strip(),
            "cataloged_since_utc": cataloged_since_filter,
            "cataloged_before_utc": cataloged_before_filter,
        }
        return _render_catalog_page(
            page=page,
            catalog_filters=catalog_filters,
            action_message=request.args.get("action_message"),
            action_error=request.args.get("action_error"),
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
            item = _decorate_catalog_item(item)

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

    @app.post("/catalog/actions/favorite/toggle")
    def catalog_favorite_toggle_action():
        """HTMX-friendly favorite toggle. Calls mark or unmark based on the
        currently_favorite form hint, then returns the freshly rendered asset
        card so the client can swap it in place. Falls back to a redirect if
        the request does not look like an HTMX invocation."""
        return _catalog_inline_toggle(kind="favorite")

    @app.post("/catalog/actions/archive/toggle")
    def catalog_archive_toggle_action():
        """HTMX-friendly archive toggle. See favorite toggle docstring."""
        return _catalog_inline_toggle(kind="archive")

    @app.post("/library/actions/reject/toggle")
    def library_reject_toggle_action():
        """HTMX-friendly reject-queue toggle used by the lightbox X-key and
        tile "×" affordance.

        Accepts ``relative_path`` and ``currently_rejected`` as form fields.
        Posts to the API's reject mark/unmark endpoint and returns a fresh
        lightbox fragment so the swapped body reflects the new queue state
        (rejected-badge, flipped button). Falls back to a plain 302 redirect
        for non-HTMX callers, preserving the folder filter if present.
        """

        relative_path = request.form.get("relative_path", "").strip()
        folder = _sanitize_library_prefix(request.form.get("folder", "").strip())
        try:
            index = max(0, int(request.form.get("index", "0")))
        except ValueError:
            index = 0
        try:
            total = max(1, int(request.form.get("total", "1")))
        except ValueError:
            total = 1
        return_to = request.form.get("return_to", "library").strip() or "library"
        currently_rejected = (
            request.form.get("currently_rejected", "false").strip().lower() == "true"
        )

        if not relative_path:
            return _library_reject_toggle_fallback(
                action_error="Missing catalog relative path for reject toggle.",
                folder=folder,
                return_to=return_to,
            )

        endpoint = (
            "/v1/admin/catalog/reject/unmark" if currently_rejected else "/v1/admin/catalog/reject"
        )
        try:
            poster(endpoint, {"relative_path": relative_path})
        except (URLError, TimeoutError, ValueError):
            return _library_reject_toggle_fallback(
                action_error=(
                    f"Failed to {'unmark' if currently_rejected else 'mark'} reject for {relative_path}."
                ),
                folder=folder,
                return_to=return_to,
            )

        if not _is_hx_request():
            if return_to == "duplicates":
                action_message = (
                    f"Restored {relative_path} from the delete queue."
                    if currently_rejected
                    else f"Marked {relative_path} for deletion."
                )
                return redirect(url_for("duplicates", action_message=action_message))
            target = "/library"
            if folder:
                target = f"{target}?folder={urlencode({'': folder})[1:]}"
            return redirect(target)

        if return_to == "duplicates":
            action_message = (
                f"Restored {relative_path} from the delete queue."
                if currently_rejected
                else f"Marked {relative_path} for deletion."
            )
            return _render_duplicates_page(action_message=action_message)

        # Re-render the lightbox fragment so the swap reflects the new state.
        try:
            fresh = fetcher(
                "/v1/admin/catalog/asset", {"relative_path": relative_path}
            )
            fresh_item = fresh.get("item")
        except (URLError, TimeoutError, ValueError):
            fresh_item = None
        if not fresh_item:
            return (
                "<div class=\"small text-danger\">Asset not found after reject toggle.</div>"
            )
        _decorate_catalog_item(fresh_item)
        return render_template(
            "_library_lightbox.html",
            asset=fresh_item,
            selected_folder=folder,
            index=index,
            total=total,
        )

    def _library_reject_toggle_fallback(*, action_error: str, folder: str, return_to: str):
        if return_to == "duplicates":
            if _is_hx_request():
                return _render_duplicates_page(action_error=action_error)
            return redirect(url_for("duplicates", action_error=action_error))
        target = "/library"
        query_parts: list[str] = []
        if folder:
            query_parts.append(urlencode({"folder": folder}))
        query_parts.append(urlencode({"action_error": action_error}))
        return redirect(target + "?" + "&".join(query_parts))

    def _catalog_inline_toggle(*, kind: str):
        assert kind in {"favorite", "archive"}
        relative_path = request.form.get("relative_path", "").strip()
        page = request.form.get("page", "1").strip() or "1"
        return_query = request.form.get("return_query", "").strip()
        currently_flag = request.form.get(
            "currently_favorite" if kind == "favorite" else "currently_archived",
            "false",
        ).strip().lower() == "true"

        if not relative_path:
            return _catalog_inline_toggle_fallback(
                action_error=f"Missing catalog relative path for {kind} toggle.",
                return_query=return_query,
            )

        target_action = "unmark" if currently_flag else "mark"
        endpoint = f"/v1/admin/catalog/{kind}/{target_action}"
        try:
            poster(endpoint, {"relative_path": relative_path})
        except (URLError, TimeoutError, ValueError):
            return _catalog_inline_toggle_fallback(
                action_error=f"Failed to {target_action} {kind} for {relative_path}.",
                return_query=return_query,
            )

        try:
            fresh = fetcher("/v1/admin/catalog/asset", {"relative_path": relative_path})
            fresh_item = fresh.get("item")
        except (URLError, TimeoutError, ValueError):
            fresh_item = None

        if request.headers.get("HX-Request", "").lower() == "true" and fresh_item:
            _decorate_catalog_item(fresh_item)
            return render_template(
                "_asset_card.html",
                asset=fresh_item,
                page=page,
                return_query=return_query,
            )

        # Non-HTMX fallback: redirect back to the catalog preserving filters.
        target = "/catalog"
        if return_query:
            target = f"{target}?{return_query}"
        return redirect(target)

    def _catalog_inline_toggle_fallback(*, action_error: str, return_query: str):
        # Consistent fallback for missing data or API errors in the HTMX path.
        target = "/catalog"
        query_parts: list[str] = []
        if return_query:
            query_parts.append(return_query)
        query_parts.append(urlencode({"action_error": action_error}))
        return redirect(target + "?" + "&".join(query_parts))

    @app.post("/catalog/actions/backfill")
    def catalog_backfill_action():
        page = request.form.get("page", "1").strip() or "1"
        try:
            page_number = max(1, int(page))
        except ValueError:
            page_number = 1
        query_state = _catalog_query_state_from_form()
        backfill_kind = request.form.get("backfill_kind", "").strip().lower()
        if backfill_kind not in {"extraction", "preview"}:
            if _is_hx_request():
                return _render_catalog_page(
                    page=page_number,
                    catalog_filters=query_state,
                    action_error="Unknown backfill kind.",
                )
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
            if _is_hx_request():
                return _render_catalog_page(
                    page=page_number,
                    catalog_filters=query_state,
                    action_error="Backfill limit must be a number.",
                )
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
            if _is_hx_request():
                return _render_catalog_page(
                    page=page_number,
                    catalog_filters=query_state,
                    action_error=f"{backfill_kind.title()} backfill request failed.",
                )
            return _catalog_action_redirect(
                relative_path="",
                page=page,
                query_state=query_state,
                return_to="catalog",
                action_error=f"{backfill_kind.title()} backfill request failed.",
            )

        success_message = (
            f"{backfill_kind.title()} backfill completed: selected={run.get('selected_count', 0)}, "
            f"succeeded={run.get('succeeded_count', 0)}, failed={run.get('failed_count', 0)}, "
            f"remaining pending={run.get('remaining_pending_count', 0)}, "
            f"remaining failed={run.get('remaining_failed_count', 0)}."
        )
        if _is_hx_request():
            return _render_catalog_page(
                page=page_number,
                catalog_filters=query_state,
                action_message=success_message,
            )
        return _catalog_action_redirect(
            relative_path="",
            page=page,
            query_state=query_state,
            return_to="catalog",
            action_message=success_message,
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

    # --- Library (grid view) ----------------------------------------------
    # The library page is a visual counterpart to /catalog: a folder tree on
    # the left, a thumbnail grid on the right. It reuses the existing
    # /v1/admin/catalog endpoint (with the new relative_path_prefix filter)
    # plus /v1/admin/catalog/folders to render the tree. It is intentionally
    # a distinct page rather than a mode on /catalog so we can iterate on
    # media-centric interactions (hover popover, lightbox) without disturbing
    # the data-dense admin table used for moderation work.

    def _sanitize_library_prefix(raw_value: str) -> str:
        return sanitize_library_prefix(raw_value)

    def _build_library_folder_tree(
        folders: list[dict[str, Any]], selected_prefix: str
    ) -> list[dict[str, Any]]:
        return build_library_folder_tree(folders, selected_prefix)

    @app.get("/library")
    def library() -> str:
        selected_prefix = _sanitize_library_prefix(
            request.args.get("folder", "")
        )
        raw_page = request.args.get("page", "1")
        try:
            page = max(1, int(raw_page))
        except ValueError:
            page = 1
        # Grid view shows a denser page than the moderation list; 60 plays
        # nicely with 3/4/5-column responsive grids.
        library_page_size = 60
        offset = (page - 1) * library_page_size
        query: dict[str, str] = {
            "limit": str(library_page_size),
            "offset": str(offset),
        }
        if selected_prefix:
            query["relative_path_prefix"] = selected_prefix

        error_message: str | None = None
        try:
            payload = fetcher("/v1/admin/catalog", query)
        except (URLError, TimeoutError, ValueError):
            payload = {
                "total": 0,
                "limit": library_page_size,
                "offset": offset,
                "items": [],
            }
            error_message = "Unable to reach photovault-api catalog endpoint."

        folders_payload: dict[str, Any]
        try:
            folders_payload = fetcher("/v1/admin/catalog/folders", {})
        except (URLError, TimeoutError, ValueError):
            folders_payload = {"folders": []}
            if error_message is None:
                error_message = (
                    "Unable to reach photovault-api catalog folders endpoint."
                )

        # Reject-queue count drives the header badge in library.html. Best-
        # effort fetch: render the page with a zero badge if the API hiccups.
        reject_queue_count = 0
        try:
            rq_payload = fetcher("/v1/admin/catalog/rejects", {"limit": "1", "offset": "0"})
            reject_queue_count = int(rq_payload.get("total", 0))
        except (URLError, TimeoutError, ValueError):
            reject_queue_count = 0

        # Trash count drives the secondary header pill. Same best-effort logic.
        trash_count = 0
        try:
            trash_payload = fetcher(
                "/v1/admin/catalog/tombstones", {"limit": "1", "offset": "0"}
            )
            trash_count = int(trash_payload.get("total", 0))
        except (URLError, TimeoutError, ValueError):
            trash_count = 0

        total = int(payload.get("total", 0))
        items = list(payload.get("items", []))
        for item in items:
            _decorate_catalog_item(item)

        has_previous = page > 1
        has_next = offset + len(items) < total
        start_index = offset + 1 if total > 0 and items else 0
        end_index = offset + len(items)

        folder_entries = _build_library_folder_tree(
            list(folders_payload.get("folders", [])), selected_prefix
        )

        nav_query: dict[str, str] = {}
        if selected_prefix:
            nav_query["folder"] = selected_prefix
        previous_url = (
            url_for("library", page=page - 1, **nav_query) if has_previous else None
        )
        next_url = (
            url_for("library", page=page + 1, **nav_query) if has_next else None
        )

        return render_template(
            "library.html",
            assets=items,
            folders=folder_entries,
            selected_folder=selected_prefix,
            page=page,
            page_size=library_page_size,
            total=total,
            has_previous=has_previous,
            has_next=has_next,
            start_index=start_index,
            end_index=end_index,
            previous_url=previous_url,
            next_url=next_url,
            reject_queue_count=reject_queue_count,
            trash_count=trash_count,
            error_message=error_message,
            active_page="library",
        )

    @app.get("/library/popover")
    def library_popover() -> str:
        """HTMX fragment: metadata + quick actions for a single asset.

        Used by the grid-tile hover/focus overlay. The surrounding tile knows
        the relative_path; we re-fetch the authoritative record so the
        popover is always current (e.g. if the asset was just re-extracted).
        """
        relative_path = request.args.get("relative_path", "").strip()
        if not relative_path:
            return "<div class=\"small text-danger\">Missing relative_path.</div>"
        try:
            payload = fetcher(
                "/v1/admin/catalog/asset", {"relative_path": relative_path}
            )
        except (URLError, TimeoutError, ValueError):
            return (
                "<div class=\"small text-danger\">Unable to load asset details."
                "</div>"
            )
        item = dict(payload.get("item") or {})
        if not item:
            return "<div class=\"small text-danger\">Asset not found.</div>"
        _decorate_catalog_item(item)
        return render_template("_library_popover.html", asset=item)

    @app.get("/library/lightbox")
    def library_lightbox() -> str:
        """HTMX fragment: the expanded-preview modal body.

        The grid renders tiles linearly; the lightbox accepts an index-based
        position within the currently filtered folder so prev/next buttons
        can walk through the page without a round-trip to compute a global
        ordering.
        """
        relative_path = request.args.get("relative_path", "").strip()
        if not relative_path:
            return "<div class=\"small text-danger\">Missing relative_path.</div>"
        selected_prefix = _sanitize_library_prefix(
            request.args.get("folder", "")
        )
        try:
            index = max(0, int(request.args.get("index", "0")))
        except ValueError:
            index = 0
        try:
            total = max(1, int(request.args.get("total", "1")))
        except ValueError:
            total = 1

        # Fetch the single asset so we can render rich metadata in the frame.
        try:
            payload = fetcher(
                "/v1/admin/catalog/asset", {"relative_path": relative_path}
            )
        except (URLError, TimeoutError, ValueError):
            return (
                "<div class=\"small text-danger\">Unable to load asset."
                "</div>"
            )
        item = dict(payload.get("item") or {})
        if not item:
            return "<div class=\"small text-danger\">Asset not found.</div>"
        _decorate_catalog_item(item)
        return render_template(
            "_library_lightbox.html",
            asset=item,
            selected_folder=selected_prefix,
            index=index,
            total=total,
        )

    @app.get("/library/rejects")
    def library_rejects() -> str:
        """Phase 3.C review page. Grid of assets in the reject queue with
        per-row Restore and a top "Delete rejected media" button. Wiring for
        the destructive execute action is complete as of Phase 3.C.
        """

        raw_page = request.args.get("page", "1")
        try:
            page = max(1, int(raw_page))
        except ValueError:
            page = 1
        page_size = 60
        offset = (page - 1) * page_size
        action_message = request.args.get("action_message")

        error_message: str | None = None
        try:
            payload = fetcher(
                "/v1/admin/catalog/rejects",
                {"limit": str(page_size), "offset": str(offset)},
            )
        except (URLError, TimeoutError, ValueError):
            payload = {"total": 0, "limit": page_size, "offset": offset, "items": []}
            error_message = (
                "Unable to reach photovault-api reject-queue endpoint."
            )

        total = int(payload.get("total", 0))
        raw_items = list(payload.get("items") or [])

        # Each API row has {relative_path, sha256_hex, marked_at_utc,
        # marked_reason, item}. We surface the embedded catalog item for the
        # thumbnail + metadata, but keep the queue metadata alongside.
        decorated: list[dict[str, Any]] = []
        for row in raw_items:
            row = dict(row)
            item = row.get("item")
            if isinstance(item, dict):
                _decorate_catalog_item(item)
                # Mirror is_rejected onto the inner item for template symmetry
                item["is_rejected"] = True
                row["item"] = item
            decorated.append(row)

        has_previous = page > 1
        has_next = offset + len(decorated) < total
        previous_url = (
            url_for("library_rejects", page=page - 1) if has_previous else None
        )
        next_url = (
            url_for("library_rejects", page=page + 1) if has_next else None
        )
        start_index = offset + 1 if total > 0 and decorated else 0
        end_index = offset + len(decorated)

        return render_template(
            "library_rejects.html",
            reject_rows=decorated,
            total=total,
            page=page,
            page_size=page_size,
            start_index=start_index,
            end_index=end_index,
            previous_url=previous_url,
            next_url=next_url,
            error_message=error_message,
            action_message=action_message,
            active_page="library",
            suppress_layout_alerts=True,
        )

    @app.post("/library/actions/reject/unmark")
    def library_reject_unmark_action():
        """Restore (un-reject) action used by the /library/rejects page.

        Accepts ``relative_path`` and redirects back to /library/rejects on
        the same page (preserving position when possible). Non-HTMX only —
        the rejects page renders a tight list and re-fetching is cheap.
        """

        relative_path = request.form.get("relative_path", "").strip()
        raw_page = request.form.get("page", "1").strip() or "1"
        try:
            page = max(1, int(raw_page))
        except ValueError:
            page = 1
        if not relative_path:
            return redirect(url_for("library_rejects", page=page))
        try:
            poster("/v1/admin/catalog/reject/unmark", {"relative_path": relative_path})
        except (URLError, TimeoutError, ValueError):
            # Non-fatal; the page will re-fetch and show whatever the API says.
            return redirect(url_for("library_rejects", page=page))
        return redirect(url_for("library_rejects", page=page))

    @app.post("/library/actions/rejects/execute")
    def library_rejects_execute_action():
        """Execute delete action for Phase 3.C reject queue execution.

        Posts to the API's /v1/admin/catalog/rejects/execute endpoint and
        redirects back to /library/rejects with a success message.
        """

        try:
            result = poster("/v1/admin/catalog/rejects/execute", {"relative_paths": None})
            executed_count = len(result.get("executed", []))
            message = f"Deleted {executed_count} asset(s); trash retained for 14 days"
        except (URLError, TimeoutError, ValueError):
            message = "Error executing delete; check server logs"

        return redirect(url_for("library_rejects", page=1, action_message=message))

    # ---------- Phase 3.D: trash triage page --------------------------------

    @app.get("/library/trash")
    def library_trash() -> str:
        """Trash triage page — shows soft-deleted assets still within the
        14-day retention window. Reviewers can restore individual assets or
        leave them to be purged by the cron script.
        """

        raw_page = request.args.get("page", "1")
        try:
            page = max(1, int(raw_page))
        except ValueError:
            page = 1
        page_size = 60
        offset = (page - 1) * page_size
        action_message = request.args.get("action_message")

        error_message: str | None = None
        try:
            payload = fetcher(
                "/v1/admin/catalog/tombstones",
                {"limit": str(page_size), "offset": str(offset)},
            )
        except (URLError, TimeoutError, ValueError):
            payload = {"total": 0, "limit": page_size, "offset": offset, "items": []}
            error_message = "Unable to reach photovault-api tombstones endpoint."

        total = int(payload.get("total", 0))
        raw_items = list(payload.get("items") or [])

        has_previous = page > 1
        has_next = offset + len(raw_items) < total
        previous_url = (
            url_for("library_trash", page=page - 1) if has_previous else None
        )
        next_url = (
            url_for("library_trash", page=page + 1) if has_next else None
        )
        start_index = offset + 1 if total > 0 and raw_items else 0
        end_index = offset + len(raw_items)

        return render_template(
            "library_trash.html",
            trash_rows=raw_items,
            total=total,
            page=page,
            page_size=page_size,
            start_index=start_index,
            end_index=end_index,
            previous_url=previous_url,
            next_url=next_url,
            error_message=error_message,
            action_message=action_message,
            active_page="library",
            suppress_layout_alerts=True,
        )

    @app.post("/library/actions/trash/restore")
    def library_trash_restore_action():
        """Restore action used by /library/trash.

        Posts to the API's /v1/admin/catalog/tombstones/restore endpoint and
        redirects back to /library/trash with a flash message.
        """

        relative_path = request.form.get("relative_path", "").strip()
        raw_page = request.form.get("page", "1").strip() or "1"
        try:
            page = max(1, int(raw_page))
        except ValueError:
            page = 1
        if not relative_path:
            return redirect(url_for("library_trash", page=page))
        try:
            poster("/v1/admin/catalog/tombstones/restore", {"relative_path": relative_path})
            message = f"Restored {relative_path}"
        except (URLError, TimeoutError, ValueError):
            message = f"Error restoring {relative_path}; check server logs"

        return redirect(url_for("library_trash", page=page, action_message=message))


    return app
