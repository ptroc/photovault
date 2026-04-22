"""SSR monitoring UI for the photovault server."""

from __future__ import annotations

import json
import os
from typing import Any, Callable
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from flask import Flask, redirect, render_template, request, url_for

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


def create_app(*, api_fetcher: ApiFetcher | None = None, api_poster: ApiPoster | None = None) -> Flask:
    app = Flask(__name__)
    fetcher = api_fetcher or _default_api_fetcher
    poster = api_poster or _default_api_poster
    page_size = 50
    insight_page_size = 25

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

        return render_template(
            "conflicts.html",
            conflicts=list(payload.get("items", [])),
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
        cataloged_since_filter = request.args.get("cataloged_since_utc", "").strip()
        cataloged_before_filter = request.args.get("cataloged_before_utc", "").strip()
        action_message = request.args.get("action_message")
        action_error = request.args.get("action_error")
        error_message: str | None = None
        query: dict[str, str] = {"limit": str(page_size), "offset": str(offset)}
        if extraction_status_filter:
            query["extraction_status"] = extraction_status_filter
        if origin_kind_filter:
            query["origin_kind"] = origin_kind_filter
        if cataloged_since_filter:
            query["cataloged_since_utc"] = cataloged_since_filter
        if cataloged_before_filter:
            query["cataloged_before_utc"] = cataloged_before_filter
        try:
            payload = fetcher("/v1/admin/catalog", query)
        except (URLError, TimeoutError, ValueError):
            payload = {"total": 0, "limit": page_size, "offset": offset, "items": []}
            error_message = "Unable to reach photovault-api catalog endpoint."

        total = int(payload.get("total", 0))
        items = list(payload.get("items", []))
        has_previous = page > 1
        has_next = offset + len(items) < total
        start_index = offset + 1 if total > 0 and items else 0
        end_index = offset + len(items)

        for item in items:
            size_bytes = int(item.get("size_bytes", 0))
            item["size_human"] = _format_size_bytes(size_bytes)
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
            item["metadata_summary"] = " | ".join(metadata_bits)

        filter_query: dict[str, str] = {}
        if extraction_status_filter:
            filter_query["extraction_status"] = extraction_status_filter
        if origin_kind_filter:
            filter_query["origin_kind"] = origin_kind_filter
        if cataloged_since_filter:
            filter_query["cataloged_since_utc"] = cataloged_since_filter
        if cataloged_before_filter:
            filter_query["cataloged_before_utc"] = cataloged_before_filter
        filter_query_suffix = f"&{urlencode(filter_query)}" if filter_query else ""

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
            cataloged_since_filter=cataloged_since_filter,
            cataloged_before_filter=cataloged_before_filter,
            filter_query_suffix=filter_query_suffix,
            active_page="catalog",
        )

    @app.post("/catalog/actions/retry")
    def catalog_retry_action():
        relative_path = request.form.get("relative_path", "").strip()
        page = request.form.get("page", "1")
        extraction_status_filter = request.form.get("extraction_status", "").strip()
        origin_kind_filter = request.form.get("origin_kind", "").strip()
        cataloged_since_filter = request.form.get("cataloged_since_utc", "").strip()
        cataloged_before_filter = request.form.get("cataloged_before_utc", "").strip()
        redirect_query: dict[str, str] = {"page": page}
        if extraction_status_filter:
            redirect_query["extraction_status"] = extraction_status_filter
        if origin_kind_filter:
            redirect_query["origin_kind"] = origin_kind_filter
        if cataloged_since_filter:
            redirect_query["cataloged_since_utc"] = cataloged_since_filter
        if cataloged_before_filter:
            redirect_query["cataloged_before_utc"] = cataloged_before_filter
        if not relative_path:
            return redirect(
                url_for("catalog", **redirect_query, action_error="Missing catalog relative path for retry.")
            )
        try:
            poster("/v1/admin/catalog/extraction/retry", {"relative_path": relative_path})
        except (URLError, TimeoutError, ValueError):
            return redirect(
                url_for("catalog", **redirect_query, action_error=f"Retry failed for {relative_path}.")
            )
        return redirect(
            url_for("catalog", **redirect_query, action_message=f"Retried extraction for {relative_path}.")
        )

    @app.post("/catalog/actions/backfill")
    def catalog_backfill_action():
        page = request.form.get("page", "1")
        extraction_status_filter = request.form.get("extraction_status", "").strip()
        origin_kind_filter = request.form.get("origin_kind", "").strip()
        cataloged_since_filter = request.form.get("cataloged_since_utc", "").strip()
        cataloged_before_filter = request.form.get("cataloged_before_utc", "").strip()
        redirect_query: dict[str, str] = {"page": page}
        if extraction_status_filter:
            redirect_query["extraction_status"] = extraction_status_filter
        if origin_kind_filter:
            redirect_query["origin_kind"] = origin_kind_filter
        if cataloged_since_filter:
            redirect_query["cataloged_since_utc"] = cataloged_since_filter
        if cataloged_before_filter:
            redirect_query["cataloged_before_utc"] = cataloged_before_filter
        raw_statuses = request.form.get("target_statuses", "pending,failed")
        statuses = [part.strip() for part in raw_statuses.split(",") if part.strip()]
        try:
            limit = max(1, min(500, int(request.form.get("limit", "100"))))
        except ValueError:
            limit = 100

        try:
            response = poster(
                "/v1/admin/catalog/extraction/backfill",
                {"target_statuses": statuses or ["pending", "failed"], "limit": limit},
            )
        except (URLError, TimeoutError, ValueError):
            return redirect(
                url_for("catalog", **redirect_query, action_error="Catalog extraction backfill failed.")
            )

        processed_count = int(response.get("processed_count", 0))
        succeeded_count = int(response.get("succeeded_count", 0))
        failed_count = int(response.get("failed_count", 0))
        return redirect(
            url_for(
                "catalog",
                **redirect_query,
                action_message=(
                    f"Backfill processed {processed_count} asset(s): "
                    f"{succeeded_count} succeeded, {failed_count} failed."
                ),
            )
        )

    return app
