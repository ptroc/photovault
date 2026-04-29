"""Client and insight page helpers for the server UI."""

from __future__ import annotations

from urllib.error import URLError

from flask import render_template, request, url_for

from .api_client import ApiFetcher
from .formatters import (
    _count_client_summary,
    _fetch_catalog_asset_for_display,
    _format_sha_for_display,
)


def is_hx_request() -> bool:
    return request.headers.get("HX-Request", "").lower() == "true"


def render_clients_page(
    fetcher: ApiFetcher,
    *,
    page: int,
    clients_query_state: dict[str, str],
    page_size: int,
    action_message: str | None = None,
    action_error: str | None = None,
    include_sort_by: bool = False,
    include_sort_order: bool = False,
) -> str:
    offset = (page - 1) * page_size
    error_message: str | None = None
    query: dict[str, str] = {"limit": str(page_size), "offset": str(offset)}
    if clients_query_state.get("presence_status", ""):
        query["presence_status"] = clients_query_state["presence_status"]
    if clients_query_state.get("workload_status", ""):
        query["workload_status"] = clients_query_state["workload_status"]
    if clients_query_state.get("enrollment_status", ""):
        query["enrollment_status"] = clients_query_state["enrollment_status"]
    if include_sort_by and clients_query_state.get("sort_by", "").strip():
        query["sort_by"] = clients_query_state["sort_by"]
    if include_sort_order and clients_query_state.get("sort_order", "").strip():
        query["sort_order"] = clients_query_state["sort_order"]
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
        url_for("clients", page=page - 1, **clients_query_state) if has_previous else None
    )
    next_url = url_for("clients", page=page + 1, **clients_query_state) if has_next else None
    template_name = "_clients_content.html" if is_hx_request() else "clients.html"
    return render_template(
        template_name,
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
        suppress_layout_alerts=True,
    )


def render_duplicates_page(
    fetcher: ApiFetcher,
    *,
    insight_page_size: int,
    action_message: str | None = None,
    action_error: str | None = None,
) -> str:
    error_message: str | None = None
    try:
        payload = fetcher("/v1/admin/duplicates", {"limit": str(insight_page_size), "offset": "0"})
    except (URLError, TimeoutError, ValueError):
        payload = {"total": 0, "limit": insight_page_size, "offset": 0, "items": []}
        error_message = "Unable to reach photovault-api duplicates endpoint."

    groups = list(payload.get("items", []))
    for group in groups:
        group["sha256_display"] = _format_sha_for_display(str(group.get("sha256_hex") or ""))
        group["assets"] = [
            _fetch_catalog_asset_for_display(fetcher, relative_path)
            for relative_path in list(group.get("relative_paths") or [])
        ]
    template_name = "_duplicates_content.html" if is_hx_request() else "duplicates.html"
    return render_template(
        template_name,
        groups=groups,
        total=int(payload.get("total", 0)),
        error_message=error_message,
        action_message=action_message,
        action_error=action_error,
        active_page="duplicates",
        suppress_layout_alerts=True,
    )


def render_conflicts_page(fetcher: ApiFetcher, *, insight_page_size: int) -> str:
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
