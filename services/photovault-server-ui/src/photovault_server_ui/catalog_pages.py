"""Catalog and library helper functions for the server UI."""

from __future__ import annotations

from typing import Any
from urllib.error import URLError
from urllib.parse import urlencode

from flask import redirect, render_template, url_for

from .api_client import ApiFetcher
from .formatters import (
    _catalog_query_state_from_values,
    _decorate_catalog_item,
    _utc_iso_to_local,
)


def catalog_action_redirect(
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


def render_catalog_page(
    fetcher: ApiFetcher,
    *,
    page: int,
    page_size: int,
    is_hx_request: bool,
    catalog_filters: dict[str, str],
    action_message: str | None = None,
    action_error: str | None = None,
) -> str:
    offset = (page - 1) * page_size
    extraction_status_filter = catalog_filters.get("extraction_status", "").strip()
    origin_kind_filter = catalog_filters.get("origin_kind", "").strip()
    media_type_filter = catalog_filters.get("media_type", "").strip()
    preview_capability_filter = catalog_filters.get("preview_capability", "").strip()
    preview_status_filter = catalog_filters.get("preview_status", "").strip()
    is_favorite_filter = catalog_filters.get("is_favorite", "").strip()
    is_archived_filter = catalog_filters.get("is_archived", "").strip()
    cataloged_since_filter = catalog_filters.get("cataloged_since_utc", "").strip()
    cataloged_before_filter = catalog_filters.get("cataloged_before_utc", "").strip()

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
        _decorate_catalog_item(item)

    filter_query = _catalog_query_state_from_values(catalog_filters)
    return_query = urlencode(filter_query)
    previous_url = url_for("catalog", page=page - 1, **filter_query) if has_previous else None
    next_url = url_for("catalog", page=page + 1, **filter_query) if has_next else None

    filter_chip_labels = {
        "extraction_status": "Extraction",
        "preview_status": "Preview",
        "origin_kind": "Origin",
        "media_type": "Media type",
        "preview_capability": "Previewable",
        "is_favorite": "Favorite",
        "is_archived": "Archived",
        "cataloged_since_utc": "Since",
        "cataloged_before_utc": "Before",
    }
    active_filters: list[dict[str, str]] = []
    for key, label in filter_chip_labels.items():
        value = filter_query.get(key, "")
        if not value:
            continue
        remaining = {
            filter_key: filter_value
            for filter_key, filter_value in filter_query.items()
            if filter_key != key
        }
        active_filters.append(
            {
                "key": key,
                "label": label,
                "value": value,
                "remove_url": url_for("catalog", **remaining),
            }
        )

    template_name = "_catalog_content.html" if is_hx_request else "catalog.html"
    return render_template(
        template_name,
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
        cataloged_since_local=_utc_iso_to_local(cataloged_since_filter),
        cataloged_before_local=_utc_iso_to_local(cataloged_before_filter),
        catalog_query_state=filter_query,
        return_query=return_query,
        active_filters=active_filters,
        latest_backfill_runs=latest_backfill_runs,
        previous_url=previous_url,
        next_url=next_url,
        active_page="catalog",
        suppress_layout_alerts=True,
    )


def sanitize_library_prefix(raw_value: str) -> str:
    """Return a normalized folder prefix, or ``""`` if invalid/empty."""

    value = (raw_value or "").strip()
    if value == "" or value.startswith("/") or "\\" in value:
        return ""
    trimmed = value.strip("/")
    if trimmed == "":
        return ""
    for segment in trimmed.split("/"):
        if segment in ("", ".", ".."):
            return ""
    return trimmed


def build_library_folder_tree(
    folders: list[dict[str, Any]],
    selected_prefix: str,
) -> list[dict[str, Any]]:
    """Shape folder rows from the API into a tree-friendly list."""

    normalized_selected = selected_prefix.strip("/")
    selected_ancestors: set[str] = set()
    if normalized_selected:
        parts = normalized_selected.split("/")
        for index in range(1, len(parts) + 1):
            selected_ancestors.add("/".join(parts[:index]))
    entries: list[dict[str, Any]] = []
    for folder in folders:
        path = str(folder.get("path", ""))
        if not path:
            continue
        display_name = path.rsplit("/", 1)[-1]
        entries.append(
            {
                "path": path,
                "depth": int(folder.get("depth", 0)),
                "direct_count": int(folder.get("direct_count", 0)),
                "total_count": int(folder.get("total_count", 0)),
                "display_name": display_name,
                "is_selected": path == normalized_selected,
                "is_ancestor_of_selected": path in selected_ancestors and path != normalized_selected,
            }
        )
    return entries
