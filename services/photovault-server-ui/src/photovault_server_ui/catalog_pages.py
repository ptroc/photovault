"""Catalog and library helper functions for the server UI."""

from __future__ import annotations

from typing import Any
from urllib.error import URLError
from urllib.parse import urlencode

from flask import redirect, render_template, url_for

from .api_client import ApiFetcher
from .formatters import (
    _catalog_query_state_from_values,
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
    is_hx_request: bool,
    catalog_filters: dict[str, str],
    action_message: str | None = None,
    action_error: str | None = None,
) -> str:
    origin_kind_filter = catalog_filters.get("origin_kind", "").strip()
    media_type_filter = catalog_filters.get("media_type", "").strip()
    preview_capability_filter = catalog_filters.get("preview_capability", "").strip()
    cataloged_since_filter = catalog_filters.get("cataloged_since_utc", "").strip()
    cataloged_before_filter = catalog_filters.get("cataloged_before_utc", "").strip()

    error_message: str | None = None
    latest_backfill_runs: dict[str, Any] = {"extraction_run": None, "preview_run": None}
    try:
        latest_backfill_runs = fetcher("/v1/admin/catalog/backfill/latest", {})
    except (URLError, TimeoutError, ValueError):
        error_message = "Unable to reach photovault-api catalog backfill endpoint."

    filter_query = _catalog_query_state_from_values(catalog_filters)
    return_query = urlencode(filter_query)

    template_name = "_catalog_content.html" if is_hx_request else "catalog.html"
    return render_template(
        template_name,
        page=page,
        error_message=error_message,
        action_message=action_message,
        action_error=action_error,
        origin_kind_filter=origin_kind_filter,
        media_type_filter=media_type_filter,
        preview_capability_filter=preview_capability_filter,
        cataloged_since_filter=cataloged_since_filter,
        cataloged_before_filter=cataloged_before_filter,
        cataloged_since_local=_utc_iso_to_local(cataloged_since_filter),
        cataloged_before_local=_utc_iso_to_local(cataloged_before_filter),
        catalog_query_state=filter_query,
        return_query=return_query,
        latest_backfill_runs=latest_backfill_runs,
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
