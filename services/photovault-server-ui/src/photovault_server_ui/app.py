"""SSR monitoring UI for the photovault server."""

from __future__ import annotations

import json
import os
from typing import Any, Callable
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from flask import Flask, render_template, request

ApiFetcher = Callable[[str, dict[str, str]], dict[str, Any]]


def _default_api_fetcher(path: str, query: dict[str, str]) -> dict[str, Any]:
    base_url = os.getenv("PHOTOVAULT_SERVER_UI_API_BASE_URL", "http://127.0.0.1:9301")
    query_suffix = f"?{urlencode(query)}" if query else ""
    url = f"{base_url}{path}{query_suffix}"
    req = Request(url=url, method="GET")
    with urlopen(req, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def _format_size_bytes(value: int) -> str:
    if value < 1024:
        return f"{value} B"
    if value < 1024 * 1024:
        return f"{value / 1024:.1f} KiB"
    if value < 1024 * 1024 * 1024:
        return f"{value / (1024 * 1024):.1f} MiB"
    return f"{value / (1024 * 1024 * 1024):.1f} GiB"


def create_app(*, api_fetcher: ApiFetcher | None = None) -> Flask:
    app = Flask(__name__)
    fetcher = api_fetcher or _default_api_fetcher
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

    return app
