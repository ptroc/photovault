"""Daemon and network context loaders for the client UI."""

from collections.abc import Callable
from typing import Any

import httpx
from flask import request

from .api_client import _describe_http_error


def is_ajax_request() -> bool:
    return request.headers.get("X-Requested-With") == "XMLHttpRequest"


def load_daemon_context(
    daemon_get: Callable[[str, str], Any],
    daemon_base_url: str,
    *,
    events_limit: int = 10,
) -> dict[str, Any]:
    context: dict[str, Any] = {
        "daemon_error": None,
        "daemon_error_detail": None,
        "state": None,
        "diagnostics": None,
        "jobs": [],
        "events": [],
    }
    try:
        context["state"] = daemon_get(daemon_base_url, "/state")
        context["diagnostics"] = daemon_get(daemon_base_url, "/diagnostics/m0")
        jobs_payload = daemon_get(daemon_base_url, "/ingest/jobs")
        events_payload = daemon_get(daemon_base_url, f"/events?limit={events_limit}")
        context["jobs"] = list(jobs_payload.get("jobs", []))
        context["events"] = list(events_payload.get("events", []))
    except httpx.HTTPError as exc:
        context["daemon_error"] = (
            "Unable to reach the local daemon API. Check photovault-clientd.service and try refresh."
        )
        context["daemon_error_detail"] = _describe_http_error(exc)
    return context


def load_selected_job(
    daemon_get: Callable[[str, str], Any],
    daemon_base_url: str,
    job_id: int | None,
) -> tuple[dict[str, Any] | None, str | None]:
    if job_id is None:
        return None, None
    try:
        return daemon_get(daemon_base_url, f"/ingest/jobs/{job_id}"), None
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            return None, f"job_id {job_id} not found"
        return None, str(exc)
    except httpx.HTTPError as exc:
        return None, str(exc)


def load_network_context(
    daemon_get: Callable[[str, str], Any],
    daemon_base_url: str,
    *,
    network_error: str | None = None,
    network_notice: str | None = None,
    ap_form_data: dict[str, str] | None = None,
    sta_form_data: dict[str, str] | None = None,
) -> dict[str, Any]:
    context: dict[str, Any] = {
        "network_snapshot": None,
        "network_ap_config": None,
        "network_error": network_error,
        "network_notice": network_notice,
        "ap_form": {"ssid": "", "password": ""},
        "sta_form": {"ssid": "", "password": ""},
    }
    if ap_form_data:
        context["ap_form"].update(ap_form_data)
    if sta_form_data:
        context["sta_form"].update(sta_form_data)
    try:
        status_payload = daemon_get(daemon_base_url, "/network/status")
        context["network_snapshot"] = status_payload.get("snapshot")
        context["network_ap_config"] = status_payload.get("ap_config")
        ap_config_payload = daemon_get(daemon_base_url, "/network/ap-config")
        if context["network_ap_config"] is None:
            context["network_ap_config"] = ap_config_payload
        context["ap_form"]["ssid"] = str(ap_config_payload.get("ssid", "")).strip()
    except httpx.HTTPError as exc:
        if context["network_error"] is None:
            context["network_error"] = f"Failed to load network status: {_describe_http_error(exc)}"
    return context
