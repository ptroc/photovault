"""Page render helpers for the client UI."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import httpx
from flask import Response, abort, make_response, render_template, url_for

from .constants import DEFAULT_SERVER_API_URL


@dataclass(slots=True)
class ClientUiRenderer:
    daemon_base_url: str
    daemon_get: Callable[[str, str], Any]
    load_daemon_context: Callable[..., dict[str, Any]]
    load_selected_job: Callable[[int | None], tuple[dict[str, Any] | None, str | None]]
    load_network_context: Callable[..., dict[str, Any]]
    dependency_snapshot_get: Callable[..., list[dict[str, str]]]
    interface_addresses_get: Callable[[], list[dict[str, Any]]]
    annotate_job_record: Callable[[dict[str, Any]], dict[str, Any]]
    block_partition_ingest_prefill: Callable[[dict[str, Any]], dict[str, str]]
    build_ingest_gate: Callable[[dict[str, Any] | None], dict[str, Any]]
    build_overview_metrics: Callable[..., dict[str, Any]]
    derive_daemon_progress_view: Callable[[dict[str, Any] | None], dict[str, Any]]
    derive_state_guidance: Callable[[dict[str, Any] | None, str | None], dict[str, str]]
    filter_jobs: Callable[[list[dict[str, Any]], str], list[dict[str, Any]]]
    format_size_bytes: Callable[[object], str]
    summarize_recent_events: Callable[[list[dict[str, Any]]], dict[str, Any]]
    is_ajax_request: Callable[[], bool]

    def render_template_response(
        self,
        *,
        page_template: str,
        fragment_template: str,
        context: dict[str, Any],
        location: str,
    ) -> Response:
        template_name = fragment_template if self.is_ajax_request() else page_template
        response = make_response(render_template(template_name, **context))
        response.headers["X-Client-Location"] = location
        return response

    def render_overview(
        self,
        *,
        ingest_error: str | None = None,
        ingest_error_detail: str | None = None,
        ingest_notice: str | None = None,
        ingest_notice_detail: str | None = None,
        operator_notice: str | None = None,
        operator_notice_pending: bool = False,
        auto_refresh_ms: int | None = None,
        form_data: dict[str, str] | None = None,
    ) -> Response:
        ingest_form = {"media_label": "", "source_paths": ""}
        if form_data:
            ingest_form.update(form_data)
        context = self.load_daemon_context()
        context["jobs"] = [self.annotate_job_record(job) for job in context["jobs"]]
        server_base_url_from_daemon = (
            context["state"].get("server_base_url", "") if isinstance(context.get("state"), dict) else ""
        ) or DEFAULT_SERVER_API_URL
        dependencies = self.dependency_snapshot_get(server_api_url=server_base_url_from_daemon)
        ingest_gate = self.build_ingest_gate(context["state"])
        overview_metrics = self.build_overview_metrics(
            jobs=context["jobs"],
            state=context["state"],
            daemon_error=context["daemon_error"],
            diagnostics=context["diagnostics"],
            dependencies=dependencies,
            events=context["events"],
        )
        interface_addresses = self.interface_addresses_get()
        context.update(
            {
                "dependencies": dependencies,
                "interface_addresses": interface_addresses,
                "overview_metrics": overview_metrics,
                "daemon_progress": self.derive_daemon_progress_view(context["state"]),
                "daemon_base_url": self.daemon_base_url,
                "ingest_error": ingest_error,
                "ingest_error_detail": ingest_error_detail,
                "ingest_notice": ingest_notice,
                "ingest_notice_detail": ingest_notice_detail,
                "operator_notice": operator_notice,
                "operator_notice_pending": operator_notice_pending,
                "auto_refresh_ms": auto_refresh_ms,
                "auto_refresh_path": url_for("index"),
                "ingest_form": ingest_form,
                "ingest_gate": ingest_gate,
                "active_page": "overview",
            }
        )
        template_name = "_overview_content.html" if self.is_ajax_request() else "overview.html"
        response = make_response(render_template(template_name, **context))
        response.headers["X-Client-Location"] = url_for("index")
        return response

    def render_jobs(self, *, selected_filter: str = "active") -> Response:
        context = self.load_daemon_context()
        jobs = [self.annotate_job_record(job) for job in context["jobs"]]
        effective_filter = (
            selected_filter
            if selected_filter in {"active", "waiting", "blocked", "completed", "all"}
            else "active"
        )
        filtered_jobs = self.filter_jobs(jobs, effective_filter)
        context.update(
            {
                "daemon_base_url": self.daemon_base_url,
                "jobs": filtered_jobs,
                "job_filter": effective_filter,
                "job_filter_counts": {
                    "active": len(self.filter_jobs(jobs, "active")),
                    "waiting": len(self.filter_jobs(jobs, "waiting")),
                    "blocked": len(self.filter_jobs(jobs, "blocked")),
                    "completed": len(self.filter_jobs(jobs, "completed")),
                    "all": len(jobs),
                },
                "active_page": "jobs",
            }
        )
        return self.render_template_response(
            page_template="jobs.html",
            fragment_template="_jobs_content.html",
            context=context,
            location=url_for("jobs_page", filter=effective_filter),
        )

    def render_job_detail(
        self,
        job_id: int,
        *,
        action_error: str | None = None,
        action_notice: str | None = None,
        action_notice_pending: bool = False,
    ) -> Response:
        context = self.load_daemon_context()
        selected_job, selected_job_error = self.load_selected_job(job_id)
        if selected_job is None:
            if selected_job_error == f"job_id {job_id} not found":
                abort(404, description=selected_job_error)
            if context["daemon_error"] is None:
                context["daemon_error"] = selected_job_error
            return self.render_jobs(selected_filter="all")

        selected_job = self.annotate_job_record(selected_job)
        job_events: list[dict[str, Any]] = []
        for event in context["events"]:
            message = str(event.get("message", ""))
            if f"job_id={job_id}" in message:
                job_events.append(event)
        if not job_events:
            job_events = context["events"][:4]
        context.update(
            {
                "daemon_base_url": self.daemon_base_url,
                "daemon_progress": self.derive_daemon_progress_view(context["state"]),
                "state_guidance": self.derive_state_guidance(
                    context["state"], context["daemon_error"]
                ),
                "job_events": job_events[:4],
                "selected_job": selected_job,
                "active_page": "jobs",
                "action_error": action_error,
                "action_notice": action_notice,
                "action_notice_pending": action_notice_pending,
            }
        )
        return self.render_template_response(
            page_template="job_detail.html",
            fragment_template="_job_detail_content.html",
            context=context,
            location=url_for("job_detail", job_id=job_id),
        )

    def render_events(self) -> str:
        context = self.load_daemon_context(events_limit=30)
        context.update(
            {
                "daemon_base_url": self.daemon_base_url,
                "event_summary": self.summarize_recent_events(context["events"]),
                "state_guidance": self.derive_state_guidance(context["state"], context["daemon_error"]),
                "active_page": "events",
            }
        )
        return render_template("events.html", **context)

    def render_network(
        self,
        *,
        network_error: str | None = None,
        network_notice: str | None = None,
        ap_form_data: dict[str, str] | None = None,
        sta_form_data: dict[str, str] | None = None,
    ) -> Response:
        context = self.load_daemon_context()
        context.update(
            self.load_network_context(
                network_error=network_error,
                network_notice=network_notice,
                ap_form_data=ap_form_data,
                sta_form_data=sta_form_data,
            )
        )
        context.update(
            {
                "daemon_base_url": self.daemon_base_url,
                "active_page": "network",
            }
        )
        return self.render_template_response(
            page_template="network.html",
            fragment_template="_network_content.html",
            context=context,
            location=url_for("network_page"),
        )

    def portal_recheck_notice(self, snapshot_payload: object, connectivity_check: object) -> str:
        status = ""
        if isinstance(snapshot_payload, dict):
            status = str(snapshot_payload.get("upstream_status", "")).strip()
        check_label = str(connectivity_check or "unknown").strip() or "unknown"
        if status == "internet_reachable":
            return (
                "Rechecked upstream connectivity: Internet is reachable now. "
                f"NetworkManager check={check_label}."
            )
        if status == "captive_portal_likely":
            return (
                "Rechecked upstream connectivity: captive portal still likely. "
                "Finish upstream login in an external browser, then recheck again. "
                f"NetworkManager check={check_label}."
            )
        if status == "no_usable_internet":
            return (
                "Rechecked upstream connectivity: upstream Wi-Fi is connected but Internet remains unusable. "
                f"NetworkManager check={check_label}."
            )
        return f"Rechecked upstream connectivity. NetworkManager check={check_label}."

    def render_block_devices(
        self,
        *,
        block_device_error: str | None = None,
        block_device_error_detail: str | None = None,
        block_device_notice: str | None = None,
    ) -> Response:
        context = self.load_daemon_context()
        devices: list[dict[str, Any]] = []
        try:
            payload = self.daemon_get(self.daemon_base_url, "/block-devices")
            raw_devices = payload.get("devices", [])
            if isinstance(raw_devices, list):
                for disk in raw_devices:
                    if not isinstance(disk, dict):
                        continue
                    disk_copy = dict(disk)
                    disk_copy["size_label"] = self.format_size_bytes(disk_copy.get("size_bytes"))
                    partitions: list[dict[str, Any]] = []
                    raw_partitions = disk_copy.get("partitions")
                    if isinstance(raw_partitions, list):
                        for partition in raw_partitions:
                            if not isinstance(partition, dict):
                                continue
                            partition_copy = dict(partition)
                            partition_copy["size_label"] = self.format_size_bytes(
                                partition_copy.get("size_bytes")
                            )
                            partition_copy["ingest_prefill"] = self.block_partition_ingest_prefill(
                                partition_copy
                            )
                            partitions.append(partition_copy)
                    disk_copy["partitions"] = partitions
                    devices.append(disk_copy)
        except httpx.HTTPError as exc:
            if block_device_error is None:
                block_device_error = "Failed to load block-device inventory."
                from .api_client import _describe_http_error

                block_device_error_detail = _describe_http_error(exc)

        context.update(
            {
                "daemon_base_url": self.daemon_base_url,
                "devices": devices,
                "block_device_error": block_device_error,
                "block_device_error_detail": block_device_error_detail,
                "block_device_notice": block_device_notice,
                "active_page": "block_devices",
            }
        )
        return self.render_template_response(
            page_template="block_devices.html",
            fragment_template="_block_devices_content.html",
            context=context,
            location=url_for("block_devices_page"),
        )
