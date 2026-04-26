"""SSR control-plane UI for the photovault client."""
from collections.abc import Callable
from typing import Any

import httpx
from flask import Flask, Response, abort, make_response, redirect, render_template, request, url_for

from .api_client import (
    _daemon_get,
    _daemon_post,
    _daemon_put,
    _describe_http_error,
    _format_ingest_source_validation_error,
)
from .constants import (
    DEFAULT_DAEMON_BASE_URL,
    DEFAULT_SERVER_API_URL,
    DEFAULT_TICK_STATUS_REFRESH_MS,
    DEFAULT_TICK_TIMEOUT_SECONDS,
)
from .system import _get_dependency_snapshot, _get_interface_addresses
from .view_models import (
    _annotate_job_record,
    _block_partition_ingest_prefill,
    _build_ingest_gate,
    _build_overview_metrics,
    _derive_daemon_progress_view,
    _derive_state_guidance,
    _filter_jobs,
    _format_size_bytes,
    _summarize_recent_events,
)


def create_app(
    daemon_base_url: str = DEFAULT_DAEMON_BASE_URL,
    daemon_get: Callable[[str, str], Any] = _daemon_get,
    daemon_post: Callable[[str, str, dict[str, Any]], Any] = _daemon_post,
    daemon_put: Callable[[str, str, dict[str, Any]], Any] = _daemon_put,
    network_snapshot_get: Callable[[], dict[str, Any]] | None = None,
    network_connect: Callable[[str, str | None], None] | None = None,
    network_scan: Callable[[], None] | None = None,
    dependency_snapshot_get: Callable[..., list[dict[str, str]]] = _get_dependency_snapshot,
    interface_addresses_get: Callable[[], list[dict[str, Any]]] = _get_interface_addresses,
) -> Flask:
    app = Flask(__name__)

    def _is_ajax_request() -> bool:
        return request.headers.get("X-Requested-With") == "XMLHttpRequest"

    def _load_daemon_context(*, events_limit: int = 10) -> dict[str, Any]:
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

    def _load_selected_job(job_id: int | None) -> tuple[dict[str, Any] | None, str | None]:
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

    def _load_network_context(
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

    def _render_overview(
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
        context = _load_daemon_context()
        context["jobs"] = [_annotate_job_record(job) for job in context["jobs"]]
        server_base_url_from_daemon = (
            context["state"].get("server_base_url", "") if isinstance(context.get("state"), dict) else ""
        ) or DEFAULT_SERVER_API_URL
        dependencies = dependency_snapshot_get(server_api_url=server_base_url_from_daemon)
        ingest_gate = _build_ingest_gate(context["state"])
        overview_metrics = _build_overview_metrics(
            jobs=context["jobs"],
            state=context["state"],
            daemon_error=context["daemon_error"],
            diagnostics=context["diagnostics"],
            dependencies=dependencies,
            events=context["events"],
        )
        interface_addresses = interface_addresses_get()
        context.update(
            {
                "dependencies": dependencies,
                "interface_addresses": interface_addresses,
                "overview_metrics": overview_metrics,
                "daemon_progress": _derive_daemon_progress_view(context["state"]),
                "daemon_base_url": daemon_base_url,
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
        template_name = "_overview_content.html" if _is_ajax_request() else "overview.html"
        response = make_response(render_template(template_name, **context))
        response.headers["X-Client-Location"] = url_for("index")
        return response

    def _render_template_response(
        *,
        page_template: str,
        fragment_template: str,
        context: dict[str, Any],
        location: str,
    ) -> Response:
        template_name = fragment_template if _is_ajax_request() else page_template
        response = make_response(render_template(template_name, **context))
        response.headers["X-Client-Location"] = location
        return response

    def _render_jobs(*, selected_filter: str = "active") -> Response:
        context = _load_daemon_context()
        jobs = [_annotate_job_record(job) for job in context["jobs"]]
        effective_filter = (
            selected_filter
            if selected_filter in {"active", "waiting", "blocked", "completed", "all"}
            else "active"
        )
        filtered_jobs = _filter_jobs(jobs, effective_filter)
        context.update(
            {
                "daemon_base_url": daemon_base_url,
                "jobs": filtered_jobs,
                "job_filter": effective_filter,
                "job_filter_counts": {
                    "active": len(_filter_jobs(jobs, "active")),
                    "waiting": len(_filter_jobs(jobs, "waiting")),
                    "blocked": len(_filter_jobs(jobs, "blocked")),
                    "completed": len(_filter_jobs(jobs, "completed")),
                    "all": len(jobs),
                },
                "active_page": "jobs",
            }
        )
        return _render_template_response(
            page_template="jobs.html",
            fragment_template="_jobs_content.html",
            context=context,
            location=url_for("jobs_page", filter=effective_filter),
        )

    def _render_job_detail(
        job_id: int,
        *,
        action_error: str | None = None,
        action_notice: str | None = None,
        action_notice_pending: bool = False,
    ) -> Response:
        context = _load_daemon_context()
        selected_job, selected_job_error = _load_selected_job(job_id)
        if selected_job is None:
            if selected_job_error == f"job_id {job_id} not found":
                abort(404, description=selected_job_error)
            if context["daemon_error"] is None:
                context["daemon_error"] = selected_job_error
            return _render_jobs(selected_filter="all")

        selected_job = _annotate_job_record(selected_job)
        job_events: list[dict[str, Any]] = []
        for event in context["events"]:
            message = str(event.get("message", ""))
            if f"job_id={job_id}" in message:
                job_events.append(event)
        if not job_events:
            job_events = context["events"][:4]
        context.update(
            {
                "daemon_base_url": daemon_base_url,
                "daemon_progress": _derive_daemon_progress_view(context["state"]),
                "state_guidance": _derive_state_guidance(context["state"], context["daemon_error"]),
                "job_events": job_events[:4],
                "selected_job": selected_job,
                "active_page": "jobs",
                "action_error": action_error,
                "action_notice": action_notice,
                "action_notice_pending": action_notice_pending,
            }
        )
        return _render_template_response(
            page_template="job_detail.html",
            fragment_template="_job_detail_content.html",
            context=context,
            location=url_for("job_detail", job_id=job_id),
        )

    def _render_events() -> str:
        context = _load_daemon_context(events_limit=30)
        context.update(
            {
                "daemon_base_url": daemon_base_url,
                "event_summary": _summarize_recent_events(context["events"]),
                "state_guidance": _derive_state_guidance(context["state"], context["daemon_error"]),
                "active_page": "events",
            }
        )
        return render_template("events.html", **context)

    def _render_network(
        *,
        network_error: str | None = None,
        network_notice: str | None = None,
        ap_form_data: dict[str, str] | None = None,
        sta_form_data: dict[str, str] | None = None,
    ) -> Response:
        context = _load_daemon_context()
        context.update(
            _load_network_context(
                network_error=network_error,
                network_notice=network_notice,
                ap_form_data=ap_form_data,
                sta_form_data=sta_form_data,
            )
        )
        context.update(
            {
                "daemon_base_url": daemon_base_url,
                "active_page": "network",
            }
        )
        return _render_template_response(
            page_template="network.html",
            fragment_template="_network_content.html",
            context=context,
            location=url_for("network_page"),
        )

    def _portal_recheck_notice(snapshot_payload: object, connectivity_check: object) -> str:
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

    def _render_block_devices(
        *,
        block_device_error: str | None = None,
        block_device_error_detail: str | None = None,
        block_device_notice: str | None = None,
    ) -> Response:
        context = _load_daemon_context()
        devices: list[dict[str, Any]] = []
        try:
            payload = daemon_get(daemon_base_url, "/block-devices")
            raw_devices = payload.get("devices", [])
            if isinstance(raw_devices, list):
                for disk in raw_devices:
                    if not isinstance(disk, dict):
                        continue
                    disk_copy = dict(disk)
                    disk_copy["size_label"] = _format_size_bytes(disk_copy.get("size_bytes"))
                    partitions: list[dict[str, Any]] = []
                    raw_partitions = disk_copy.get("partitions")
                    if isinstance(raw_partitions, list):
                        for partition in raw_partitions:
                            if not isinstance(partition, dict):
                                continue
                            partition_copy = dict(partition)
                            partition_copy["size_label"] = _format_size_bytes(
                                partition_copy.get("size_bytes")
                            )
                            partition_copy["ingest_prefill"] = _block_partition_ingest_prefill(
                                partition_copy
                            )
                            partitions.append(partition_copy)
                    disk_copy["partitions"] = partitions
                    devices.append(disk_copy)
        except httpx.HTTPError as exc:
            if block_device_error is None:
                block_device_error = "Failed to load block-device inventory."
                block_device_error_detail = _describe_http_error(exc)

        context.update(
            {
                "daemon_base_url": daemon_base_url,
                "devices": devices,
                "block_device_error": block_device_error,
                "block_device_error_detail": block_device_error_detail,
                "block_device_notice": block_device_notice,
                "active_page": "block_devices",
            }
        )
        return _render_template_response(
            page_template="block_devices.html",
            fragment_template="_block_devices_content.html",
            context=context,
            location=url_for("block_devices_page"),
        )

    @app.get("/")
    def index() -> Response:
        return _render_overview()

    @app.get("/jobs")
    def jobs_page() -> Response:
        return _render_jobs(selected_filter=request.args.get("filter", "active"))

    @app.get("/events")
    def events_page() -> str:
        return _render_events()

    @app.get("/network")
    def network_page() -> Response:
        return _render_network()

    @app.get("/block-devices")
    def block_devices_page() -> Response:
        return _render_block_devices()

    @app.post("/ingest/jobs")
    def create_ingest_job() -> Any:
        media_label = request.form.get("media_label", "").strip()
        source_paths_text = request.form.get("source_paths", "")
        source_paths = [line.strip() for line in source_paths_text.splitlines() if line.strip()]
        normalized_source_path = source_paths[0] if len(source_paths) == 1 else source_paths_text.strip()
        form_data = {"media_label": media_label, "source_paths": normalized_source_path}

        if not media_label:
            return _render_overview(ingest_error="Media label is required.", form_data=form_data)
        if not source_paths:
            return _render_overview(
                ingest_error="A source path is required.",
                form_data=form_data,
            )
        if len(source_paths) > 1:
            return _render_overview(
                ingest_error="Use one absolute source path per ingest job.",
                form_data=form_data,
            )

        try:
            state = daemon_get(daemon_base_url, "/state")
        except httpx.HTTPError as exc:
            return _render_overview(
                ingest_error="Cannot start ingest because daemon readiness could not be confirmed.",
                ingest_error_detail=_describe_http_error(exc),
                form_data=form_data,
            )

        ingest_gate = _build_ingest_gate(state)
        if not ingest_gate["can_start"]:
            return _render_overview(
                ingest_error=(
                    "Cannot start ingest while daemon state is "
                    f"{ingest_gate['current_state']}. {ingest_gate['operator_action']}"
                ),
                form_data=form_data,
            )

        try:
            created = daemon_post(
                daemon_base_url,
                "/ingest/jobs",
                {"media_label": media_label, "source_paths": source_paths},
            )
        except httpx.HTTPStatusError as exc:
            validation_error, validation_detail = _format_ingest_source_validation_error(exc)
            if validation_error:
                return _render_overview(
                    ingest_error=validation_error,
                    ingest_error_detail=validation_detail,
                    form_data=form_data,
                )
            if exc.response.status_code == 409:
                conflict_state: dict[str, Any] | None = None
                try:
                    conflict_state = daemon_get(daemon_base_url, "/state")
                except httpx.HTTPError:
                    conflict_state = state
                conflict_gate = _build_ingest_gate(conflict_state)
                return _render_overview(
                    ingest_error=(
                        "Daemon rejected ingest creation because it is not ready yet. "
                        f"Current state: {conflict_gate['current_state']}. {conflict_gate['operator_action']}"
                    ),
                    ingest_error_detail=_describe_http_error(exc),
                    form_data=form_data,
                )
            return _render_overview(
                ingest_error="Daemon failed to create ingest job.",
                ingest_error_detail=_describe_http_error(exc),
                form_data=form_data,
            )
        except httpx.HTTPError as exc:
            return _render_overview(
                ingest_error="Failed to create ingest job due to daemon communication error.",
                ingest_error_detail=_describe_http_error(exc),
                form_data=form_data,
            )

        discovered_count = created.get("discovered_count")
        filtered_count = int(created.get("filtered_count", 0) or 0)
        filtered_sources = created.get("filtered_sources")
        if discovered_count is None:
            notice = f"Created ingest job #{created['job_id']}."
        else:
            notice = f"Created ingest job #{created['job_id']} with {discovered_count} discovered file(s)."
        notice_detail = None
        if filtered_count > 0:
            notice = f"{notice} Skipped {filtered_count} file(s) by the v1 ingest policy."
            if isinstance(filtered_sources, list) and filtered_sources:
                detail_lines: list[str] = []
                for item in filtered_sources:
                    if not isinstance(item, dict):
                        continue
                    source_path = str(item.get("source_path", "")).strip()
                    reason = str(item.get("reason", "")).strip()
                    if source_path and reason:
                        detail_lines.append(f"{source_path}: {reason}")
                if detail_lines:
                    notice_detail = "\n".join(detail_lines)
        if _is_ajax_request():
            return _render_overview(ingest_notice=notice, ingest_notice_detail=notice_detail)
        return redirect(url_for("job_detail", job_id=created["job_id"]))

    @app.post("/actions/block-devices/mount")
    def mount_block_device() -> str:
        device_path = request.form.get("device_path", "").strip()
        if not device_path:
            return _render_block_devices(block_device_error="Missing device_path for mount action.")
        try:
            outcome = daemon_post(daemon_base_url, "/block-devices/mount", {"device_path": device_path})
        except httpx.HTTPError as exc:
            return _render_block_devices(
                block_device_error=f"Failed to mount {device_path}.",
                block_device_error_detail=_describe_http_error(exc),
            )
        mounted_device = outcome.get("device_path", device_path)
        mounted_path = outcome.get("mount_path", "")
        return _render_block_devices(
            block_device_notice=f"Mounted {mounted_device} at {mounted_path}."
        )

    @app.post("/actions/block-devices/unmount")
    def unmount_block_device() -> str:
        device_path = request.form.get("device_path", "").strip()
        if not device_path:
            return _render_block_devices(block_device_error="Missing device_path for unmount action.")
        try:
            outcome = daemon_post(daemon_base_url, "/block-devices/unmount", {"device_path": device_path})
        except httpx.HTTPError as exc:
            return _render_block_devices(
                block_device_error=f"Failed to unmount {device_path}.",
                block_device_error_detail=_describe_http_error(exc),
            )
        unmounted_device = outcome.get("device_path", device_path)
        unmounted_path = outcome.get("mount_path", "")
        return _render_block_devices(
            block_device_notice=f"Unmounted {unmounted_device} from {unmounted_path}."
        )

    @app.post("/actions/block-devices/use-as-ingest-source")
    def use_block_device_as_ingest_source() -> Response:
        mount_path = request.form.get("mount_path", "").strip()
        media_label = request.form.get("media_label", "").strip()
        if not mount_path:
            return make_response(
                _render_block_devices(block_device_error="Missing mount_path for ingest prefill action.")
            )
        form_data = {"media_label": media_label or "mounted-media", "source_paths": mount_path}
        notice = f"Prepared ingest form for mounted source {mount_path}. Review and submit to start ingest."
        return _render_overview(operator_notice=notice, form_data=form_data)

    @app.post("/actions/daemon/tick")
    def tick_daemon() -> Response:
        return_to = request.form.get("return_to", "").strip()
        try:
            outcome = daemon_post(
                daemon_base_url,
                "/daemon/tick",
                {},
                timeout_seconds=DEFAULT_TICK_TIMEOUT_SECONDS,
            )
        except httpx.TimeoutException:
            if return_to and return_to.startswith("/jobs/"):
                if _is_ajax_request():
                    try:
                        job_id = int(return_to.rsplit("/", 1)[-1])
                    except ValueError:
                        return _render_overview(
                            operator_notice="Daemon action is still running. Refreshing status...",
                            operator_notice_pending=True,
                            auto_refresh_ms=DEFAULT_TICK_STATUS_REFRESH_MS,
                        )
                    return make_response(
                        _render_job_detail(
                            job_id,
                            action_notice="Daemon action is still running. Refreshing status...",
                            action_notice_pending=True,
                        )
                    )
                return redirect(return_to)
            return _render_overview(
                operator_notice="Daemon action is still running. Refreshing status...",
                operator_notice_pending=True,
                auto_refresh_ms=DEFAULT_TICK_STATUS_REFRESH_MS,
            )
        except httpx.HTTPError as exc:
            if return_to and return_to.startswith("/jobs/"):
                try:
                    job_id = int(return_to.rsplit("/", 1)[-1])
                except ValueError:
                    return _render_overview(
                        ingest_error="Failed to run daemon tick.",
                        ingest_error_detail=_describe_http_error(exc),
                    )
                return make_response(
                    _render_job_detail(
                        job_id,
                        action_error=f"Failed to run daemon tick: {_describe_http_error(exc)}",
                    )
                )
            return _render_overview(
                ingest_error="Failed to run daemon tick.",
                ingest_error_detail=_describe_http_error(exc),
            )

        if outcome.get("handled"):
            if outcome.get("already_progressing"):
                next_state = outcome.get("next_state", outcome.get("state", "UNKNOWN"))
                message = (
                    "Daemon is already progressing in state "
                    f"{next_state}; wait and refresh instead of running manual ticks."
                )
            else:
                next_state = outcome.get("next_state", outcome.get("state", "UNKNOWN"))
                message = f"Daemon tick completed in state {next_state}."
        else:
            message = f"Daemon tick was a no-op in state {outcome.get('state', 'UNKNOWN')}."

        if return_to and return_to.startswith("/jobs/"):
            try:
                job_id = int(return_to.rsplit("/", 1)[-1])
            except ValueError:
                return _render_overview(operator_notice=message)
            return make_response(
                _render_job_detail(
                    job_id,
                    action_notice=message,
                    action_notice_pending=bool(outcome.get("already_progressing")),
                )
            )
        return _render_overview(
            operator_notice=message,
            operator_notice_pending=bool(outcome.get("already_progressing")),
        )

    @app.post("/actions/retry-upload")
    def retry_error_upload() -> Response:
        file_id = request.form.get("file_id", type=int)
        job_id = request.form.get("job_id", type=int)
        if file_id is None:
            if job_id is not None:
                return make_response(
                    _render_job_detail(
                        job_id,
                        action_error="Missing file_id for retry action.",
                    )
                )
            return _render_overview(ingest_error="Missing file_id for retry action.")
        try:
            outcome = daemon_post(daemon_base_url, f"/ingest/files/{file_id}/retry-upload", {})
        except httpx.HTTPError as exc:
            detail = _describe_http_error(exc)
            if job_id is not None:
                return make_response(
                    _render_job_detail(
                        job_id,
                        action_error=f"Failed to requeue file #{file_id} for upload: {detail}",
                    )
                )
            return _render_overview(
                ingest_error=f"Failed to requeue file #{file_id} for upload.",
                ingest_error_detail=detail,
            )

        next_state = outcome.get("next_state", "UPLOAD_PREPARE")
        message = f"File #{file_id} requeued for upload; daemon moved to {next_state}."
        if job_id is not None:
            return make_response(_render_job_detail(job_id, action_notice=message))
        return _render_overview(operator_notice=message)

    @app.post("/network/scan")
    def scan_wifi() -> Any:
        try:
            daemon_post(daemon_base_url, "/network/wifi-scan", {})
        except httpx.HTTPError as exc:
            return _render_network(network_error=f"Failed to scan Wi-Fi: {_describe_http_error(exc)}")
        if _is_ajax_request():
            return _render_network(network_notice="Triggered Wi-Fi scan and refreshed network status.")
        return redirect(url_for("network_page"))

    @app.post("/network/upstream-recheck")
    def recheck_upstream_status() -> str:
        try:
            outcome = daemon_post(daemon_base_url, "/network/upstream-recheck", {})
        except httpx.HTTPError as exc:
            return _render_network(
                network_error=f"Failed to recheck upstream connectivity: {_describe_http_error(exc)}"
            )
        return _render_network(
            network_notice=_portal_recheck_notice(
                outcome.get("snapshot"),
                outcome.get("connectivity_check"),
            )
        )

    @app.post("/network/portal-handoff/start")
    def start_portal_handoff() -> str:
        try:
            daemon_post(daemon_base_url, "/network/portal-handoff/start", {})
        except httpx.HTTPError as exc:
            return _render_network(
                network_error=f"Failed to start portal handoff: {_describe_http_error(exc)}"
            )
        return _render_network(
            network_notice=(
                "Portal handoff started. Join local AP from phone/laptop, complete portal login "
                "using http://neverssl.com, then recheck and stop handoff."
            )
        )

    @app.post("/network/portal-handoff/stop")
    def stop_portal_handoff() -> str:
        try:
            daemon_post(daemon_base_url, "/network/portal-handoff/stop", {})
        except httpx.HTTPError as exc:
            return _render_network(
                network_error=f"Failed to stop portal handoff: {_describe_http_error(exc)}"
            )
        return _render_network(
            network_notice="Portal handoff stopped and Ethernet route preferences were restored."
        )

    @app.post("/network/ap-config")
    def update_ap_config() -> str:
        ssid = request.form.get("ssid", "")
        password = request.form.get("password", "")
        ap_form = {"ssid": ssid, "password": password}

        if not ssid.strip():
            return _render_network(
                network_error="AP SSID is required.",
                ap_form_data=ap_form,
            )

        try:
            daemon_put(
                daemon_base_url,
                "/network/ap-config",
                {"ssid": ssid, "password": password},
            )
        except httpx.HTTPError as exc:
            return _render_network(
                network_error=f"Failed to update AP config: {_describe_http_error(exc)}",
                ap_form_data=ap_form,
            )

        return _render_network(
            network_notice="AP configuration updated and applied via NetworkManager.",
            ap_form_data={"ssid": ssid.strip(), "password": ""},
        )

    @app.post("/network/sta-connect")
    def connect_sta() -> str:
        ssid = request.form.get("sta_ssid", "")
        password = request.form.get("sta_password", "")
        sta_form = {"ssid": ssid, "password": password}

        if not ssid.strip():
            return _render_network(
                network_error="Upstream Wi-Fi SSID is required.",
                sta_form_data=sta_form,
            )

        payload: dict[str, Any] = {"ssid": ssid}
        if password.strip():
            payload["password"] = password
        try:
            daemon_post(daemon_base_url, "/network/sta-connect", payload)
        except httpx.HTTPError as exc:
            return _render_network(
                network_error=f"Failed to connect upstream Wi-Fi: {_describe_http_error(exc)}",
                sta_form_data=sta_form,
            )

        return _render_network(
            network_notice=f"Upstream Wi-Fi connect requested for SSID '{ssid.strip()}'.",
            sta_form_data={"ssid": ssid.strip(), "password": ""},
        )

    @app.get("/jobs/<int:job_id>")
    def job_detail(job_id: int) -> Response:
        return _render_job_detail(job_id)

    return app
