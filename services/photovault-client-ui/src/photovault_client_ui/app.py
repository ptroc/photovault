"""SSR control-plane UI for the photovault client."""

from collections.abc import Callable
from typing import Any

import httpx
from flask import Flask, Response, make_response, redirect, request, url_for

from .api_client import (
    _daemon_get,
    _daemon_post,
    _daemon_put,
    _describe_http_error,
    _format_ingest_source_validation_error,
)
from .constants import (
    DEFAULT_DAEMON_BASE_URL,
    DEFAULT_TICK_STATUS_REFRESH_MS,
    DEFAULT_TICK_TIMEOUT_SECONDS,
)
from .context_loaders import is_ajax_request, load_daemon_context, load_network_context, load_selected_job
from .page_renderers import ClientUiRenderer
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
    del network_snapshot_get, network_connect, network_scan
    app = Flask(__name__)
    renderer = ClientUiRenderer(
        daemon_base_url=daemon_base_url,
        daemon_get=daemon_get,
        load_daemon_context=lambda *, events_limit=10: load_daemon_context(
            daemon_get, daemon_base_url, events_limit=events_limit
        ),
        load_selected_job=lambda job_id: load_selected_job(daemon_get, daemon_base_url, job_id),
        load_network_context=lambda **kwargs: load_network_context(
            daemon_get,
            daemon_base_url,
            **kwargs,
        ),
        dependency_snapshot_get=dependency_snapshot_get,
        interface_addresses_get=interface_addresses_get,
        annotate_job_record=_annotate_job_record,
        block_partition_ingest_prefill=_block_partition_ingest_prefill,
        build_ingest_gate=_build_ingest_gate,
        build_overview_metrics=_build_overview_metrics,
        derive_daemon_progress_view=_derive_daemon_progress_view,
        derive_state_guidance=_derive_state_guidance,
        filter_jobs=_filter_jobs,
        format_size_bytes=_format_size_bytes,
        summarize_recent_events=_summarize_recent_events,
        is_ajax_request=is_ajax_request,
    )

    @app.get("/")
    def index() -> Response:
        return renderer.render_overview()

    @app.get("/jobs")
    def jobs_page() -> Response:
        return renderer.render_jobs(selected_filter=request.args.get("filter", "active"))

    @app.get("/events")
    def events_page() -> str:
        return renderer.render_events()

    @app.get("/network")
    def network_page() -> Response:
        return renderer.render_network()

    @app.get("/block-devices")
    def block_devices_page() -> Response:
        return renderer.render_block_devices()

    @app.post("/ingest/jobs")
    def create_ingest_job() -> Any:
        media_label = request.form.get("media_label", "").strip()
        source_paths_text = request.form.get("source_paths", "")
        source_paths = [line.strip() for line in source_paths_text.splitlines() if line.strip()]
        normalized_source_path = source_paths[0] if len(source_paths) == 1 else source_paths_text.strip()
        form_data = {"media_label": media_label, "source_paths": normalized_source_path}

        if not media_label:
            return renderer.render_overview(ingest_error="Media label is required.", form_data=form_data)
        if not source_paths:
            return renderer.render_overview(
                ingest_error="A source path is required.",
                form_data=form_data,
            )
        if len(source_paths) > 1:
            return renderer.render_overview(
                ingest_error="Use one absolute source path per ingest job.",
                form_data=form_data,
            )

        try:
            state = daemon_get(daemon_base_url, "/state")
        except httpx.HTTPError as exc:
            return renderer.render_overview(
                ingest_error="Cannot start ingest because daemon readiness could not be confirmed.",
                ingest_error_detail=_describe_http_error(exc),
                form_data=form_data,
            )

        ingest_gate = _build_ingest_gate(state)
        if not ingest_gate["can_start"]:
            return renderer.render_overview(
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
                return renderer.render_overview(
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
                return renderer.render_overview(
                    ingest_error=(
                        "Daemon rejected ingest creation because it is not ready yet. "
                        f"Current state: {conflict_gate['current_state']}. {conflict_gate['operator_action']}"
                    ),
                    ingest_error_detail=_describe_http_error(exc),
                    form_data=form_data,
                )
            return renderer.render_overview(
                ingest_error="Daemon failed to create ingest job.",
                ingest_error_detail=_describe_http_error(exc),
                form_data=form_data,
            )
        except httpx.HTTPError as exc:
            return renderer.render_overview(
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
        if is_ajax_request():
            return renderer.render_overview(ingest_notice=notice, ingest_notice_detail=notice_detail)
        return redirect(url_for("job_detail", job_id=created["job_id"]))

    @app.post("/actions/block-devices/mount")
    def mount_block_device() -> str:
        device_path = request.form.get("device_path", "").strip()
        if not device_path:
            return renderer.render_block_devices(block_device_error="Missing device_path for mount action.")
        try:
            outcome = daemon_post(daemon_base_url, "/block-devices/mount", {"device_path": device_path})
        except httpx.HTTPError as exc:
            return renderer.render_block_devices(
                block_device_error=f"Failed to mount {device_path}.",
                block_device_error_detail=_describe_http_error(exc),
            )
        mounted_device = outcome.get("device_path", device_path)
        mounted_path = outcome.get("mount_path", "")
        return renderer.render_block_devices(
            block_device_notice=f"Mounted {mounted_device} at {mounted_path}."
        )

    @app.post("/actions/block-devices/unmount")
    def unmount_block_device() -> str:
        device_path = request.form.get("device_path", "").strip()
        if not device_path:
            return renderer.render_block_devices(block_device_error="Missing device_path for unmount action.")
        try:
            outcome = daemon_post(daemon_base_url, "/block-devices/unmount", {"device_path": device_path})
        except httpx.HTTPError as exc:
            return renderer.render_block_devices(
                block_device_error=f"Failed to unmount {device_path}.",
                block_device_error_detail=_describe_http_error(exc),
            )
        unmounted_device = outcome.get("device_path", device_path)
        unmounted_path = outcome.get("mount_path", "")
        return renderer.render_block_devices(
            block_device_notice=f"Unmounted {unmounted_device} from {unmounted_path}."
        )

    @app.post("/actions/block-devices/use-as-ingest-source")
    def use_block_device_as_ingest_source() -> Response:
        mount_path = request.form.get("mount_path", "").strip()
        media_label = request.form.get("media_label", "").strip()
        if not mount_path:
            return make_response(
                renderer.render_block_devices(
                    block_device_error="Missing mount_path for ingest prefill action."
                )
            )
        form_data = {"media_label": media_label or "mounted-media", "source_paths": mount_path}
        notice = f"Prepared ingest form for mounted source {mount_path}. Review and submit to start ingest."
        return renderer.render_overview(operator_notice=notice, form_data=form_data)

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
                if is_ajax_request():
                    try:
                        job_id = int(return_to.rsplit("/", 1)[-1])
                    except ValueError:
                        return renderer.render_overview(
                            operator_notice="Daemon action is still running. Refreshing status...",
                            operator_notice_pending=True,
                            auto_refresh_ms=DEFAULT_TICK_STATUS_REFRESH_MS,
                        )
                    return make_response(
                        renderer.render_job_detail(
                            job_id,
                            action_notice="Daemon action is still running. Refreshing status...",
                            action_notice_pending=True,
                        )
                    )
                return redirect(return_to)
            return renderer.render_overview(
                operator_notice="Daemon action is still running. Refreshing status...",
                operator_notice_pending=True,
                auto_refresh_ms=DEFAULT_TICK_STATUS_REFRESH_MS,
            )
        except httpx.HTTPError as exc:
            if return_to and return_to.startswith("/jobs/"):
                try:
                    job_id = int(return_to.rsplit("/", 1)[-1])
                except ValueError:
                    return renderer.render_overview(
                        ingest_error="Failed to run daemon tick.",
                        ingest_error_detail=_describe_http_error(exc),
                    )
                return make_response(
                    renderer.render_job_detail(
                        job_id,
                        action_error=f"Failed to run daemon tick: {_describe_http_error(exc)}",
                    )
                )
            return renderer.render_overview(
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
                return renderer.render_overview(operator_notice=message)
            return make_response(
                renderer.render_job_detail(
                    job_id,
                    action_notice=message,
                    action_notice_pending=bool(outcome.get("already_progressing")),
                )
            )
        return renderer.render_overview(
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
                    renderer.render_job_detail(
                        job_id,
                        action_error="Missing file_id for retry action.",
                    )
                )
            return renderer.render_overview(ingest_error="Missing file_id for retry action.")
        try:
            outcome = daemon_post(daemon_base_url, f"/ingest/files/{file_id}/retry-upload", {})
        except httpx.HTTPError as exc:
            detail = _describe_http_error(exc)
            if job_id is not None:
                return make_response(
                    renderer.render_job_detail(
                        job_id,
                        action_error=f"Failed to requeue file #{file_id} for upload: {detail}",
                    )
                )
            return renderer.render_overview(
                ingest_error=f"Failed to requeue file #{file_id} for upload.",
                ingest_error_detail=detail,
            )

        next_state = outcome.get("next_state", "UPLOAD_PREPARE")
        message = f"File #{file_id} requeued for upload; daemon moved to {next_state}."
        if job_id is not None:
            return make_response(renderer.render_job_detail(job_id, action_notice=message))
        return renderer.render_overview(operator_notice=message)

    @app.post("/network/scan")
    def scan_wifi() -> Any:
        try:
            daemon_post(daemon_base_url, "/network/wifi-scan", {})
        except httpx.HTTPError as exc:
            return renderer.render_network(
                network_error=f"Failed to scan Wi-Fi: {_describe_http_error(exc)}"
            )
        if is_ajax_request():
            return renderer.render_network(
                network_notice="Triggered Wi-Fi scan and refreshed network status."
            )
        return redirect(url_for("network_page"))

    @app.post("/network/upstream-recheck")
    def recheck_upstream_status() -> str:
        try:
            outcome = daemon_post(daemon_base_url, "/network/upstream-recheck", {})
        except httpx.HTTPError as exc:
            return renderer.render_network(
                network_error=f"Failed to recheck upstream connectivity: {_describe_http_error(exc)}"
            )
        return renderer.render_network(
            network_notice=renderer.portal_recheck_notice(
                outcome.get("snapshot"),
                outcome.get("connectivity_check"),
            )
        )

    @app.post("/network/portal-handoff/start")
    def start_portal_handoff() -> str:
        try:
            daemon_post(daemon_base_url, "/network/portal-handoff/start", {})
        except httpx.HTTPError as exc:
            return renderer.render_network(
                network_error=f"Failed to start portal handoff: {_describe_http_error(exc)}"
            )
        return renderer.render_network(
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
            return renderer.render_network(
                network_error=f"Failed to stop portal handoff: {_describe_http_error(exc)}"
            )
        return renderer.render_network(
            network_notice="Portal handoff stopped and Ethernet route preferences were restored."
        )

    @app.post("/network/ap-config")
    def update_ap_config() -> str:
        ssid = request.form.get("ssid", "")
        password = request.form.get("password", "")
        ap_form = {"ssid": ssid, "password": password}

        if not ssid.strip():
            return renderer.render_network(
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
            return renderer.render_network(
                network_error=f"Failed to update AP config: {_describe_http_error(exc)}",
                ap_form_data=ap_form,
            )

        return renderer.render_network(
            network_notice="AP configuration updated and applied via NetworkManager.",
            ap_form_data={"ssid": ssid.strip(), "password": ""},
        )

    @app.post("/network/sta-connect")
    def connect_sta() -> str:
        ssid = request.form.get("sta_ssid", "")
        password = request.form.get("sta_password", "")
        sta_form = {"ssid": ssid, "password": password}

        if not ssid.strip():
            return renderer.render_network(
                network_error="Upstream Wi-Fi SSID is required.",
                sta_form_data=sta_form,
            )

        payload: dict[str, Any] = {"ssid": ssid}
        if password.strip():
            payload["password"] = password
        try:
            daemon_post(daemon_base_url, "/network/sta-connect", payload)
        except httpx.HTTPError as exc:
            return renderer.render_network(
                network_error=f"Failed to connect upstream Wi-Fi: {_describe_http_error(exc)}",
                sta_form_data=sta_form,
            )

        return renderer.render_network(
            network_notice=f"Upstream Wi-Fi connect requested for SSID '{ssid.strip()}'.",
            sta_form_data={"ssid": ssid.strip(), "password": ""},
        )

    @app.get("/jobs/<int:job_id>")
    def job_detail(job_id: int) -> Response:
        return renderer.render_job_detail(job_id)

    return app
