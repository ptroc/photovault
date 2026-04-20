"""SSR control-plane UI for the photovault client."""

import subprocess
from collections.abc import Callable
from typing import Any

import httpx
from flask import Flask, Response, abort, make_response, redirect, render_template, request, url_for

DEFAULT_DAEMON_BASE_URL = "http://127.0.0.1:9101"
DEFAULT_HTTP_TIMEOUT_SECONDS = 2.0


def _daemon_get(daemon_base_url: str, path: str) -> Any:
    with httpx.Client(base_url=daemon_base_url, timeout=DEFAULT_HTTP_TIMEOUT_SECONDS) as client:
        response = client.get(path)
        response.raise_for_status()
        return response.json()


def _daemon_post(daemon_base_url: str, path: str, payload: dict[str, Any]) -> Any:
    with httpx.Client(base_url=daemon_base_url, timeout=DEFAULT_HTTP_TIMEOUT_SECONDS) as client:
        response = client.post(path, json=payload)
        response.raise_for_status()
        return response.json()


def _run_command(args: list[str]) -> str:
    completed = subprocess.run(
        args,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout


def _parse_nmcli_multiline(output: str) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    current: dict[str, str] = {}
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line:
            if current:
                records.append(current)
                current = {}
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        normalized_key = key.strip()
        if normalized_key in current and current:
            records.append(current)
            current = {}
        current[normalized_key] = value.strip()
    if current:
        records.append(current)
    return records


def _get_network_snapshot(command_runner: Callable[[list[str]], str] = _run_command) -> dict[str, Any]:
    general_output = command_runner(["nmcli", "-m", "multiline", "-f", "STATE,CONNECTIVITY,WIFI", "general"])
    devices_output = command_runner(
        ["nmcli", "-m", "multiline", "-f", "DEVICE,TYPE,STATE,CONNECTION", "device", "status"]
    )
    wifi_output = command_runner(
        ["nmcli", "-m", "multiline", "-f", "IN-USE,SSID,SIGNAL,SECURITY,CHAN,RATE", "device", "wifi", "list"]
    )

    general_records = _parse_nmcli_multiline(general_output)
    general = general_records[0] if general_records else {}
    devices = _parse_nmcli_multiline(devices_output)
    wifi_networks = _parse_nmcli_multiline(wifi_output)

    return {
        "general": {
            "state": general.get("STATE", "unknown"),
            "connectivity": general.get("CONNECTIVITY", "unknown"),
            "wifi": general.get("WIFI", "unknown"),
        },
        "devices": [
            {
                "device": item.get("DEVICE", ""),
                "type": item.get("TYPE", ""),
                "state": item.get("STATE", ""),
                "connection": item.get("CONNECTION", ""),
            }
            for item in devices
        ],
        "wifi_networks": [
            {
                "in_use": item.get("IN-USE", ""),
                "ssid": item.get("SSID", ""),
                "signal": item.get("SIGNAL", ""),
                "security": item.get("SECURITY", ""),
                "channel": item.get("CHAN", ""),
                "rate": item.get("RATE", ""),
            }
            for item in wifi_networks
            if item.get("SSID", "")
        ],
    }


def _scan_networks(command_runner: Callable[[list[str]], str] = _run_command) -> None:
    command_runner(["nmcli", "device", "wifi", "rescan"])


def _connect_network(
    ssid: str,
    password: str | None,
    command_runner: Callable[[list[str]], str] = _run_command,
) -> None:
    args = ["nmcli", "device", "wifi", "connect", ssid]
    if password:
        args.extend(["password", password])
    command_runner(args)


def _format_network_error(action: str, exc: subprocess.CalledProcessError | FileNotFoundError) -> str:
    if isinstance(exc, FileNotFoundError):
        return f"Failed to {action}: nmcli is not installed on this device."

    stderr = (exc.stderr or "").strip()
    stdout = (exc.stdout or "").strip()
    details = stderr or stdout
    if "not authorized" in details.lower():
        return (
            f"Failed to {action}: NetworkManager denied the photovault service user. "
            "This device needs a polkit rule that allows Wi-Fi management."
        )
    if details:
        return f"Failed to {action}: {details}"
    return f"Failed to {action}: nmcli exited with status {exc.returncode}."


def create_app(
    daemon_base_url: str = DEFAULT_DAEMON_BASE_URL,
    daemon_get: Callable[[str, str], Any] = _daemon_get,
    daemon_post: Callable[[str, str, dict[str, Any]], Any] = _daemon_post,
    network_snapshot_get: Callable[[], dict[str, Any]] = _get_network_snapshot,
    network_connect: Callable[[str, str | None], None] = _connect_network,
    network_scan: Callable[[], None] = _scan_networks,
) -> Flask:
    app = Flask(__name__)

    def _is_ajax_request() -> bool:
        return request.headers.get("X-Requested-With") == "XMLHttpRequest"

    def _load_daemon_context() -> dict[str, Any]:
        context: dict[str, Any] = {
            "daemon_error": None,
            "state": None,
            "diagnostics": None,
            "jobs": [],
            "events": [],
        }
        try:
            context["state"] = daemon_get(daemon_base_url, "/state")
            context["diagnostics"] = daemon_get(daemon_base_url, "/diagnostics/m0")
            jobs_payload = daemon_get(daemon_base_url, "/ingest/jobs")
            events_payload = daemon_get(daemon_base_url, "/events?limit=10")
            context["jobs"] = list(jobs_payload.get("jobs", []))
            context["events"] = list(events_payload.get("events", []))
        except httpx.HTTPError as exc:
            context["daemon_error"] = str(exc)
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
        network_form_data: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        context: dict[str, Any] = {
            "network_snapshot": None,
            "network_error": network_error,
            "network_form": {"ssid": "", "password": ""},
        }
        if network_form_data:
            context["network_form"].update(network_form_data)
        try:
            context["network_snapshot"] = network_snapshot_get()
        except (subprocess.CalledProcessError, FileNotFoundError) as exc:
            if context["network_error"] is None:
                context["network_error"] = _format_network_error("load NetworkManager status", exc)
        return context

    def _render_overview(
        *,
        selected_job_id: int | None = None,
        ingest_error: str | None = None,
        ingest_notice: str | None = None,
        operator_notice: str | None = None,
        form_data: dict[str, str] | None = None,
    ) -> Response | str:
        ingest_form = {"media_label": "", "source_paths": ""}
        if form_data:
            ingest_form.update(form_data)
        context = _load_daemon_context()
        selected_job, selected_job_error = _load_selected_job(selected_job_id)
        if selected_job_error and context["daemon_error"] is None:
            context["daemon_error"] = selected_job_error
        context.update(
            {
                "daemon_base_url": daemon_base_url,
                "selected_job": selected_job,
                "selected_job_id": selected_job_id,
                "ingest_error": ingest_error,
                "ingest_notice": ingest_notice,
                "operator_notice": operator_notice,
                "ingest_form": ingest_form,
                "active_page": "overview",
            }
        )
        template_name = "_overview_content.html" if _is_ajax_request() else "overview.html"
        response = make_response(render_template(template_name, **context))
        if selected_job_id is not None:
            response.headers["X-Client-Location"] = url_for("job_detail", job_id=selected_job_id)
        else:
            response.headers["X-Client-Location"] = url_for("index")
        return response

    def _render_network(
        *,
        network_error: str | None = None,
        network_form_data: dict[str, str] | None = None,
    ) -> str:
        context = _load_daemon_context()
        context.update(
            _load_network_context(
                network_error=network_error,
                network_form_data=network_form_data,
            )
        )
        context.update(
            {
                "daemon_base_url": daemon_base_url,
                "active_page": "network",
            }
        )
        return render_template("network.html", **context)

    @app.get("/")
    def index() -> str:
        return _render_overview()

    @app.get("/network")
    def network_page() -> str:
        return _render_network()

    @app.post("/ingest/jobs")
    def create_ingest_job() -> Any:
        media_label = request.form.get("media_label", "").strip()
        source_paths_text = request.form.get("source_paths", "")
        source_paths = [line.strip() for line in source_paths_text.splitlines() if line.strip()]
        form_data = {"media_label": media_label, "source_paths": source_paths_text}

        if not media_label:
            return _render_overview(ingest_error="Media label is required.", form_data=form_data)
        if not source_paths:
            return _render_overview(
                ingest_error="At least one source path is required.",
                form_data=form_data,
            )

        try:
            created = daemon_post(
                daemon_base_url,
                "/ingest/jobs",
                {"media_label": media_label, "source_paths": source_paths},
            )
        except httpx.HTTPError as exc:
            return _render_overview(
                ingest_error=f"Failed to create ingest job: {exc}",
                form_data=form_data,
            )

        discovered_count = created.get("discovered_count")
        if discovered_count is None:
            notice = f"Created ingest job #{created['job_id']}."
        else:
            notice = f"Created ingest job #{created['job_id']} with {discovered_count} discovered file(s)."
        if _is_ajax_request():
            return _render_overview(
                selected_job_id=created["job_id"],
                ingest_notice=notice,
            )
        return redirect(url_for("job_detail", job_id=created["job_id"]))

    @app.post("/actions/daemon/tick")
    def tick_daemon() -> Response | str:
        selected_job_id = request.form.get("selected_job_id", type=int)
        try:
            outcome = daemon_post(daemon_base_url, "/daemon/tick", {})
        except httpx.HTTPError as exc:
            return _render_overview(
                selected_job_id=selected_job_id,
                ingest_error=f"Failed to run daemon tick: {exc}",
            )

        if outcome.get("handled"):
            next_state = outcome.get("next_state", outcome.get("state", "UNKNOWN"))
            message = f"Daemon tick completed in state {next_state}."
        else:
            message = f"Daemon tick was a no-op in state {outcome.get('state', 'UNKNOWN')}."
        return _render_overview(
            selected_job_id=selected_job_id,
            operator_notice=message,
        )

    @app.post("/actions/retry-upload")
    def retry_error_upload() -> Response | str:
        selected_job_id = request.form.get("selected_job_id", type=int)
        file_id = request.form.get("file_id", type=int)
        if file_id is None:
            return _render_overview(
                selected_job_id=selected_job_id,
                ingest_error="Missing file_id for retry action.",
            )
        try:
            outcome = daemon_post(daemon_base_url, f"/ingest/files/{file_id}/retry-upload", {})
        except httpx.HTTPError as exc:
            return _render_overview(
                selected_job_id=selected_job_id,
                ingest_error=f"Failed to requeue file #{file_id} for upload: {exc}",
            )

        next_state = outcome.get("next_state", "UPLOAD_PREPARE")
        message = f"File #{file_id} requeued for upload; daemon moved to {next_state}."
        return _render_overview(
            selected_job_id=selected_job_id,
            operator_notice=message,
        )

    @app.post("/network/scan")
    def scan_wifi() -> Any:
        try:
            network_scan()
        except (subprocess.CalledProcessError, FileNotFoundError) as exc:
            return _render_network(network_error=_format_network_error("scan Wi-Fi", exc))
        return redirect(url_for("network_page"))

    @app.post("/network/connect")
    def connect_wifi() -> str:
        ssid = request.form.get("ssid", "").strip()
        password = request.form.get("password", "")
        network_form = {"ssid": ssid, "password": password}

        if not ssid:
            return _render_network(
                network_error="SSID is required.",
                network_form_data=network_form,
            )

        try:
            network_connect(ssid, password or None)
        except (subprocess.CalledProcessError, FileNotFoundError) as exc:
            return _render_network(
                network_error=_format_network_error("connect Wi-Fi", exc),
                network_form_data=network_form,
            )

        return redirect(url_for("network_page"))

    @app.get("/jobs/<int:job_id>")
    def job_detail(job_id: int) -> str:
        selected_job, selected_job_error = _load_selected_job(job_id)
        if selected_job is None:
            if selected_job_error == f"job_id {job_id} not found":
                abort(404, description=selected_job_error)
            return _render_overview()
        return _render_overview(selected_job_id=job_id)

    return app
