"""HTTP and auth helpers for clientd engine uploads and heartbeats."""

import json
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from photovault_clientd.db import (
    CLIENT_ENROLLMENT_APPROVED,
    CLIENT_ENROLLMENT_PENDING,
    CLIENT_ENROLLMENT_REVOKED,
    fetch_server_auth_state,
    upsert_server_auth_state,
)

DEFAULT_HANDSHAKE_TIMEOUT_SECONDS = 5.0
DEFAULT_ENROLL_TIMEOUT_SECONDS = 5.0

_CLIENT_REQUEST_AUTH_HEADERS: dict[str, str] = {}


def _core_urlopen():
    core_module = sys.modules.get("photovault_clientd.engine.core")
    if core_module is None:
        return urlopen
    return getattr(core_module, "urlopen", urlopen)


def _set_client_request_auth_headers(headers: dict[str, str] | None) -> None:
    global _CLIENT_REQUEST_AUTH_HEADERS
    _CLIENT_REQUEST_AUTH_HEADERS = dict(headers or {})


def _extract_http_error_detail(exc: HTTPError) -> str:
    try:
        body = exc.read().decode("utf-8")
    except Exception:
        body = ""
    if not body:
        return f"HTTP {exc.code}"
    try:
        decoded = json.loads(body)
    except json.JSONDecodeError:
        return body
    detail = decoded.get("detail")
    if isinstance(detail, str):
        return detail
    return body


def _build_client_auth_headers(
    conn,
    *,
    server_base_url: str,
    client_id: str,
    display_name: str,
    bootstrap_token: str | None,
    now_utc: str,
) -> tuple[dict[str, str] | None, str | None]:
    auth_state = fetch_server_auth_state(conn)
    effective_client_id = client_id
    effective_display_name = display_name
    if auth_state is not None:
        existing_client_id = str(auth_state.get("client_id", "")).strip()
        existing_display_name = str(auth_state.get("display_name", "")).strip()
        if existing_client_id:
            effective_client_id = existing_client_id
        if existing_display_name:
            effective_display_name = existing_display_name

    if (
        auth_state is not None
        and str(auth_state.get("enrollment_status")) == CLIENT_ENROLLMENT_APPROVED
        and isinstance(auth_state.get("auth_token"), str)
        and str(auth_state.get("auth_token"))
    ):
        return {
            "x-photovault-client-id": str(auth_state["client_id"]),
            "x-photovault-client-token": str(auth_state["auth_token"]),
        }, None

    if not bootstrap_token:
        if auth_state is not None and str(auth_state.get("enrollment_status")) == CLIENT_ENROLLMENT_PENDING:
            return None, "CLIENT_PENDING_APPROVAL"
        if auth_state is not None and str(auth_state.get("enrollment_status")) == CLIENT_ENROLLMENT_REVOKED:
            return None, "CLIENT_REVOKED"
        return {}, None

    request = Request(
        url=f"{server_base_url.rstrip('/')}/v1/client/enroll/bootstrap",
        data=json.dumps(
            {
                "client_id": effective_client_id,
                "display_name": effective_display_name,
                "bootstrap_token": bootstrap_token,
            }
        ).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with _core_urlopen()(request, timeout=DEFAULT_ENROLL_TIMEOUT_SECONDS) as response:
            body = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        detail = _extract_http_error_detail(exc)
        upsert_server_auth_state(
            conn,
            client_id=effective_client_id,
            display_name=effective_display_name,
            enrollment_status=CLIENT_ENROLLMENT_PENDING,
            auth_token=None,
            server_first_seen_at_utc=None,
            server_last_enrolled_at_utc=None,
            approved_at_utc=None,
            revoked_at_utc=None,
            last_enrollment_attempt_at_utc=now_utc,
            last_enrollment_result_at_utc=now_utc,
            last_error=detail,
            updated_at_utc=now_utc,
        )
        conn.commit()
        return None, detail
    except (URLError, TimeoutError, ValueError, OSError, json.JSONDecodeError) as exc:
        upsert_server_auth_state(
            conn,
            client_id=effective_client_id,
            display_name=effective_display_name,
            enrollment_status=CLIENT_ENROLLMENT_PENDING,
            auth_token=None,
            server_first_seen_at_utc=None,
            server_last_enrolled_at_utc=None,
            approved_at_utc=None,
            revoked_at_utc=None,
            last_enrollment_attempt_at_utc=now_utc,
            last_enrollment_result_at_utc=now_utc,
            last_error=str(exc),
            updated_at_utc=now_utc,
        )
        conn.commit()
        return None, str(exc)

    enrollment_status = str(body.get("enrollment_status") or CLIENT_ENROLLMENT_PENDING)
    auth_token = body.get("auth_token")
    approved_at_utc = now_utc if enrollment_status == CLIENT_ENROLLMENT_APPROVED else None
    revoked_at_utc = now_utc if enrollment_status == CLIENT_ENROLLMENT_REVOKED else None
    upsert_server_auth_state(
        conn,
        client_id=str(body.get("client_id") or client_id),
        display_name=str(body.get("display_name") or display_name),
        enrollment_status=enrollment_status,
        auth_token=str(auth_token) if isinstance(auth_token, str) and auth_token else None,
        server_first_seen_at_utc=(
            str(body.get("first_seen_at_utc")) if body.get("first_seen_at_utc") is not None else None
        ),
        server_last_enrolled_at_utc=(
            str(body.get("last_enrolled_at_utc"))
            if body.get("last_enrolled_at_utc") is not None
            else None
        ),
        approved_at_utc=approved_at_utc,
        revoked_at_utc=revoked_at_utc,
        last_enrollment_attempt_at_utc=now_utc,
        last_enrollment_result_at_utc=now_utc,
        last_error=None,
        updated_at_utc=now_utc,
    )
    conn.commit()
    if enrollment_status == CLIENT_ENROLLMENT_APPROVED and isinstance(auth_token, str) and auth_token:
        return {
            "x-photovault-client-id": str(body.get("client_id") or effective_client_id),
            "x-photovault-client-token": auth_token,
        }, None
    if enrollment_status == CLIENT_ENROLLMENT_REVOKED:
        return None, "CLIENT_REVOKED"
    return None, "CLIENT_PENDING_APPROVAL"


def _update_auth_state_from_privileged_http_error(
    conn,
    *,
    now_utc: str,
    exc: HTTPError,
) -> str | None:
    detail = _extract_http_error_detail(exc)
    auth_state = fetch_server_auth_state(conn)
    if auth_state is None:
        return None
    if detail == "CLIENT_PENDING_APPROVAL":
        upsert_server_auth_state(
            conn,
            client_id=str(auth_state["client_id"]),
            display_name=str(auth_state["display_name"]),
            enrollment_status=CLIENT_ENROLLMENT_PENDING,
            auth_token=None,
            server_first_seen_at_utc=auth_state.get("server_first_seen_at_utc"),
            server_last_enrolled_at_utc=auth_state.get("server_last_enrolled_at_utc"),
            approved_at_utc=None,
            revoked_at_utc=auth_state.get("revoked_at_utc"),
            last_enrollment_attempt_at_utc=auth_state.get("last_enrollment_attempt_at_utc"),
            last_enrollment_result_at_utc=auth_state.get("last_enrollment_result_at_utc"),
            last_error=detail,
            updated_at_utc=now_utc,
        )
        conn.commit()
        return detail
    if detail == "CLIENT_REVOKED":
        upsert_server_auth_state(
            conn,
            client_id=str(auth_state["client_id"]),
            display_name=str(auth_state["display_name"]),
            enrollment_status=CLIENT_ENROLLMENT_REVOKED,
            auth_token=auth_state.get("auth_token"),
            server_first_seen_at_utc=auth_state.get("server_first_seen_at_utc"),
            server_last_enrolled_at_utc=auth_state.get("server_last_enrolled_at_utc"),
            approved_at_utc=auth_state.get("approved_at_utc"),
            revoked_at_utc=now_utc,
            last_enrollment_attempt_at_utc=auth_state.get("last_enrollment_attempt_at_utc"),
            last_enrollment_result_at_utc=auth_state.get("last_enrollment_result_at_utc"),
            last_error=detail,
            updated_at_utc=now_utc,
        )
        conn.commit()
        return detail
    if detail in {"CLIENT_AUTH_REQUIRED", "CLIENT_AUTH_INVALID"}:
        upsert_server_auth_state(
            conn,
            client_id=str(auth_state["client_id"]),
            display_name=str(auth_state["display_name"]),
            enrollment_status=str(auth_state["enrollment_status"]),
            auth_token=auth_state.get("auth_token"),
            server_first_seen_at_utc=auth_state.get("server_first_seen_at_utc"),
            server_last_enrolled_at_utc=auth_state.get("server_last_enrolled_at_utc"),
            approved_at_utc=auth_state.get("approved_at_utc"),
            revoked_at_utc=auth_state.get("revoked_at_utc"),
            last_enrollment_attempt_at_utc=auth_state.get("last_enrollment_attempt_at_utc"),
            last_enrollment_result_at_utc=auth_state.get("last_enrollment_result_at_utc"),
            last_error=detail,
            updated_at_utc=now_utc,
        )
        conn.commit()
        return detail
    return None


def _post_metadata_handshake(
    *,
    server_base_url: str,
    files: list[dict[str, object]],
    timeout_seconds: float = DEFAULT_HANDSHAKE_TIMEOUT_SECONDS,
) -> dict[int, str]:
    payload = {
        "files": [
            {
                "client_file_id": int(item["file_id"]),
                "sha256_hex": str(item["sha256_hex"]),
                "size_bytes": int(item["size_bytes"]),
            }
            for item in files
        ]
    }
    request = Request(
        url=f"{server_base_url.rstrip('/')}/v1/upload/metadata-handshake",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", **_CLIENT_REQUEST_AUTH_HEADERS},
        method="POST",
    )
    with _core_urlopen()(request, timeout=timeout_seconds) as response:
        body = json.loads(response.read().decode("utf-8"))

    raw_results = body.get("results")
    if not isinstance(raw_results, list):
        raise ValueError("handshake response missing results list")

    results: dict[int, str] = {}
    for item in raw_results:
        if not isinstance(item, dict):
            raise ValueError("handshake result item must be an object")
        file_id = item.get("client_file_id")
        decision = item.get("decision")
        if not isinstance(file_id, int):
            raise ValueError("handshake result missing numeric client_file_id")
        if decision not in {"ALREADY_EXISTS", "UPLOAD_REQUIRED"}:
            raise ValueError(f"handshake result has invalid decision for file_id={file_id}")
        results[file_id] = decision
    return results


def _upload_file_content(
    *,
    server_base_url: str,
    sha256_hex: str,
    size_bytes: int,
    content: bytes,
    job_name: str | None = None,
    original_filename: str | None = None,
    timeout_seconds: float = DEFAULT_HANDSHAKE_TIMEOUT_SECONDS,
) -> str:
    headers = {"x-size-bytes": str(size_bytes)}
    headers.update(_CLIENT_REQUEST_AUTH_HEADERS)
    if job_name is not None:
        headers["x-job-name"] = job_name
    if original_filename is not None:
        headers["x-original-filename"] = original_filename

    request = Request(
        url=f"{server_base_url.rstrip('/')}/v1/upload/content/{sha256_hex}",
        data=content,
        headers=headers,
        method="PUT",
    )
    with _core_urlopen()(request, timeout=timeout_seconds) as response:
        body = json.loads(response.read().decode("utf-8"))

    status = body.get("status")
    if status not in {"STORED_TEMP", "ALREADY_EXISTS"}:
        raise ValueError("upload response has invalid status")
    return str(status)


def _post_server_verify(
    *,
    server_base_url: str,
    sha256_hex: str,
    size_bytes: int,
    timeout_seconds: float = DEFAULT_HANDSHAKE_TIMEOUT_SECONDS,
) -> str:
    request = Request(
        url=f"{server_base_url.rstrip('/')}/v1/upload/verify",
        data=json.dumps({"sha256_hex": sha256_hex, "size_bytes": size_bytes}).encode("utf-8"),
        headers={"Content-Type": "application/json", **_CLIENT_REQUEST_AUTH_HEADERS},
        method="POST",
    )
    with _core_urlopen()(request, timeout=timeout_seconds) as response:
        body = json.loads(response.read().decode("utf-8"))

    status = body.get("status")
    if status not in {"VERIFIED", "ALREADY_EXISTS", "VERIFY_FAILED"}:
        raise ValueError("verify response has invalid status")
    return str(status)


def _post_client_heartbeat(
    *,
    server_base_url: str,
    headers: dict[str, str],
    payload: dict[str, object],
    timeout_seconds: float = DEFAULT_HANDSHAKE_TIMEOUT_SECONDS,
) -> dict[str, object]:
    request = Request(
        url=f"{server_base_url.rstrip('/')}/v1/client/heartbeat",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", **headers},
        method="POST",
    )
    with _core_urlopen()(request, timeout=timeout_seconds) as response:
        body = json.loads(response.read().decode("utf-8"))
    if not isinstance(body, dict):
        raise ValueError("heartbeat response must be a JSON object")
    return body
