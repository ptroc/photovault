"""HTTP API client helpers for photovault-clientd."""
from typing import Any

import httpx

from .constants import DEFAULT_HTTP_TIMEOUT_SECONDS


def _describe_http_error(exc: httpx.HTTPError) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        status_code = exc.response.status_code
        detail = ""
        try:
            payload = exc.response.json()
            if isinstance(payload, dict):
                detail_obj = payload.get("detail", "")
                if isinstance(detail_obj, dict):
                    code = str(detail_obj.get("code", "")).strip()
                    message = str(detail_obj.get("message", "")).strip()
                    suggestion = str(detail_obj.get("suggestion", "")).strip()
                    detail_parts = []
                    if code:
                        detail_parts.append(f"[{code}]")
                    if message:
                        detail_parts.append(message)
                    if suggestion:
                        detail_parts.append(suggestion)
                    detail = " ".join(detail_parts).strip()
                else:
                    detail = str(detail_obj).strip()
        except ValueError:
            detail = exc.response.text.strip()
        summary = f"daemon API returned HTTP {status_code}"
        if detail:
            return f"{summary}: {detail}"
        return summary

    message = str(exc).strip()
    if isinstance(exc, httpx.ConnectError):
        return f"connection failure: {message or 'unable to reach daemon endpoint'}"
    if isinstance(exc, httpx.TimeoutException):
        return f"request timeout: {message or 'daemon did not respond in time'}"
    if message:
        return message
    return exc.__class__.__name__


def _format_ingest_source_validation_error(exc: httpx.HTTPStatusError) -> tuple[str | None, str | None]:
    try:
        payload = exc.response.json()
    except ValueError:
        return None, None

    if not isinstance(payload, dict):
        return None, None

    detail = payload.get("detail")
    if not isinstance(detail, dict):
        return None, None
    if str(detail.get("code", "")).strip() != "INGEST_SOURCE_PATH_INVALID":
        return None, None

    message = str(detail.get("message", "")).strip() or "One or more source paths are invalid."
    suggestion = str(detail.get("suggestion", "")).strip()
    invalid_sources = detail.get("invalid_sources")

    source_lines: list[str] = []
    if isinstance(invalid_sources, list):
        for item in invalid_sources:
            if not isinstance(item, dict):
                continue
            source_path = str(item.get("source_path", "")).strip()
            reason = str(item.get("reason", "")).strip()
            if source_path and reason:
                source_lines.append(f"{source_path}: {reason}")

    operator_message = (
        f"{message} {suggestion}".strip() if suggestion else message
    )
    technical_detail = _describe_http_error(exc)
    if source_lines:
        technical_detail = "\n".join([technical_detail, *source_lines])
    return operator_message, technical_detail


def _daemon_get(daemon_base_url: str, path: str) -> Any:
    with httpx.Client(base_url=daemon_base_url, timeout=DEFAULT_HTTP_TIMEOUT_SECONDS) as client:
        response = client.get(path)
        response.raise_for_status()
        return response.json()


def _daemon_post(
    daemon_base_url: str,
    path: str,
    payload: dict[str, Any],
    *,
    timeout_seconds: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
) -> Any:
    with httpx.Client(base_url=daemon_base_url, timeout=timeout_seconds) as client:
        response = client.post(path, json=payload)
        response.raise_for_status()
        return response.json()


def _daemon_put(
    daemon_base_url: str,
    path: str,
    payload: dict[str, Any],
    *,
    timeout_seconds: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
) -> Any:
    with httpx.Client(base_url=daemon_base_url, timeout=timeout_seconds) as client:
        response = client.put(path, json=payload)
        response.raise_for_status()
        return response.json()
