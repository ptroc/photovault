#!/usr/bin/env python3
"""Deterministic M4 storage/index smoke check for local or remote hosts."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from urllib import error, request

SMOKE_RELATIVE_PATH = "_photovault_smoke/m4/manual-smoke.txt"
SMOKE_CONTENT = b"photovault-m4-smoke-check\n"


def _http_json(method: str, url: str, payload: dict | None = None) -> dict:
    data = None
    headers: dict[str, str] = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = request.Request(url=url, data=data, headers=headers, method=method)
    with request.urlopen(req, timeout=10) as response:
        body = response.read().decode("utf-8")
    return json.loads(body)


def _http_ok(url: str) -> None:
    with request.urlopen(url, timeout=10) as response:
        response.read()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-base-url", default="http://127.0.0.1:9301")
    parser.add_argument("--server-ui-url", default="http://127.0.0.1:9401/")
    parser.add_argument("--storage-root", required=True)
    args = parser.parse_args()

    storage_root = Path(args.storage_root).expanduser().resolve()
    if not storage_root.is_dir():
        raise SystemExit(f"storage root is not a directory: {storage_root}")
    if not os.access(storage_root, os.W_OK):
        raise SystemExit(f"storage root is not writable by current user: {storage_root}")

    smoke_path = storage_root / SMOKE_RELATIVE_PATH
    smoke_path.parent.mkdir(parents=True, exist_ok=True)
    smoke_path.write_bytes(SMOKE_CONTENT)
    smoke_sha = hashlib.sha256(SMOKE_CONTENT).hexdigest()

    try:
        _http_ok(f"{args.api_base_url.rstrip('/')}/healthz")
        _http_ok(args.server_ui_url)

        index_payload = _http_json("POST", f"{args.api_base_url.rstrip('/')}/v1/storage/index")
        handshake_payload = _http_json(
            "POST",
            f"{args.api_base_url.rstrip('/')}/v1/upload/metadata-handshake",
            {
                "files": [
                    {
                        "client_file_id": 1,
                        "sha256_hex": smoke_sha,
                        "size_bytes": len(SMOKE_CONTENT),
                    }
                ]
            },
        )
        latest_run_payload = _http_json(
            "GET",
            f"{args.api_base_url.rstrip('/')}/v1/admin/latest-index-run",
        )
        files_payload = _http_json(
            "GET",
            f"{args.api_base_url.rstrip('/')}/v1/admin/files?limit=200&offset=0",
        )
    except (error.URLError, error.HTTPError, ValueError) as exc:
        raise SystemExit(f"M4 smoke check failed: {exc}") from exc

    if handshake_payload["results"][0]["decision"] != "ALREADY_EXISTS":
        raise SystemExit("M4 smoke check failed: smoke file was not recognized as already indexed")

    latest_run = latest_run_payload.get("latest_run")
    if latest_run is None:
        raise SystemExit("M4 smoke check failed: latest index run was not recorded")

    indexed_paths = {item["relative_path"] for item in files_payload.get("items", [])}
    if SMOKE_RELATIVE_PATH not in indexed_paths:
        raise SystemExit("M4 smoke check failed: smoke path missing from admin files view")

    print("m4-smoke: ok")
    print(f"storage_root={storage_root}")
    print(
        "index_result="
        f"scanned={index_payload['scanned_files']} "
        f"indexed={index_payload['indexed_files']} "
        f"existing_sha_matches={index_payload['existing_sha_matches']} "
        f"path_conflicts={index_payload['path_conflicts']} "
        f"errors={index_payload['errors']}"
    )
    print(
        "latest_run="
        f"completed_at={latest_run['completed_at_utc']} "
        f"scanned={latest_run['scanned_files']} "
        f"indexed={latest_run['indexed_files']}"
    )
    print(f"smoke_relative_path={SMOKE_RELATIVE_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
