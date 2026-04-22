import hashlib
import importlib.util
import json
from io import BytesIO
from pathlib import Path
from urllib import error, request

import pytest

_MODULE_PATH = Path(__file__).resolve().parents[1] / "m4_smoke_check.py"
_MODULE_SPEC = importlib.util.spec_from_file_location("m4_smoke_check", _MODULE_PATH)
assert _MODULE_SPEC is not None and _MODULE_SPEC.loader is not None
m4_smoke_check = importlib.util.module_from_spec(_MODULE_SPEC)
_MODULE_SPEC.loader.exec_module(m4_smoke_check)


class _Response:
    def __init__(self, payload: dict[str, object], status: int = 200) -> None:
        self._payload = payload
        self.status = status

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def test_m4_smoke_check_requires_auth_boundary_and_indexes_fixture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    storage_root = tmp_path / "storage"
    storage_root.mkdir(parents=True, exist_ok=True)

    def fake_urlopen(req, timeout=10):
        url = req.full_url if isinstance(req, request.Request) else str(req)
        method = req.get_method() if isinstance(req, request.Request) else "GET"
        if method == "GET" and url.endswith("/healthz"):
            return _Response({"status": "ok"})
        if method == "GET" and url == "http://127.0.0.1:9401/":
            return _Response({"ok": True})
        if method == "POST" and url.endswith("/v1/storage/index"):
            return _Response(
                {
                    "scanned_files": 1,
                    "indexed_files": 1,
                    "new_sha_entries": 1,
                    "existing_sha_matches": 0,
                    "path_conflicts": 0,
                    "errors": 0,
                }
            )
        if method == "POST" and url.endswith("/v1/upload/metadata-handshake"):
            raise error.HTTPError(
                url=url,
                code=401,
                msg="Unauthorized",
                hdrs=None,
                fp=BytesIO(b'{"detail":"CLIENT_AUTH_REQUIRED"}'),
            )
        if method == "GET" and url.endswith("/v1/admin/latest-index-run"):
            return _Response(
                {
                    "latest_run": {
                        "completed_at_utc": "2026-04-22T11:00:00+00:00",
                        "scanned_files": 1,
                        "indexed_files": 1,
                        "new_sha_entries": 1,
                        "existing_sha_matches": 0,
                        "path_conflicts": 0,
                        "errors": 0,
                    }
                }
            )
        if method == "GET" and url.endswith("/v1/admin/files?limit=200&offset=0"):
            return _Response(
                {
                    "total": 1,
                    "limit": 200,
                    "offset": 0,
                    "items": [
                        {
                            "relative_path": m4_smoke_check.SMOKE_RELATIVE_PATH,
                            "sha256_hex": hashlib.sha256(m4_smoke_check.SMOKE_CONTENT).hexdigest(),
                            "size_bytes": len(m4_smoke_check.SMOKE_CONTENT),
                            "source_kind": "index_scan",
                            "first_seen_at_utc": "2026-04-22T11:00:00+00:00",
                            "last_seen_at_utc": "2026-04-22T11:00:00+00:00",
                        }
                    ],
                }
            )
        raise AssertionError(f"unexpected request: method={method} url={url}")

    monkeypatch.setattr(m4_smoke_check.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(
        "sys.argv",
        [
            "m4_smoke_check.py",
            "--storage-root",
            str(storage_root),
        ],
    )

    result = m4_smoke_check.main()
    assert result == 0
    assert (storage_root / m4_smoke_check.SMOKE_RELATIVE_PATH).read_bytes() == m4_smoke_check.SMOKE_CONTENT
    output = capsys.readouterr().out
    assert "m4-smoke: ok" in output
    assert "auth_boundary=metadata_handshake_requires_client_auth" in output


def test_m4_smoke_check_fails_when_metadata_handshake_does_not_require_auth(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    storage_root = tmp_path / "storage"
    storage_root.mkdir(parents=True, exist_ok=True)

    def fake_urlopen(req, timeout=10):
        url = req.full_url if isinstance(req, request.Request) else str(req)
        method = req.get_method() if isinstance(req, request.Request) else "GET"
        if method == "GET" and url.endswith("/healthz"):
            return _Response({"status": "ok"})
        if method == "GET" and url == "http://127.0.0.1:9401/":
            return _Response({"ok": True})
        if method == "POST" and url.endswith("/v1/storage/index"):
            return _Response(
                {
                    "scanned_files": 1,
                    "indexed_files": 1,
                    "new_sha_entries": 1,
                    "existing_sha_matches": 0,
                    "path_conflicts": 0,
                    "errors": 0,
                }
            )
        if method == "POST" and url.endswith("/v1/upload/metadata-handshake"):
            return _Response({"results": []}, status=200)
        if method == "GET" and url.endswith("/v1/admin/latest-index-run"):
            return _Response(
                {
                    "latest_run": {
                        "completed_at_utc": "2026-04-22T11:00:00+00:00",
                        "scanned_files": 1,
                        "indexed_files": 1,
                        "new_sha_entries": 1,
                        "existing_sha_matches": 0,
                        "path_conflicts": 0,
                        "errors": 0,
                    }
                }
            )
        if method == "GET" and url.endswith("/v1/admin/files?limit=200&offset=0"):
            return _Response({"total": 0, "limit": 200, "offset": 0, "items": []})
        raise AssertionError(f"unexpected request: method={method} url={url}")

    monkeypatch.setattr(m4_smoke_check.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(
        "sys.argv",
        [
            "m4_smoke_check.py",
            "--storage-root",
            str(storage_root),
        ],
    )

    with pytest.raises(SystemExit, match="did not enforce required client auth"):
        m4_smoke_check.main()
