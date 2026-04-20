import hashlib
from datetime import datetime
from pathlib import Path

import photovault_api.app as app_module
from fastapi.testclient import TestClient
from photovault_api.app import create_app
from photovault_api.state_store import InMemoryUploadStateStore


def _upload_headers(
    *,
    size_bytes: int,
    job_name: str = "job",
    original_filename: str = "file.jpg",
) -> dict[str, str]:
    return {
        "x-size-bytes": str(size_bytes),
        "x-job-name": job_name,
        "x-original-filename": original_filename,
    }


def test_healthz(tmp_path: Path) -> None:
    client = TestClient(create_app(storage_root=tmp_path))
    response = client.get("/healthz")
    assert response.status_code == 200


def test_metadata_handshake_reports_already_exists_for_known_sha(tmp_path: Path) -> None:
    known_sha = "a" * 64
    client = TestClient(create_app(initial_known_sha256={known_sha}, storage_root=tmp_path))
    response = client.post(
        "/v1/upload/metadata-handshake",
        json={
            "files": [
                {"client_file_id": 1, "sha256_hex": known_sha, "size_bytes": 42},
            ]
        },
    )
    assert response.status_code == 200
    assert response.json() == {
        "results": [
            {"client_file_id": 1, "decision": "ALREADY_EXISTS"},
        ]
    }


def test_metadata_handshake_classifies_mixed_batch_with_single_lookup(tmp_path: Path) -> None:
    known_sha = "a" * 64
    unknown_sha = "b" * 64
    observed: dict[str, object] = {}

    class _BatchStore:
        def initialize(self) -> None:
            return None

        def has_sha(self, sha256_hex: str) -> bool:
            observed["has_sha_called"] = True
            return False

        def has_shas(self, sha256_hex_values: list[str]) -> set[str]:
            observed["lookup"] = list(sha256_hex_values)
            return {known_sha}

        def get_temp_upload(self, sha256_hex: str):
            return None

        def upsert_temp_upload(self, **kwargs) -> None:
            return None

        def mark_sha_verified(self, sha256_hex: str) -> bool:
            return False

        def upsert_stored_file(self, **kwargs) -> None:
            return None

        def get_stored_file_by_path(self, relative_path: str):
            return None

        def remove_temp_upload(self, sha256_hex: str) -> None:
            return None

    client = TestClient(create_app(state_store=_BatchStore(), storage_root=tmp_path))
    response = client.post(
        "/v1/upload/metadata-handshake",
        json={
            "files": [
                {"client_file_id": 1, "sha256_hex": known_sha, "size_bytes": 10},
                {"client_file_id": 2, "sha256_hex": unknown_sha, "size_bytes": 20},
            ]
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "results": [
            {"client_file_id": 1, "decision": "ALREADY_EXISTS"},
            {"client_file_id": 2, "decision": "UPLOAD_REQUIRED"},
        ]
    }
    assert observed["lookup"] == [known_sha, unknown_sha]
    assert observed.get("has_sha_called") is None


def test_upload_content_and_verify_writes_filesystem_and_promotes_sha(tmp_path: Path) -> None:
    store = InMemoryUploadStateStore()
    content = b"hello-upload"
    sha256_hex = hashlib.sha256(content).hexdigest()
    client = TestClient(create_app(state_store=store, storage_root=tmp_path))

    upload_response = client.put(
        f"/v1/upload/content/{sha256_hex}",
        content=content,
        headers=_upload_headers(
            size_bytes=len(content),
            job_name="Wedding Shoot",
            original_filename="IMG_0001.JPG",
        ),
    )
    assert upload_response.status_code == 200
    assert upload_response.json()["status"] == "STORED_TEMP"
    temp_path = tmp_path / ".temp_uploads" / f"{sha256_hex}.upload"
    assert temp_path.read_bytes() == content

    temp_record = store.get_temp_upload(sha256_hex)
    assert temp_record is not None
    verify_response = client.post(
        "/v1/upload/verify",
        json={"sha256_hex": sha256_hex, "size_bytes": len(content)},
    )
    assert verify_response.status_code == 200
    assert verify_response.json()["status"] == "VERIFIED"

    received = datetime.fromisoformat(temp_record.received_at_utc)
    final_path = (
        tmp_path / f"{received.year:04d}" / f"{received.month:02d}" / "Wedding_Shoot" / "IMG_0001.JPG"
    )
    assert final_path.read_bytes() == content
    assert not temp_path.exists()

    handshake_response = client.post(
        "/v1/upload/metadata-handshake",
        json={"files": [{"client_file_id": 1, "sha256_hex": sha256_hex, "size_bytes": len(content)}]},
    )
    assert handshake_response.status_code == 200
    assert handshake_response.json()["results"][0]["decision"] == "ALREADY_EXISTS"


def test_verify_handles_filename_collision_with_deterministic_sha_suffix(tmp_path: Path) -> None:
    store = InMemoryUploadStateStore()
    content = b"new-content"
    sha256_hex = hashlib.sha256(content).hexdigest()
    client = TestClient(create_app(state_store=store, storage_root=tmp_path))

    upload_response = client.put(
        f"/v1/upload/content/{sha256_hex}",
        content=content,
        headers=_upload_headers(size_bytes=len(content), job_name="Trip 1", original_filename="photo.jpg"),
    )
    assert upload_response.status_code == 200
    temp_record = store.get_temp_upload(sha256_hex)
    assert temp_record is not None
    received = datetime.fromisoformat(temp_record.received_at_utc)
    base_dir = tmp_path / f"{received.year:04d}" / f"{received.month:02d}" / "Trip_1"
    base_dir.mkdir(parents=True, exist_ok=True)
    (base_dir / "photo.jpg").write_bytes(b"other-content")

    verify_response = client.post(
        "/v1/upload/verify",
        json={"sha256_hex": sha256_hex, "size_bytes": len(content)},
    )
    assert verify_response.status_code == 200
    assert verify_response.json()["status"] == "VERIFIED"
    assert (base_dir / f"photo__{sha256_hex[:12]}.jpg").read_bytes() == content


def test_storage_index_registers_existing_files_and_reindex_is_idempotent(tmp_path: Path) -> None:
    content = b"same-content"
    sha256_hex = hashlib.sha256(content).hexdigest()
    first = tmp_path / "2026" / "04" / "Job_A" / "a.jpg"
    second = tmp_path / "2026" / "04" / "Job_B" / "b.jpg"
    first.parent.mkdir(parents=True, exist_ok=True)
    second.parent.mkdir(parents=True, exist_ok=True)
    first.write_bytes(content)
    second.write_bytes(content)

    client = TestClient(create_app(storage_root=tmp_path))
    first_index = client.post("/v1/storage/index")
    assert first_index.status_code == 200
    assert first_index.json() == {
        "scanned_files": 2,
        "indexed_files": 2,
        "new_sha_entries": 1,
        "existing_sha_matches": 1,
        "path_conflicts": 0,
        "errors": 0,
    }

    handshake_response = client.post(
        "/v1/upload/metadata-handshake",
        json={"files": [{"client_file_id": 5, "sha256_hex": sha256_hex, "size_bytes": len(content)}]},
    )
    assert handshake_response.status_code == 200
    assert handshake_response.json()["results"][0]["decision"] == "ALREADY_EXISTS"

    second_index = client.post("/v1/storage/index")
    assert second_index.status_code == 200
    assert second_index.json() == {
        "scanned_files": 2,
        "indexed_files": 2,
        "new_sha_entries": 0,
        "existing_sha_matches": 2,
        "path_conflicts": 0,
        "errors": 0,
    }


def test_verify_returns_verify_failed_when_content_not_uploaded(tmp_path: Path) -> None:
    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v1/upload/verify",
        json={"sha256_hex": "f" * 64, "size_bytes": 10},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "VERIFY_FAILED"


def test_create_app_requires_storage_root_when_not_provided(monkeypatch) -> None:
    monkeypatch.delenv("PHOTOVAULT_API_STORAGE_ROOT", raising=False)
    try:
        create_app()
        raise AssertionError("expected RuntimeError when storage root is missing")
    except RuntimeError as exc:
        assert "PHOTOVAULT_API_STORAGE_ROOT" in str(exc)


def test_create_app_uses_postgres_store_when_database_url_env_set(monkeypatch, tmp_path: Path) -> None:
    class _FakePostgresStore:
        def __init__(self, *, database_url: str) -> None:
            self.database_url = database_url
            self.initialized = False

        def initialize(self) -> None:
            self.initialized = True

        def has_sha(self, sha256_hex: str) -> bool:
            return False

        def has_shas(self, sha256_hex_values: list[str]) -> set[str]:
            return set()

        def get_temp_upload(self, sha256_hex: str):
            return None

        def upsert_temp_upload(self, **kwargs) -> None:
            return None

        def mark_sha_verified(self, sha256_hex: str) -> bool:
            return False

        def upsert_stored_file(self, **kwargs) -> None:
            return None

        def get_stored_file_by_path(self, relative_path: str):
            return None

        def remove_temp_upload(self, sha256_hex: str) -> None:
            return None

    monkeypatch.setenv("PHOTOVAULT_API_STORAGE_ROOT", str(tmp_path))
    monkeypatch.setenv("PHOTOVAULT_API_DATABASE_URL", "postgresql://photovault:pw@db/photovault")
    monkeypatch.setattr(app_module, "PostgresUploadStateStore", _FakePostgresStore)

    app = create_app()
    store = app.state.upload_state_store
    assert isinstance(store, _FakePostgresStore)
    assert store.database_url == "postgresql://photovault:pw@db/photovault"
    assert store.initialized is True
