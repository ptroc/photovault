import photovault_api.app as app_module
from fastapi.testclient import TestClient
from photovault_api.app import create_app
from photovault_api.state_store import InMemoryUploadStateStore


def test_healthz() -> None:
    client = TestClient(create_app())
    response = client.get("/healthz")
    assert response.status_code == 200


def test_metadata_handshake_reports_already_exists_for_known_sha() -> None:
    known_sha = "a" * 64
    client = TestClient(create_app(initial_known_sha256={known_sha}))
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


def test_metadata_handshake_reports_upload_required_for_unknown_sha() -> None:
    client = TestClient(create_app(initial_known_sha256={"b" * 64}))
    response = client.post(
        "/v1/upload/metadata-handshake",
        json={
            "files": [
                {"client_file_id": 7, "sha256_hex": "c" * 64, "size_bytes": 99},
            ]
        },
    )
    assert response.status_code == 200
    assert response.json() == {
        "results": [
            {"client_file_id": 7, "decision": "UPLOAD_REQUIRED"},
        ]
    }


def test_metadata_handshake_classifies_mixed_batch_with_single_lookup() -> None:
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

        def get_temp_upload(self, sha256_hex: str) -> tuple[int, bytes] | None:
            return None

        def upsert_temp_upload(self, sha256_hex: str, size_bytes: int, content: bytes) -> None:
            return None

        def mark_sha_verified(self, sha256_hex: str) -> None:
            return None

        def remove_temp_upload(self, sha256_hex: str) -> None:
            return None

    client = TestClient(create_app(state_store=_BatchStore()))
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


def test_upload_content_and_verify_promotes_sha_to_known_registry() -> None:
    content = b"hello-upload"
    sha256_hex = "520d43ad6c18a946d2e5c3a4d81f9d4261cf6a3c12f5a59d9667cc4b336c3550"
    client = TestClient(create_app())

    upload_response = client.put(
        f"/v1/upload/content/{sha256_hex}",
        content=content,
        headers={"x-size-bytes": str(len(content))},
    )
    assert upload_response.status_code == 200
    assert upload_response.json()["status"] == "STORED_TEMP"

    verify_response = client.post(
        "/v1/upload/verify",
        json={"sha256_hex": sha256_hex, "size_bytes": len(content)},
    )
    assert verify_response.status_code == 200
    assert verify_response.json()["status"] == "VERIFIED"

    handshake_response = client.post(
        "/v1/upload/metadata-handshake",
        json={
            "files": [
                {"client_file_id": 1, "sha256_hex": sha256_hex, "size_bytes": len(content)},
            ]
        },
    )
    assert handshake_response.status_code == 200
    assert handshake_response.json()["results"][0]["decision"] == "ALREADY_EXISTS"


def test_verify_returns_verify_failed_when_content_not_uploaded() -> None:
    client = TestClient(create_app())
    response = client.post(
        "/v1/upload/verify",
        json={"sha256_hex": "f" * 64, "size_bytes": 10},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "VERIFY_FAILED"


def test_known_sha_and_temp_upload_persist_across_app_restart_with_shared_store() -> None:
    store = InMemoryUploadStateStore()
    content = b"restart-persist"
    sha256_hex = "7829033a1d930735112bd3343bfca98908306b3add85360c790c8f62b15dfe35"

    first_client = TestClient(create_app(state_store=store))
    upload_response = first_client.put(
        f"/v1/upload/content/{sha256_hex}",
        content=content,
        headers={"x-size-bytes": str(len(content))},
    )
    assert upload_response.status_code == 200
    assert upload_response.json()["status"] == "STORED_TEMP"

    second_client = TestClient(create_app(state_store=store))
    verify_response = second_client.post(
        "/v1/upload/verify",
        json={"sha256_hex": sha256_hex, "size_bytes": len(content)},
    )
    assert verify_response.status_code == 200
    assert verify_response.json()["status"] == "VERIFIED"

    third_client = TestClient(create_app(state_store=store))
    handshake_response = third_client.post(
        "/v1/upload/metadata-handshake",
        json={"files": [{"client_file_id": 9, "sha256_hex": sha256_hex, "size_bytes": len(content)}]},
    )
    assert handshake_response.status_code == 200
    assert handshake_response.json()["results"][0]["decision"] == "ALREADY_EXISTS"


def test_create_app_uses_postgres_store_when_database_url_env_set(monkeypatch) -> None:
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

        def get_temp_upload(self, sha256_hex: str) -> tuple[int, bytes] | None:
            return None

        def upsert_temp_upload(self, sha256_hex: str, size_bytes: int, content: bytes) -> None:
            return None

        def mark_sha_verified(self, sha256_hex: str) -> None:
            return None

        def remove_temp_upload(self, sha256_hex: str) -> None:
            return None

    monkeypatch.setenv("PHOTOVAULT_API_DATABASE_URL", "postgresql://photovault:pw@db/photovault")
    monkeypatch.setattr(app_module, "PostgresUploadStateStore", _FakePostgresStore)

    app = create_app()
    store = app.state.upload_state_store
    assert isinstance(store, _FakePostgresStore)
    assert store.database_url == "postgresql://photovault:pw@db/photovault"
    assert store.initialized is True
