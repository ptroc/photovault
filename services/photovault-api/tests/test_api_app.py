from fastapi.testclient import TestClient
from photovault_api.app import create_app


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


def test_upload_content_and_verify_promotes_sha_to_known_registry() -> None:
    content = b"hello-upload"
    sha256_hex = "520d43ad6c18a946d2e5c3a4d81f9d4261cf6a3c12f5a59d9667cc4b336c3550"
    client = TestClient(create_app())

    upload_response = client.put(
        f"/v1/upload/content/{sha256_hex}",
        data=content,
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
