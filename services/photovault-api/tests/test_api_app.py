import hashlib
from datetime import UTC, datetime
from pathlib import Path

import photovault_api.app as app_module
from fastapi.testclient import TestClient
from photovault_api.app import create_app
from photovault_api.state_store import InMemoryUploadStateStore

TINY_PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
    b"\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\x0cIDATx\x9cc`\x00\x00"
    b"\x00\x02\x00\x01\xe5'\xd4\xa2\x00\x00\x00\x00IEND\xaeB`\x82"
)


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

        def list_stored_files(self, *, limit: int, offset: int):
            return 0, []

        def list_duplicate_sha_groups(self, *, limit: int, offset: int):
            return 0, []

        def record_path_conflict(self, **kwargs) -> None:
            return None

        def list_path_conflicts(self, *, limit: int, offset: int):
            return 0, []

        def record_storage_index_run(self, record) -> None:
            return None

        def get_latest_storage_index_run(self):
            return None

        def summarize_storage(self):
            return {
                "total_known_sha256": 0,
                "total_stored_files": 0,
                "indexed_files": 0,
                "uploaded_files": 0,
                "duplicate_file_paths": 0,
                "recent_indexed_files_24h": 0,
                "recent_uploaded_files_24h": 0,
                "last_indexed_at_utc": None,
                "last_uploaded_at_utc": None,
            }

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
    content = TINY_PNG
    sha256_hex = hashlib.sha256(content).hexdigest()
    client = TestClient(create_app(state_store=store, storage_root=tmp_path))

    upload_response = client.put(
        f"/v1/upload/content/{sha256_hex}",
        content=content,
        headers=_upload_headers(
            size_bytes=len(content),
            job_name="Wedding Shoot",
            original_filename="IMG_0001.PNG",
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
    repeat_verify_response = client.post(
        "/v1/upload/verify",
        json={"sha256_hex": sha256_hex, "size_bytes": len(content)},
    )
    assert repeat_verify_response.status_code == 200
    assert repeat_verify_response.json()["status"] == "ALREADY_EXISTS"

    received = datetime.fromisoformat(temp_record.received_at_utc)
    final_path = (
        tmp_path / f"{received.year:04d}" / f"{received.month:02d}" / "Wedding_Shoot" / "IMG_0001.PNG"
    )
    assert final_path.read_bytes() == content
    assert not temp_path.exists()

    handshake_response = client.post(
        "/v1/upload/metadata-handshake",
        json={"files": [{"client_file_id": 1, "sha256_hex": sha256_hex, "size_bytes": len(content)}]},
    )
    assert handshake_response.status_code == 200
    assert handshake_response.json()["results"][0]["decision"] == "ALREADY_EXISTS"

    catalog_response = client.get("/v1/admin/catalog")
    assert catalog_response.status_code == 200
    catalog_payload = catalog_response.json()
    assert catalog_payload["total"] == 1
    assert catalog_payload["items"] == [
        {
            "relative_path": str(final_path.relative_to(tmp_path).as_posix()),
            "sha256_hex": sha256_hex,
            "size_bytes": len(content),
            "origin_kind": "uploaded",
            "last_observed_origin_kind": "uploaded",
            "provenance_job_name": "Wedding Shoot",
            "provenance_original_filename": "IMG_0001.PNG",
            "first_cataloged_at_utc": catalog_payload["items"][0]["first_cataloged_at_utc"],
            "last_cataloged_at_utc": catalog_payload["items"][0]["last_cataloged_at_utc"],
            "extraction_status": "succeeded",
            "extraction_last_attempted_at_utc": catalog_payload["items"][0][
                "extraction_last_attempted_at_utc"
            ],
            "extraction_last_succeeded_at_utc": catalog_payload["items"][0][
                "extraction_last_succeeded_at_utc"
            ],
            "extraction_last_failed_at_utc": None,
            "extraction_failure_detail": None,
            "capture_timestamp_utc": None,
            "camera_make": None,
            "camera_model": None,
            "image_width": 1,
            "image_height": 1,
            "orientation": None,
            "lens_model": None,
        }
    ]


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
    content = TINY_PNG
    sha256_hex = hashlib.sha256(content).hexdigest()
    first = tmp_path / "2026" / "04" / "Job_A" / "a.png"
    second = tmp_path / "2026" / "04" / "Job_B" / "b.png"
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

    catalog_response = client.get("/v1/admin/catalog")
    assert catalog_response.status_code == 200
    catalog_payload = catalog_response.json()
    assert catalog_payload["total"] == 2
    assert [item["relative_path"] for item in catalog_payload["items"]] == [
        "2026/04/Job_A/a.png",
        "2026/04/Job_B/b.png",
    ]
    assert all(item["origin_kind"] == "indexed" for item in catalog_payload["items"])
    assert all(item["last_observed_origin_kind"] == "indexed" for item in catalog_payload["items"])
    assert all(item["extraction_status"] == "succeeded" for item in catalog_payload["items"])
    assert all(item["image_width"] == 1 for item in catalog_payload["items"])
    assert all(item["image_height"] == 1 for item in catalog_payload["items"])


def test_storage_index_records_extraction_failure_without_invalidating_catalog(tmp_path: Path) -> None:
    bad_path = tmp_path / "2026" / "04" / "Job_A" / "note.txt"
    bad_path.parent.mkdir(parents=True, exist_ok=True)
    bad_path.write_text("not-an-image", encoding="utf-8")

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post("/v1/storage/index")
    assert response.status_code == 200
    assert response.json()["indexed_files"] == 1
    assert response.json()["errors"] == 0

    catalog_response = client.get("/v1/admin/catalog")
    assert catalog_response.status_code == 200
    payload = catalog_response.json()
    assert payload["total"] == 1
    item = payload["items"][0]
    assert item["relative_path"] == "2026/04/Job_A/note.txt"
    assert item["extraction_status"] == "failed"
    assert item["extraction_last_failed_at_utc"] is not None
    assert "unsupported media format for extraction" in str(item["extraction_failure_detail"])


def test_storage_index_counts_same_path_conflict_and_updates_metadata(tmp_path: Path) -> None:
    original_content = b"original"
    replacement_content = b"replacement"
    original_sha = hashlib.sha256(original_content).hexdigest()
    replacement_sha = hashlib.sha256(replacement_content).hexdigest()
    target = tmp_path / "2026" / "04" / "Job_A" / "photo.jpg"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(original_content)

    store = InMemoryUploadStateStore()
    client = TestClient(create_app(state_store=store, storage_root=tmp_path))

    first_index = client.post("/v1/storage/index")
    assert first_index.status_code == 200
    assert first_index.json() == {
        "scanned_files": 1,
        "indexed_files": 1,
        "new_sha_entries": 1,
        "existing_sha_matches": 0,
        "path_conflicts": 0,
        "errors": 0,
    }
    assert store.get_stored_file_by_path("2026/04/Job_A/photo.jpg") is not None
    assert store.get_stored_file_by_path("2026/04/Job_A/photo.jpg").sha256_hex == original_sha

    target.write_bytes(replacement_content)

    second_index = client.post("/v1/storage/index")
    assert second_index.status_code == 200
    assert second_index.json() == {
        "scanned_files": 1,
        "indexed_files": 1,
        "new_sha_entries": 1,
        "existing_sha_matches": 0,
        "path_conflicts": 1,
        "errors": 0,
    }

    updated_record = store.get_stored_file_by_path("2026/04/Job_A/photo.jpg")
    assert updated_record is not None
    assert updated_record.sha256_hex == replacement_sha
    assert updated_record.size_bytes == len(replacement_content)


def test_storage_index_ignores_temp_uploads_content(tmp_path: Path) -> None:
    stored_content = b"persisted"
    temp_content = b"temporary"
    stored_sha = hashlib.sha256(stored_content).hexdigest()
    real_file = tmp_path / "2026" / "04" / "Job_A" / "a.jpg"
    temp_file = tmp_path / ".temp_uploads" / "ignored.upload"
    real_file.parent.mkdir(parents=True, exist_ok=True)
    temp_file.parent.mkdir(parents=True, exist_ok=True)
    real_file.write_bytes(stored_content)
    temp_file.write_bytes(temp_content)

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post("/v1/storage/index")

    assert response.status_code == 200
    assert response.json() == {
        "scanned_files": 1,
        "indexed_files": 1,
        "new_sha_entries": 1,
        "existing_sha_matches": 0,
        "path_conflicts": 0,
        "errors": 0,
    }

    handshake_response = client.post(
        "/v1/upload/metadata-handshake",
        json={"files": [{"client_file_id": 9, "sha256_hex": stored_sha, "size_bytes": len(stored_content)}]},
    )
    assert handshake_response.status_code == 200
    assert handshake_response.json()["results"][0]["decision"] == "ALREADY_EXISTS"


def test_storage_index_records_partial_scan_errors_without_aborting(tmp_path: Path, monkeypatch) -> None:
    good_content = b"good"
    bad_content = b"bad"
    good_file = tmp_path / "2026" / "04" / "Job_A" / "good.jpg"
    bad_file = tmp_path / "2026" / "04" / "Job_A" / "bad.jpg"
    good_file.parent.mkdir(parents=True, exist_ok=True)
    good_file.write_bytes(good_content)
    bad_file.write_bytes(bad_content)

    original_compute_sha256 = app_module._compute_sha256

    def _flaky_compute_sha256(path: Path) -> str:
        if path.name == "bad.jpg":
            raise OSError("simulated read failure")
        return original_compute_sha256(path)

    monkeypatch.setattr(app_module, "_compute_sha256", _flaky_compute_sha256)

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post("/v1/storage/index")

    assert response.status_code == 200
    assert response.json() == {
        "scanned_files": 2,
        "indexed_files": 1,
        "new_sha_entries": 1,
        "existing_sha_matches": 0,
        "path_conflicts": 0,
        "errors": 1,
    }


def test_admin_duplicates_returns_duplicate_sha_groups(tmp_path: Path) -> None:
    store = InMemoryUploadStateStore()
    now = "2026-04-21T08:00:00+00:00"
    later = "2026-04-21T09:00:00+00:00"
    store.upsert_stored_file(
        relative_path="2026/04/Job_A/a.jpg",
        sha256_hex="a" * 64,
        size_bytes=10,
        source_kind="index_scan",
        seen_at_utc=now,
    )
    store.upsert_stored_file(
        relative_path="2026/04/Job_B/b.jpg",
        sha256_hex="a" * 64,
        size_bytes=10,
        source_kind="upload_verify",
        seen_at_utc=later,
    )
    store.upsert_stored_file(
        relative_path="2026/04/Job_C/c.jpg",
        sha256_hex="b" * 64,
        size_bytes=11,
        source_kind="index_scan",
        seen_at_utc=later,
    )

    client = TestClient(create_app(state_store=store, storage_root=tmp_path))
    response = client.get("/v1/admin/duplicates")

    assert response.status_code == 200
    assert response.json() == {
        "total": 1,
        "limit": 25,
        "offset": 0,
        "items": [
            {
                "sha256_hex": "a" * 64,
                "file_count": 2,
                "first_seen_at_utc": now,
                "last_seen_at_utc": later,
                "relative_paths": ["2026/04/Job_A/a.jpg", "2026/04/Job_B/b.jpg"],
            }
        ],
    }


def test_admin_path_conflicts_and_latest_run_reflect_index_activity(tmp_path: Path) -> None:
    target = tmp_path / "2026" / "04" / "Job_A" / "photo.jpg"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"first")
    client = TestClient(create_app(storage_root=tmp_path))

    first_index = client.post("/v1/storage/index")
    assert first_index.status_code == 200

    target.write_bytes(b"second")
    second_index = client.post("/v1/storage/index")
    assert second_index.status_code == 200
    assert second_index.json()["path_conflicts"] == 1

    conflict_response = client.get("/v1/admin/path-conflicts")
    assert conflict_response.status_code == 200
    conflict_payload = conflict_response.json()
    assert conflict_payload["total"] == 1
    assert conflict_payload["items"][0]["relative_path"] == "2026/04/Job_A/photo.jpg"
    assert conflict_payload["items"][0]["previous_sha256_hex"] == hashlib.sha256(b"first").hexdigest()
    assert conflict_payload["items"][0]["current_sha256_hex"] == hashlib.sha256(b"second").hexdigest()

    latest_run_response = client.get("/v1/admin/latest-index-run")
    assert latest_run_response.status_code == 200
    latest_run = latest_run_response.json()["latest_run"]
    assert latest_run is not None
    assert latest_run["scanned_files"] == 1
    assert latest_run["indexed_files"] == 1
    assert latest_run["path_conflicts"] == 1
    assert latest_run["errors"] == 0


def test_admin_latest_index_run_is_none_before_any_scan(tmp_path: Path) -> None:
    client = TestClient(create_app(storage_root=tmp_path))
    response = client.get("/v1/admin/latest-index-run")
    assert response.status_code == 200
    assert response.json() == {"latest_run": None}


def test_verify_returns_verify_failed_when_content_not_uploaded(tmp_path: Path) -> None:
    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v1/upload/verify",
        json={"sha256_hex": "f" * 64, "size_bytes": 10},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "VERIFY_FAILED"


def test_admin_overview_reports_storage_summary(tmp_path: Path) -> None:
    store = InMemoryUploadStateStore()
    now = datetime.now(UTC).isoformat()
    older = "2020-01-01T00:00:00+00:00"
    store.mark_sha_verified("a" * 64)
    store.mark_sha_verified("b" * 64)
    store.upsert_stored_file(
        relative_path="2026/04/Job_A/a.jpg",
        sha256_hex="a" * 64,
        size_bytes=10,
        source_kind="index_scan",
        seen_at_utc=now,
    )
    store.upsert_stored_file(
        relative_path="2026/04/Job_B/b.jpg",
        sha256_hex="a" * 64,
        size_bytes=11,
        source_kind="upload_verify",
        seen_at_utc=now,
    )
    store.upsert_stored_file(
        relative_path="2026/04/Job_C/c.jpg",
        sha256_hex="b" * 64,
        size_bytes=12,
        source_kind="index_scan",
        seen_at_utc=older,
    )
    client = TestClient(create_app(state_store=store, storage_root=tmp_path))
    response = client.get("/v1/admin/overview")
    assert response.status_code == 200
    assert response.json() == {
        "total_known_sha256": 2,
        "total_stored_files": 3,
        "indexed_files": 2,
        "uploaded_files": 1,
        "duplicate_file_paths": 1,
        "recent_indexed_files_24h": 1,
        "recent_uploaded_files_24h": 1,
        "last_indexed_at_utc": now,
        "last_uploaded_at_utc": now,
    }


def test_admin_files_returns_paged_results(tmp_path: Path) -> None:
    store = InMemoryUploadStateStore()
    t1 = "2026-04-20T10:00:00+00:00"
    t2 = "2026-04-20T11:00:00+00:00"
    t3 = "2026-04-20T12:00:00+00:00"
    store.upsert_stored_file(
        relative_path="2026/04/Job_A/a.jpg",
        sha256_hex="a" * 64,
        size_bytes=100,
        source_kind="index_scan",
        seen_at_utc=t1,
    )
    store.upsert_stored_file(
        relative_path="2026/04/Job_B/b.jpg",
        sha256_hex="b" * 64,
        size_bytes=200,
        source_kind="upload_verify",
        seen_at_utc=t2,
    )
    store.upsert_stored_file(
        relative_path="2026/04/Job_C/c.jpg",
        sha256_hex="c" * 64,
        size_bytes=300,
        source_kind="index_scan",
        seen_at_utc=t3,
    )

    client = TestClient(create_app(state_store=store, storage_root=tmp_path))
    response = client.get("/v1/admin/files?limit=2&offset=1")
    assert response.status_code == 200
    assert response.json() == {
        "total": 3,
        "limit": 2,
        "offset": 1,
        "items": [
            {
                "relative_path": "2026/04/Job_B/b.jpg",
                "sha256_hex": "b" * 64,
                "size_bytes": 200,
                "source_kind": "upload_verify",
                "first_seen_at_utc": t2,
                "last_seen_at_utc": t2,
            },
            {
                "relative_path": "2026/04/Job_A/a.jpg",
                "sha256_hex": "a" * 64,
                "size_bytes": 100,
                "source_kind": "index_scan",
                "first_seen_at_utc": t1,
                "last_seen_at_utc": t1,
            },
        ],
    }


def test_admin_catalog_returns_paged_results(tmp_path: Path) -> None:
    store = InMemoryUploadStateStore()
    t1 = "2026-04-20T10:00:00+00:00"
    t2 = "2026-04-20T11:00:00+00:00"
    store.upsert_stored_file(
        relative_path="2026/04/Job_A/a.jpg",
        sha256_hex="a" * 64,
        size_bytes=100,
        source_kind="upload_verify",
        seen_at_utc=t1,
    )
    store.upsert_media_asset(
        relative_path="2026/04/Job_A/a.jpg",
        sha256_hex="a" * 64,
        size_bytes=100,
        origin_kind="uploaded",
        observed_at_utc=t1,
        provenance_job_name="Job_A",
        provenance_original_filename="a.jpg",
    )
    store.upsert_stored_file(
        relative_path="2026/04/Job_B/b.jpg",
        sha256_hex="b" * 64,
        size_bytes=200,
        source_kind="index_scan",
        seen_at_utc=t2,
    )
    store.upsert_media_asset(
        relative_path="2026/04/Job_B/b.jpg",
        sha256_hex="b" * 64,
        size_bytes=200,
        origin_kind="indexed",
        observed_at_utc=t2,
    )
    store.upsert_media_asset_extraction(
        relative_path="2026/04/Job_B/b.jpg",
        extraction_status="failed",
        attempted_at_utc=t2,
        succeeded_at_utc=None,
        failed_at_utc=t2,
        failure_detail="unsupported media format for extraction: .jpg",
        capture_timestamp_utc=None,
        camera_make=None,
        camera_model=None,
        image_width=None,
        image_height=None,
        orientation=None,
        lens_model=None,
        recorded_at_utc=t2,
    )

    client = TestClient(create_app(state_store=store, storage_root=tmp_path))
    response = client.get("/v1/admin/catalog?limit=1&offset=0")

    assert response.status_code == 200
    assert response.json() == {
        "total": 2,
        "limit": 1,
        "offset": 0,
        "items": [
            {
                "relative_path": "2026/04/Job_B/b.jpg",
                "sha256_hex": "b" * 64,
                "size_bytes": 200,
                "origin_kind": "indexed",
                "last_observed_origin_kind": "indexed",
                "provenance_job_name": None,
                "provenance_original_filename": None,
                "first_cataloged_at_utc": t2,
                "last_cataloged_at_utc": t2,
                "extraction_status": "failed",
                "extraction_last_attempted_at_utc": t2,
                "extraction_last_succeeded_at_utc": None,
                "extraction_last_failed_at_utc": t2,
                "extraction_failure_detail": "unsupported media format for extraction: .jpg",
                "capture_timestamp_utc": None,
                "camera_make": None,
                "camera_model": None,
                "image_width": None,
                "image_height": None,
                "orientation": None,
                "lens_model": None,
            }
        ],
    }


def test_admin_files_empty_state(tmp_path: Path) -> None:
    client = TestClient(create_app(storage_root=tmp_path))
    response = client.get("/v1/admin/files")
    assert response.status_code == 200
    assert response.json() == {"total": 0, "limit": 50, "offset": 0, "items": []}


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

        def list_stored_files(self, *, limit: int, offset: int):
            return 0, []

        def list_duplicate_sha_groups(self, *, limit: int, offset: int):
            return 0, []

        def record_path_conflict(self, **kwargs) -> None:
            return None

        def list_path_conflicts(self, *, limit: int, offset: int):
            return 0, []

        def record_storage_index_run(self, record) -> None:
            return None

        def get_latest_storage_index_run(self):
            return None

        def summarize_storage(self):
            class _Summary:
                total_known_sha256 = 0
                total_stored_files = 0
                indexed_files = 0
                uploaded_files = 0
                duplicate_file_paths = 0
                recent_indexed_files_24h = 0
                recent_uploaded_files_24h = 0
                last_indexed_at_utc = None
                last_uploaded_at_utc = None

            return _Summary()

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
