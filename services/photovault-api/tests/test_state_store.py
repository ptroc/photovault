from photovault_api.state_store import PostgresUploadStateStore


def test_in_memory_upsert_stored_file_preserves_first_seen_and_updates_latest_metadata() -> None:
    from photovault_api.state_store import InMemoryUploadStateStore

    store = InMemoryUploadStateStore()

    store.upsert_stored_file(
        relative_path="2026/04/job/photo.jpg",
        sha256_hex="a" * 64,
        size_bytes=12,
        source_kind="index_scan",
        seen_at_utc="2026-04-20T12:01:00+00:00",
    )
    store.upsert_stored_file(
        relative_path="2026/04/job/photo.jpg",
        sha256_hex="b" * 64,
        size_bytes=24,
        source_kind="index_scan",
        seen_at_utc="2026-04-20T12:05:00+00:00",
    )

    record = store.get_stored_file_by_path("2026/04/job/photo.jpg")
    assert record is not None
    assert record.first_seen_at_utc == "2026-04-20T12:01:00+00:00"
    assert record.last_seen_at_utc == "2026-04-20T12:05:00+00:00"
    assert record.sha256_hex == "b" * 64
    assert record.size_bytes == 24


def test_in_memory_upsert_media_asset_preserves_origin_and_first_cataloged_metadata() -> None:
    from photovault_api.state_store import InMemoryUploadStateStore

    store = InMemoryUploadStateStore()
    store.upsert_media_asset(
        relative_path="2026/04/job/photo.jpg",
        sha256_hex="a" * 64,
        size_bytes=12,
        origin_kind="uploaded",
        observed_at_utc="2026-04-20T12:01:00+00:00",
        provenance_job_name="Job_A",
        provenance_original_filename="photo.jpg",
    )
    store.upsert_media_asset(
        relative_path="2026/04/job/photo.jpg",
        sha256_hex="b" * 64,
        size_bytes=24,
        origin_kind="indexed",
        observed_at_utc="2026-04-20T12:05:00+00:00",
    )

    total, rows = store.list_media_assets(limit=10, offset=0)
    assert total == 1
    row = rows[0]
    assert row.origin_kind == "uploaded"
    assert row.last_observed_origin_kind == "indexed"
    assert row.first_cataloged_at_utc == "2026-04-20T12:01:00+00:00"
    assert row.last_cataloged_at_utc == "2026-04-20T12:05:00+00:00"
    assert row.provenance_job_name == "Job_A"
    assert row.provenance_original_filename == "photo.jpg"
    assert row.sha256_hex == "b" * 64
    assert row.size_bytes == 24
    assert row.extraction_status == "pending"
    assert row.image_width is None


def test_in_memory_media_asset_extraction_updates_status_and_metadata() -> None:
    from photovault_api.state_store import InMemoryUploadStateStore

    store = InMemoryUploadStateStore()
    observed_at = "2026-04-20T12:01:00+00:00"
    store.upsert_media_asset(
        relative_path="2026/04/job/photo.png",
        sha256_hex="a" * 64,
        size_bytes=99,
        origin_kind="indexed",
        observed_at_utc=observed_at,
    )
    store.upsert_media_asset_extraction(
        relative_path="2026/04/job/photo.png",
        extraction_status="succeeded",
        attempted_at_utc=observed_at,
        succeeded_at_utc=observed_at,
        failed_at_utc=None,
        failure_detail=None,
        capture_timestamp_utc=None,
        camera_make=None,
        camera_model=None,
        image_width=4000,
        image_height=3000,
        orientation=1,
        lens_model=None,
        recorded_at_utc=observed_at,
    )

    total, rows = store.list_media_assets(limit=10, offset=0)
    assert total == 1
    assert rows[0].extraction_status == "succeeded"
    assert rows[0].image_width == 4000
    assert rows[0].image_height == 3000
    assert rows[0].orientation == 1


def test_in_memory_media_asset_preview_updates_status_path_and_failure_detail() -> None:
    from photovault_api.state_store import InMemoryUploadStateStore

    store = InMemoryUploadStateStore()
    observed_at = "2026-04-20T12:01:00+00:00"
    store.upsert_media_asset(
        relative_path="2026/04/job/photo.png",
        sha256_hex="a" * 64,
        size_bytes=99,
        origin_kind="indexed",
        observed_at_utc=observed_at,
    )
    store.upsert_media_asset_preview(
        relative_path="2026/04/job/photo.png",
        preview_status="failed",
        preview_relative_path=None,
        attempted_at_utc=observed_at,
        succeeded_at_utc=None,
        failed_at_utc=observed_at,
        failure_detail="preview generation failed: unsupported",
        recorded_at_utc=observed_at,
    )

    record = store.get_media_asset_by_path("2026/04/job/photo.png")
    assert record is not None
    assert record.preview_status == "failed"
    assert record.preview_relative_path is None
    assert record.preview_failure_detail == "preview generation failed: unsupported"

    store.upsert_media_asset_preview(
        relative_path="2026/04/job/photo.png",
        preview_status="succeeded",
        preview_relative_path="2026/04/job/photo__abc123__w1024.jpg",
        attempted_at_utc="2026-04-20T12:02:00+00:00",
        succeeded_at_utc="2026-04-20T12:02:00+00:00",
        failed_at_utc=None,
        failure_detail=None,
        recorded_at_utc="2026-04-20T12:02:00+00:00",
    )
    updated = store.get_media_asset_by_path("2026/04/job/photo.png")
    assert updated is not None
    assert updated.preview_status == "succeeded"
    assert updated.preview_relative_path == "2026/04/job/photo__abc123__w1024.jpg"
    assert updated.preview_failure_detail is None


def test_in_memory_client_lifecycle_pending_approve_revoke() -> None:
    from photovault_api.state_store import InMemoryUploadStateStore

    store = InMemoryUploadStateStore()
    pending = store.upsert_client_pending(
        client_id="pi-kitchen",
        display_name="Kitchen Pi",
        enrolled_at_utc="2026-04-22T10:00:00+00:00",
    )
    assert pending.enrollment_status == "pending"
    assert pending.auth_token is None

    approved = store.approve_client(
        client_id="pi-kitchen",
        approved_at_utc="2026-04-22T10:05:00+00:00",
        auth_token="token-1",
    )
    assert approved is not None
    assert approved.enrollment_status == "approved"
    assert approved.auth_token == "token-1"
    assert approved.approved_at_utc == "2026-04-22T10:05:00+00:00"

    revoked = store.revoke_client(
        client_id="pi-kitchen",
        revoked_at_utc="2026-04-22T10:07:00+00:00",
    )
    assert revoked is not None
    assert revoked.enrollment_status == "revoked"
    assert revoked.auth_token == "token-1"
    assert revoked.revoked_at_utc == "2026-04-22T10:07:00+00:00"


def test_in_memory_upsert_client_pending_preserves_approved_status_and_token() -> None:
    from photovault_api.state_store import InMemoryUploadStateStore

    store = InMemoryUploadStateStore()
    store.upsert_client_pending(
        client_id="pi-kitchen",
        display_name="Kitchen Pi",
        enrolled_at_utc="2026-04-22T10:00:00+00:00",
    )
    approved = store.approve_client(
        client_id="pi-kitchen",
        approved_at_utc="2026-04-22T10:05:00+00:00",
        auth_token="token-1",
    )
    assert approved is not None

    seen_again = store.upsert_client_pending(
        client_id="pi-kitchen",
        display_name="Kitchen Pi Updated",
        enrolled_at_utc="2026-04-22T10:10:00+00:00",
    )
    assert seen_again.enrollment_status == "approved"
    assert seen_again.auth_token == "token-1"
    assert seen_again.display_name == "Kitchen Pi Updated"


def test_in_memory_client_heartbeat_upsert_and_fetch_latest_snapshot() -> None:
    from photovault_api.state_store import InMemoryUploadStateStore

    store = InMemoryUploadStateStore()
    first = store.upsert_client_heartbeat(
        client_id="pi-kitchen",
        last_seen_at_utc="2026-04-22T10:00:00+00:00",
        daemon_state="WAIT_NETWORK",
        workload_status="waiting",
        active_job_id=1,
        active_job_label="SD-A",
        active_job_status="UPLOAD_PREPARE",
        active_job_ready_to_upload=4,
        active_job_uploaded=0,
        active_job_retrying=1,
        active_job_total_files=9,
        active_job_non_terminal_files=5,
        active_job_error_files=1,
        active_job_blocking_reason=None,
        retry_pending_count=2,
        retry_next_at_utc="2026-04-22T10:01:00+00:00",
        retry_reason="upload offline",
        auth_block_reason=None,
        recent_error_category="UPLOAD_RETRY_SCHEDULED",
        recent_error_message="temporary upload failure",
        recent_error_at_utc="2026-04-22T09:59:00+00:00",
        updated_at_utc="2026-04-22T10:00:00+00:00",
    )
    assert first.daemon_state == "WAIT_NETWORK"

    second = store.upsert_client_heartbeat(
        client_id="pi-kitchen",
        last_seen_at_utc="2026-04-22T10:02:00+00:00",
        daemon_state="UPLOAD_FILE",
        workload_status="working",
        active_job_id=1,
        active_job_label="SD-A",
        active_job_status="UPLOAD_FILE",
        active_job_ready_to_upload=3,
        active_job_uploaded=1,
        active_job_retrying=0,
        active_job_total_files=9,
        active_job_non_terminal_files=4,
        active_job_error_files=0,
        active_job_blocking_reason=None,
        retry_pending_count=1,
        retry_next_at_utc=None,
        retry_reason="n/a",
        auth_block_reason=None,
        recent_error_category=None,
        recent_error_message=None,
        recent_error_at_utc=None,
        updated_at_utc="2026-04-22T10:02:00+00:00",
    )
    assert second.daemon_state == "UPLOAD_FILE"

    fetched = store.get_client_heartbeat("pi-kitchen")
    assert fetched is not None
    assert fetched.last_seen_at_utc == "2026-04-22T10:02:00+00:00"
    assert fetched.workload_status == "working"
    assert fetched.active_job_uploaded == 1
    assert fetched.active_job_total_files == 9
    assert fetched.active_job_non_terminal_files == 4
    assert fetched.active_job_error_files == 0


def test_postgres_upsert_client_pending_uses_conflict_update() -> None:
    observed: dict[str, object] = {}

    class _FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def execute(self, query: str, params: tuple[object, ...]) -> None:
            observed["query"] = query
            observed["params"] = params

        def fetchone(self):
            return (
                "pi-kitchen",
                "Kitchen Pi",
                "pending",
                "2026-04-22T10:00:00+00:00",
                "2026-04-22T10:00:00+00:00",
                None,
                None,
                None,
            )

    class _FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def cursor(self) -> _FakeCursor:
            return _FakeCursor()

        def commit(self) -> None:
            observed["committed"] = True

    class _TestStore(PostgresUploadStateStore):
        def _connect(self):  # type: ignore[override]
            return _FakeConnection()

    store = _TestStore(database_url="postgresql://unused")
    record = store.upsert_client_pending(
        client_id="pi-kitchen",
        display_name="Kitchen Pi",
        enrolled_at_utc="2026-04-22T10:00:00+00:00",
    )

    assert "INSERT INTO api_clients" in str(observed["query"])
    assert "ON CONFLICT (client_id) DO UPDATE" in str(observed["query"])
    assert observed["params"] == (
        "pi-kitchen",
        "Kitchen Pi",
        "2026-04-22T10:00:00+00:00",
        "2026-04-22T10:00:00+00:00",
    )
    assert observed["committed"] is True
    assert record.enrollment_status == "pending"


def test_postgres_upsert_client_heartbeat_uses_conflict_update() -> None:
    observed: dict[str, object] = {}

    class _FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def execute(self, query: str, params: tuple[object, ...]) -> None:
            observed["query"] = query
            observed["params"] = params

        def fetchone(self):
            return (
                "pi-kitchen",
                "2026-04-22T10:00:00+00:00",
                "WAIT_NETWORK",
                "waiting",
                3,
                "SD-A",
                "UPLOAD_PREPARE",
                4,
                0,
                1,
                9,
                5,
                1,
                "WAIT_NETWORK",
                2,
                "2026-04-22T10:01:00+00:00",
                "upload offline",
                None,
                "UPLOAD_RETRY_SCHEDULED",
                "temporary upload failure",
                "2026-04-22T09:59:00+00:00",
                "2026-04-22T10:00:00+00:00",
            )

    class _FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def cursor(self) -> _FakeCursor:
            return _FakeCursor()

        def commit(self) -> None:
            observed["committed"] = True

    class _TestStore(PostgresUploadStateStore):
        def _connect(self):  # type: ignore[override]
            return _FakeConnection()

    store = _TestStore(database_url="postgresql://unused")
    record = store.upsert_client_heartbeat(
        client_id="pi-kitchen",
        last_seen_at_utc="2026-04-22T10:00:00+00:00",
        daemon_state="WAIT_NETWORK",
        workload_status="waiting",
        active_job_id=3,
        active_job_label="SD-A",
        active_job_status="UPLOAD_PREPARE",
        active_job_ready_to_upload=4,
        active_job_uploaded=0,
        active_job_retrying=1,
        active_job_total_files=9,
        active_job_non_terminal_files=5,
        active_job_error_files=1,
        active_job_blocking_reason="WAIT_NETWORK",
        retry_pending_count=2,
        retry_next_at_utc="2026-04-22T10:01:00+00:00",
        retry_reason="upload offline",
        auth_block_reason=None,
        recent_error_category="UPLOAD_RETRY_SCHEDULED",
        recent_error_message="temporary upload failure",
        recent_error_at_utc="2026-04-22T09:59:00+00:00",
        updated_at_utc="2026-04-22T10:00:00+00:00",
    )

    assert "INSERT INTO api_client_heartbeats" in str(observed["query"])
    assert "ON CONFLICT (client_id) DO UPDATE" in str(observed["query"])
    assert observed["params"] == (
        "pi-kitchen",
        "2026-04-22T10:00:00+00:00",
        "WAIT_NETWORK",
        "waiting",
        3,
        "SD-A",
        "UPLOAD_PREPARE",
        4,
        0,
        1,
        9,
        5,
        1,
        "WAIT_NETWORK",
        2,
        "2026-04-22T10:01:00+00:00",
        "upload offline",
        None,
        "UPLOAD_RETRY_SCHEDULED",
        "temporary upload failure",
        "2026-04-22T09:59:00+00:00",
        "2026-04-22T10:00:00+00:00",
    )
    assert observed["committed"] is True
    assert record.client_id == "pi-kitchen"
    assert record.daemon_state == "WAIT_NETWORK"
    assert record.active_job_total_files == 9
    assert record.active_job_non_terminal_files == 5
    assert record.active_job_error_files == 1


def test_postgres_has_shas_uses_single_query_and_returns_known_set() -> None:
    observed: dict[str, object] = {"connect_calls": 0}

    class _FakeCursor:
        def __init__(self) -> None:
            self._rows = [("a" * 64,), ("c" * 64,)]

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def execute(self, query: str, params: tuple[object, ...]) -> None:
            observed["query"] = query
            observed["params"] = params

        def fetchall(self) -> list[tuple[str]]:
            return list(self._rows)

    class _FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def cursor(self) -> _FakeCursor:
            return _FakeCursor()

    class _TestStore(PostgresUploadStateStore):
        def _connect(self):  # type: ignore[override]
            observed["connect_calls"] = int(observed["connect_calls"]) + 1
            return _FakeConnection()

    store = _TestStore(database_url="postgresql://unused")
    result = store.has_shas(["a" * 64, "b" * 64, "a" * 64, "c" * 64])

    assert result == {"a" * 64, "c" * 64}
    assert observed["connect_calls"] == 1
    assert "WHERE sha256_hex = ANY(%s)" in str(observed["query"])
    assert observed["params"] == (["a" * 64, "b" * 64, "c" * 64],)


def test_postgres_has_shas_returns_empty_set_without_database_call_for_empty_input() -> None:
    observed: dict[str, int] = {"connect_calls": 0}

    class _TestStore(PostgresUploadStateStore):
        def _connect(self):  # type: ignore[override]
            observed["connect_calls"] += 1
            raise AssertionError("should not connect for empty lookup")

    store = _TestStore(database_url="postgresql://unused")
    assert store.has_shas([]) == set()
    assert observed["connect_calls"] == 0


def test_postgres_upsert_temp_upload_metadata_uses_conflict_update() -> None:
    observed: dict[str, object] = {}

    class _FakeCursor:
        rowcount = 1

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def execute(self, query: str, params: tuple[object, ...]) -> None:
            observed["query"] = query
            observed["params"] = params

    class _FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def cursor(self) -> _FakeCursor:
            return _FakeCursor()

        def commit(self) -> None:
            observed["committed"] = True

    class _TestStore(PostgresUploadStateStore):
        def _connect(self):  # type: ignore[override]
            return _FakeConnection()

    store = _TestStore(database_url="postgresql://unused")
    store.upsert_temp_upload(
        sha256_hex="a" * 64,
        size_bytes=99,
        temp_relative_path=".temp_uploads/a.upload",
        job_name="job",
        original_filename="photo.jpg",
        received_at_utc="2026-04-20T12:00:00+00:00",
    )

    assert "ON CONFLICT (sha256_hex) DO UPDATE" in str(observed["query"])
    params = observed["params"]
    assert isinstance(params, tuple)
    assert params[0] == "a" * 64
    assert params[1] == 99
    assert params[2] == ".temp_uploads/a.upload"
    assert params[3] == "job"
    assert params[4] == "photo.jpg"
    assert params[5] == "2026-04-20T12:00:00+00:00"
    assert observed["committed"] is True


def test_postgres_upsert_stored_file_metadata_uses_conflict_update() -> None:
    observed: dict[str, object] = {}

    class _FakeCursor:
        rowcount = 1

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def execute(self, query: str, params: tuple[object, ...]) -> None:
            observed["query"] = query
            observed["params"] = params

    class _FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def cursor(self) -> _FakeCursor:
            return _FakeCursor()

        def commit(self) -> None:
            observed["committed"] = True

    class _TestStore(PostgresUploadStateStore):
        def _connect(self):  # type: ignore[override]
            return _FakeConnection()

    store = _TestStore(database_url="postgresql://unused")
    store.upsert_stored_file(
        relative_path="2026/04/job/photo.jpg",
        sha256_hex="b" * 64,
        size_bytes=12,
        source_kind="index_scan",
        seen_at_utc="2026-04-20T12:01:00+00:00",
    )

    assert "ON CONFLICT (relative_path) DO UPDATE" in str(observed["query"])
    assert observed["params"] == (
        "2026/04/job/photo.jpg",
        "b" * 64,
        12,
        "index_scan",
        "2026-04-20T12:01:00+00:00",
        "2026-04-20T12:01:00+00:00",
    )
    assert observed["committed"] is True


def test_postgres_upsert_stored_file_uses_latest_metadata_but_preserves_first_seen() -> None:
    observed: dict[str, object] = {}

    class _FakeCursor:
        rowcount = 1

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def execute(self, query: str, params: tuple[object, ...]) -> None:
            observed["query"] = query
            observed["params"] = params

    class _FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def cursor(self) -> _FakeCursor:
            return _FakeCursor()

        def commit(self) -> None:
            observed["committed"] = True

    class _TestStore(PostgresUploadStateStore):
        def _connect(self):  # type: ignore[override]
            return _FakeConnection()

    store = _TestStore(database_url="postgresql://unused")
    store.upsert_stored_file(
        relative_path="2026/04/job/photo.jpg",
        sha256_hex="c" * 64,
        size_bytes=99,
        source_kind="upload_verify",
        seen_at_utc="2026-04-20T12:09:00+00:00",
    )

    query = str(observed["query"])
    assert "first_seen_at_utc" in query
    assert "ON CONFLICT (relative_path) DO UPDATE" in query
    assert "last_seen_at_utc = EXCLUDED.last_seen_at_utc" in query
    assert "first_seen_at_utc = EXCLUDED.first_seen_at_utc" not in query
    assert observed["committed"] is True


def test_postgres_upsert_media_asset_uses_conflict_update_and_preserves_origin() -> None:
    observed: dict[str, object] = {}

    class _FakeCursor:
        rowcount = 1

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def execute(self, query: str, params: tuple[object, ...]) -> None:
            observed["query"] = query
            observed["params"] = params

    class _FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def cursor(self) -> _FakeCursor:
            return _FakeCursor()

        def commit(self) -> None:
            observed["committed"] = True

    class _TestStore(PostgresUploadStateStore):
        def _connect(self):  # type: ignore[override]
            return _FakeConnection()

    store = _TestStore(database_url="postgresql://unused")
    store.upsert_media_asset(
        relative_path="2026/04/job/photo.jpg",
        sha256_hex="d" * 64,
        size_bytes=33,
        origin_kind="uploaded",
        observed_at_utc="2026-04-20T12:11:00+00:00",
        provenance_job_name="Job_B",
        provenance_original_filename="photo.jpg",
    )

    query = str(observed["query"])
    assert "ON CONFLICT (relative_path) DO UPDATE" in query
    assert "last_observed_origin_kind = EXCLUDED.last_observed_origin_kind" in query
    assert "origin_kind = EXCLUDED.origin_kind" not in query
    assert "COALESCE(" in query
    assert observed["params"] == (
        "2026/04/job/photo.jpg",
        "d" * 64,
        33,
        "uploaded",
        "uploaded",
        "Job_B",
        "photo.jpg",
        "2026-04-20T12:11:00+00:00",
        "2026-04-20T12:11:00+00:00",
    )
    assert observed["committed"] is True


def test_postgres_upsert_media_asset_extraction_uses_conflict_update() -> None:
    observed: dict[str, object] = {}

    class _FakeCursor:
        rowcount = 1

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def execute(self, query: str, params: tuple[object, ...]) -> None:
            observed["query"] = query
            observed["params"] = params

    class _FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def cursor(self) -> _FakeCursor:
            return _FakeCursor()

        def commit(self) -> None:
            observed["committed"] = True

    class _TestStore(PostgresUploadStateStore):
        def _connect(self):  # type: ignore[override]
            return _FakeConnection()

    store = _TestStore(database_url="postgresql://unused")
    store.upsert_media_asset_extraction(
        relative_path="2026/04/job/photo.png",
        extraction_status="failed",
        attempted_at_utc="2026-04-20T12:15:00+00:00",
        succeeded_at_utc=None,
        failed_at_utc="2026-04-20T12:15:00+00:00",
        failure_detail="unsupported media format",
        capture_timestamp_utc=None,
        camera_make=None,
        camera_model=None,
        image_width=None,
        image_height=None,
        orientation=None,
        lens_model=None,
        recorded_at_utc="2026-04-20T12:15:00+00:00",
    )

    query = str(observed["query"])
    assert "ON CONFLICT (relative_path) DO UPDATE" in query
    assert "extraction_status = EXCLUDED.extraction_status" in query
    assert observed["params"] == (
        "2026/04/job/photo.png",
        "failed",
        "2026-04-20T12:15:00+00:00",
        None,
        "2026-04-20T12:15:00+00:00",
        "unsupported media format",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        "2026-04-20T12:15:00+00:00",
    )
    assert observed["committed"] is True


def test_postgres_upsert_media_asset_preview_uses_conflict_update() -> None:
    observed: dict[str, object] = {}

    class _FakeCursor:
        rowcount = 1

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def execute(self, query: str, params: tuple[object, ...]) -> None:
            observed["query"] = query
            observed["params"] = params

    class _FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def cursor(self) -> _FakeCursor:
            return _FakeCursor()

        def commit(self) -> None:
            observed["committed"] = True

    class _TestStore(PostgresUploadStateStore):
        def _connect(self):  # type: ignore[override]
            return _FakeConnection()

    store = _TestStore(database_url="postgresql://unused")
    store.upsert_media_asset_preview(
        relative_path="2026/04/job/photo.jpg",
        preview_status="succeeded",
        preview_relative_path="2026/04/job/photo__abc__w1024.jpg",
        attempted_at_utc="2026-04-20T12:16:00+00:00",
        succeeded_at_utc="2026-04-20T12:16:00+00:00",
        failed_at_utc=None,
        failure_detail=None,
        recorded_at_utc="2026-04-20T12:16:00+00:00",
    )

    query = str(observed["query"])
    assert "ON CONFLICT (relative_path) DO UPDATE" in query
    assert "preview_status = EXCLUDED.preview_status" in query
    assert observed["params"] == (
        "2026/04/job/photo.jpg",
        "succeeded",
        "2026/04/job/photo__abc__w1024.jpg",
        "2026-04-20T12:16:00+00:00",
        "2026-04-20T12:16:00+00:00",
        None,
        None,
        "2026-04-20T12:16:00+00:00",
    )
    assert observed["committed"] is True


def test_in_memory_media_asset_lookup_and_extraction_selection() -> None:
    from photovault_api.state_store import InMemoryUploadStateStore

    store = InMemoryUploadStateStore()
    t1 = "2026-04-20T12:00:00+00:00"
    t2 = "2026-04-20T12:05:00+00:00"
    t3 = "2026-04-20T12:10:00+00:00"
    store.upsert_media_asset(
        relative_path="2026/04/job/a.jpg",
        sha256_hex="a" * 64,
        size_bytes=10,
        origin_kind="indexed",
        observed_at_utc=t1,
    )
    store.upsert_media_asset(
        relative_path="2026/04/job/b.jpg",
        sha256_hex="b" * 64,
        size_bytes=20,
        origin_kind="indexed",
        observed_at_utc=t2,
    )
    store.upsert_media_asset(
        relative_path="2026/04/job/c.jpg",
        sha256_hex="c" * 64,
        size_bytes=30,
        origin_kind="indexed",
        observed_at_utc=t3,
    )
    store.upsert_media_asset_extraction(
        relative_path="2026/04/job/b.jpg",
        extraction_status="failed",
        attempted_at_utc=t2,
        succeeded_at_utc=None,
        failed_at_utc=t2,
        failure_detail="boom",
        capture_timestamp_utc=None,
        camera_make=None,
        camera_model=None,
        image_width=None,
        image_height=None,
        orientation=None,
        lens_model=None,
        recorded_at_utc=t2,
    )
    store.upsert_media_asset_extraction(
        relative_path="2026/04/job/c.jpg",
        extraction_status="succeeded",
        attempted_at_utc=t3,
        succeeded_at_utc=t3,
        failed_at_utc=None,
        failure_detail=None,
        capture_timestamp_utc=None,
        camera_make=None,
        camera_model=None,
        image_width=100,
        image_height=200,
        orientation=None,
        lens_model=None,
        recorded_at_utc=t3,
    )

    looked_up = store.get_media_asset_by_path("2026/04/job/b.jpg")
    assert looked_up is not None
    assert looked_up.extraction_status == "failed"
    assert looked_up.extraction_failure_detail == "boom"

    selection = store.list_media_assets_for_extraction(
        extraction_statuses=["pending", "failed"],
        limit=10,
    )
    assert [item.relative_path for item in selection] == [
        "2026/04/job/b.jpg",
        "2026/04/job/a.jpg",
    ]


def test_postgres_get_media_asset_by_path_uses_joined_extraction_row() -> None:
    observed: dict[str, object] = {}

    class _FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def execute(self, query: str, params: tuple[object, ...]) -> None:
            observed["query"] = query
            observed["params"] = params

        def fetchone(self):
            return (
                "2026/04/job/photo.jpg",
                "a" * 64,
                12,
                "indexed",
                "indexed",
                None,
                None,
                "2026-04-20T12:00:00+00:00",
                "2026-04-20T12:00:00+00:00",
                "failed",
                "2026-04-20T12:01:00+00:00",
                None,
                "2026-04-20T12:01:00+00:00",
                "bad file",
                "failed",
                None,
                "2026-04-20T12:02:00+00:00",
                None,
                "2026-04-20T12:02:00+00:00",
                "preview error",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )

    class _FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def cursor(self) -> _FakeCursor:
            return _FakeCursor()

    class _TestStore(PostgresUploadStateStore):
        def _connect(self):  # type: ignore[override]
            return _FakeConnection()

    store = _TestStore(database_url="postgresql://unused")
    record = store.get_media_asset_by_path("2026/04/job/photo.jpg")

    assert record is not None
    assert record.relative_path == "2026/04/job/photo.jpg"
    assert record.extraction_status == "failed"
    assert record.extraction_failure_detail == "bad file"
    assert record.preview_status == "failed"
    assert record.preview_failure_detail == "preview error"
    assert "LEFT JOIN api_media_asset_extractions" in str(observed["query"])
    assert "LEFT JOIN api_media_asset_previews" in str(observed["query"])
    assert observed["params"] == ("2026/04/job/photo.jpg",)


def test_postgres_list_media_assets_for_extraction_filters_by_status_and_limit() -> None:
    observed: dict[str, object] = {}

    class _FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def execute(self, query: str, params: tuple[object, ...]) -> None:
            observed["query"] = query
            observed["params"] = params

        def fetchall(self) -> list[tuple[object, ...]]:
            return [
                (
                    "2026/04/job/pending.jpg",
                    "a" * 64,
                    10,
                    "indexed",
                    "indexed",
                    None,
                    None,
                    "2026-04-20T12:00:00+00:00",
                    "2026-04-20T12:00:00+00:00",
                    "pending",
                    None,
                    None,
                    None,
                    None,
                    "pending",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ),
                (
                    "2026/04/job/failed.jpg",
                    "b" * 64,
                    20,
                    "indexed",
                    "indexed",
                    None,
                    None,
                    "2026-04-20T12:05:00+00:00",
                    "2026-04-20T12:05:00+00:00",
                    "failed",
                    "2026-04-20T12:06:00+00:00",
                    None,
                    "2026-04-20T12:06:00+00:00",
                    "broken",
                    "succeeded",
                    "cache/2026/04/job/failed.jpg__preview.jpg",
                    "2026-04-20T12:07:00+00:00",
                    "2026-04-20T12:07:00+00:00",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ),
            ]

    class _FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def cursor(self) -> _FakeCursor:
            return _FakeCursor()

    class _TestStore(PostgresUploadStateStore):
        def _connect(self):  # type: ignore[override]
            return _FakeConnection()

    store = _TestStore(database_url="postgresql://unused")
    rows = store.list_media_assets_for_extraction(extraction_statuses=["pending", "failed"], limit=5)

    assert len(rows) == 2
    assert rows[0].extraction_status == "pending"
    assert rows[1].extraction_status == "failed"
    assert rows[0].preview_status == "pending"
    assert rows[1].preview_status == "succeeded"
    assert "COALESCE(me.extraction_status, 'pending') = ANY(%s)" in str(observed["query"])
    assert observed["params"] == (["pending", "failed"], 5)


def test_in_memory_list_media_assets_supports_status_origin_and_catalog_date_filters() -> None:
    from photovault_api.state_store import InMemoryUploadStateStore

    store = InMemoryUploadStateStore()
    t1 = "2026-04-22T09:00:00+00:00"
    t2 = "2026-04-22T10:00:00+00:00"
    t3 = "2026-04-22T11:00:00+00:00"

    store.upsert_media_asset(
        relative_path="2026/04/job/pending.jpg",
        sha256_hex="a" * 64,
        size_bytes=10,
        origin_kind="indexed",
        observed_at_utc=t1,
    )
    store.upsert_media_asset(
        relative_path="2026/04/job/failed.jpg",
        sha256_hex="b" * 64,
        size_bytes=20,
        origin_kind="uploaded",
        observed_at_utc=t2,
    )
    store.upsert_media_asset_extraction(
        relative_path="2026/04/job/failed.jpg",
        extraction_status="failed",
        attempted_at_utc=t2,
        succeeded_at_utc=None,
        failed_at_utc=t2,
        failure_detail="broken",
        capture_timestamp_utc=None,
        camera_make=None,
        camera_model=None,
        image_width=None,
        image_height=None,
        orientation=None,
        lens_model=None,
        recorded_at_utc=t2,
    )
    store.upsert_media_asset(
        relative_path="2026/04/job/succeeded.jpg",
        sha256_hex="c" * 64,
        size_bytes=30,
        origin_kind="uploaded",
        observed_at_utc=t3,
    )
    store.upsert_media_asset_extraction(
        relative_path="2026/04/job/succeeded.jpg",
        extraction_status="succeeded",
        attempted_at_utc=t3,
        succeeded_at_utc=t3,
        failed_at_utc=None,
        failure_detail=None,
        capture_timestamp_utc=None,
        camera_make=None,
        camera_model=None,
        image_width=100,
        image_height=200,
        orientation=None,
        lens_model=None,
        recorded_at_utc=t3,
    )

    total_pending, pending_rows = store.list_media_assets(limit=10, offset=0, extraction_status="pending")
    assert total_pending == 1
    assert [row.relative_path for row in pending_rows] == ["2026/04/job/pending.jpg"]

    total_uploaded, uploaded_rows = store.list_media_assets(limit=10, offset=0, origin_kind="uploaded")
    assert total_uploaded == 2
    assert [row.relative_path for row in uploaded_rows] == [
        "2026/04/job/succeeded.jpg",
        "2026/04/job/failed.jpg",
    ]

    total_since, since_rows = store.list_media_assets(
        limit=10,
        offset=0,
        cataloged_since_utc="2026-04-22T10:30:00+00:00",
    )
    assert total_since == 1
    assert [row.relative_path for row in since_rows] == ["2026/04/job/succeeded.jpg"]


def test_postgres_list_media_assets_uses_bounded_filter_where_clause() -> None:
    observed: dict[str, object] = {}

    class _FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def execute(self, query: str, params: tuple[object, ...]) -> None:
            if "COUNT(*)" in query:
                observed["count_query"] = query
                observed["count_params"] = params
            else:
                observed["select_query"] = query
                observed["select_params"] = params

        def fetchone(self):
            return (1,)

        def fetchall(self) -> list[tuple[object, ...]]:
            return [
                (
                    "2026/04/job/a.jpg",
                    "a" * 64,
                    10,
                    "uploaded",
                    "uploaded",
                    None,
                    None,
                    "2026-04-22T10:00:00+00:00",
                    "2026-04-22T10:00:00+00:00",
                    "failed",
                    "2026-04-22T10:01:00+00:00",
                    None,
                    "2026-04-22T10:01:00+00:00",
                    "broken",
                    "pending",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
            ]

    class _FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def cursor(self) -> _FakeCursor:
            return _FakeCursor()

    class _TestStore(PostgresUploadStateStore):
        def _connect(self):  # type: ignore[override]
            return _FakeConnection()

    store = _TestStore(database_url="postgresql://unused")
    total, rows = store.list_media_assets(
        limit=20,
        offset=40,
        extraction_status="failed",
        origin_kind="uploaded",
        cataloged_since_utc="2026-04-01T00:00:00+00:00",
        cataloged_before_utc="2026-04-30T23:59:59+00:00",
    )

    assert total == 1
    assert len(rows) == 1
    assert "COALESCE(me.extraction_status, 'pending') = %s" in str(observed["count_query"])
    assert "ma.origin_kind = %s" in str(observed["count_query"])
    assert "ma.last_cataloged_at_utc >= %s" in str(observed["count_query"])
    assert "ma.last_cataloged_at_utc <= %s" in str(observed["count_query"])
    assert observed["count_params"] == (
        "failed",
        "uploaded",
        "2026-04-01T00:00:00+00:00",
        "2026-04-30T23:59:59+00:00",
    )
    assert observed["select_params"] == (
        "failed",
        "uploaded",
        "2026-04-01T00:00:00+00:00",
        "2026-04-30T23:59:59+00:00",
        20,
        40,
    )
