from photovault_api.state_store import PostgresUploadStateStore


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
