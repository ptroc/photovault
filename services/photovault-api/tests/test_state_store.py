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
