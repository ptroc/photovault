from photovault_server_ui.app import create_app


def test_dashboard_renders_overview_metrics() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        assert path == "/v1/admin/overview"
        assert query == {}
        return {
            "total_known_sha256": 3,
            "total_stored_files": 7,
            "indexed_files": 5,
            "uploaded_files": 2,
            "duplicate_file_paths": 1,
            "recent_indexed_files_24h": 4,
            "recent_uploaded_files_24h": 1,
            "last_indexed_at_utc": "2026-04-20T11:00:00+00:00",
            "last_uploaded_at_utc": "2026-04-20T10:00:00+00:00",
        }

    app = create_app(api_fetcher=_fetcher)
    client = app.test_client()
    response = client.get("/")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Server Overview" in html
    assert "Total known SHA256" in html
    assert ">3<" in html
    assert "Last indexed file" in html
    assert "2026-04-20T11:00:00+00:00" in html


def test_files_page_renders_rows_and_pager() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        assert path == "/v1/admin/files"
        assert query == {"limit": "50", "offset": "0"}
        return {
            "total": 51,
            "limit": 50,
            "offset": 0,
            "items": [
                {
                    "relative_path": "2026/04/Trip/photo.jpg",
                    "sha256_hex": "a" * 64,
                    "size_bytes": 2048,
                    "source_kind": "upload_verify",
                    "first_seen_at_utc": "2026-04-20T09:00:00+00:00",
                    "last_seen_at_utc": "2026-04-20T09:00:00+00:00",
                }
            ],
        }

    app = create_app(api_fetcher=_fetcher)
    client = app.test_client()
    response = client.get("/files")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Server Files" in html
    assert "2026/04/Trip/photo.jpg" in html
    assert "2.0 KiB" in html
    assert "Showing 1-1 of 51 files." in html
    assert 'href="/files?page=2"' in html


def test_files_page_empty_state_is_clear() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        return {"total": 0, "limit": 50, "offset": 0, "items": []}

    app = create_app(api_fetcher=_fetcher)
    client = app.test_client()
    response = client.get("/files")
    assert response.status_code == 200
    assert "No files are indexed or uploaded yet." in response.get_data(as_text=True)


def test_dashboard_error_state_is_clear() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        raise TimeoutError("api unavailable")

    app = create_app(api_fetcher=_fetcher)
    client = app.test_client()
    response = client.get("/")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Unable to reach photovault-api overview endpoint." in html
    assert ">0<" in html
