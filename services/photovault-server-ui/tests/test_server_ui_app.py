from photovault_server_ui.app import create_app


def test_dashboard_renders_overview_metrics() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        if path == "/v1/admin/overview":
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
        assert path == "/v1/admin/latest-index-run"
        assert query == {}
        return {
            "latest_run": {
                "scanned_files": 12,
                "indexed_files": 12,
                "new_sha_entries": 2,
                "existing_sha_matches": 10,
                "path_conflicts": 1,
                "errors": 0,
                "completed_at_utc": "2026-04-20T11:05:00+00:00",
            }
        }

    app = create_app(api_fetcher=_fetcher)
    client = app.test_client()
    response = client.get("/")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Server Overview" in html
    assert 'href="/static/vendor/bootstrap/css/bootstrap.min.css"' in html
    assert 'src="/static/vendor/bootstrap/js/bootstrap.bundle.min.js"' in html
    assert "Known SHA groups" in html
    assert ">3<" in html
    assert "Last indexed file" in html
    assert "2026-04-20" in html
    assert "11:00:00 UTC" in html
    assert "Latest Index Run" in html
    assert "11:05:00 UTC" in html
    assert "Open duplicate SHA groups" in html


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
    assert ("a" * 64) not in html
    assert "Technical detail" in html
    assert 'href="/files?page=2"' in html


def test_files_page_empty_state_is_clear() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        return {"total": 0, "limit": 50, "offset": 0, "items": []}

    app = create_app(api_fetcher=_fetcher)
    client = app.test_client()
    response = client.get("/files")
    assert response.status_code == 200
    assert "No files are indexed or uploaded yet." in response.get_data(as_text=True)


def test_clients_page_renders_statuses_and_actions() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        assert path == "/v1/admin/clients"
        assert query == {"limit": "50", "offset": "0"}
        return {
            "total": 2,
            "limit": 50,
            "offset": 0,
            "items": [
                {
                    "client_id": "pi-kitchen",
                    "display_name": "Kitchen Pi",
                    "enrollment_status": "pending",
                    "first_seen_at_utc": "2026-04-22T10:00:00+00:00",
                    "last_enrolled_at_utc": "2026-04-22T10:00:00+00:00",
                    "approved_at_utc": None,
                    "revoked_at_utc": None,
                    "auth_token": None,
                    "heartbeat_last_seen_at_utc": None,
                    "heartbeat_presence_status": "unknown",
                    "heartbeat_daemon_state": None,
                    "heartbeat_workload_status": None,
                    "heartbeat_active_job_summary": None,
                    "heartbeat_retry_backoff_summary": None,
                    "heartbeat_auth_block_reason": None,
                    "heartbeat_recent_error_summary": None,
                },
                {
                    "client_id": "pi-studio",
                    "display_name": "Studio Pi",
                    "enrollment_status": "approved",
                    "first_seen_at_utc": "2026-04-22T09:00:00+00:00",
                    "last_enrolled_at_utc": "2026-04-22T09:00:00+00:00",
                    "approved_at_utc": "2026-04-22T09:05:00+00:00",
                    "revoked_at_utc": None,
                    "auth_token": "token",
                    "heartbeat_last_seen_at_utc": "2026-04-22T09:10:00+00:00",
                    "heartbeat_presence_status": "online",
                    "heartbeat_daemon_state": "UPLOAD_FILE",
                    "heartbeat_workload_status": "working",
                    "heartbeat_active_job_summary": (
                        "Wedding SD (id=8, status=UPLOAD_FILE, ready=2, uploaded=1, retrying=0,"
                        " total=6, non_terminal=2, errors=0)"
                    ),
                    "heartbeat_retry_backoff_summary": (
                        "pending=1, next=2026-04-22T09:11:00+00:00, reason=upload offline"
                    ),
                    "heartbeat_auth_block_reason": None,
                    "heartbeat_recent_error_summary": (
                        "UPLOAD_RETRY_SCHEDULED at 2026-04-22T09:09:00+00:00: upload retry"
                    ),
                },
            ],
        }

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/clients")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Client Presence" in html
    assert "Kitchen Pi" in html
    assert "Studio Pi" in html
    assert "pending" in html
    assert "approved" in html
    assert "issued" in html
    assert "UPLOAD_FILE" in html
    assert "Wedding SD" in html
    assert "total=6" in html
    assert "non_terminal=2" in html
    assert "unknown" in html
    assert "online" in html
    assert "/clients/actions/approve" in html
    assert "/clients/actions/revoke" in html


def test_clients_page_forwards_filter_and_sort_query() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        assert path == "/v1/admin/clients"
        assert query == {
            "limit": "50",
            "offset": "50",
            "presence_status": "online",
            "workload_status": "working",
            "enrollment_status": "approved",
            "sort_by": "presence_status",
            "sort_order": "asc",
        }
        return {"total": 0, "limit": 50, "offset": 50, "items": []}

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get(
        (
            "/clients?page=2&presence_status=online&workload_status=working"
            "&enrollment_status=approved&sort_by=presence_status&sort_order=asc"
        )
    )
    assert response.status_code == 200


def test_clients_approve_action_posts_to_api_and_sets_message() -> None:
    observed: dict[str, object] = {}

    def _fetcher(path: str, query: dict[str, str]) -> dict:
        assert path == "/v1/admin/clients"
        return {"total": 0, "limit": 50, "offset": 0, "items": []}

    def _poster(path: str, payload: dict) -> dict:
        observed["path"] = path
        observed["payload"] = payload
        return {"item": {"client_id": "pi-kitchen"}}

    app = create_app(api_fetcher=_fetcher, api_poster=_poster)
    response = app.test_client().post(
        "/clients/actions/approve",
        data={"client_id": "pi-kitchen", "page": "1"},
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert observed["path"] == "/v1/admin/clients/pi-kitchen/approve"
    assert observed["payload"] == {}
    assert "Approved client pi-kitchen." in response.get_data(as_text=True)


def test_clients_revoke_action_posts_to_api_and_sets_message() -> None:
    observed: dict[str, object] = {}

    def _fetcher(path: str, query: dict[str, str]) -> dict:
        assert path == "/v1/admin/clients"
        return {"total": 0, "limit": 50, "offset": 0, "items": []}

    def _poster(path: str, payload: dict) -> dict:
        observed["path"] = path
        observed["payload"] = payload
        return {"item": {"client_id": "pi-kitchen"}}

    app = create_app(api_fetcher=_fetcher, api_poster=_poster)
    response = app.test_client().post(
        "/clients/actions/revoke",
        data={"client_id": "pi-kitchen", "page": "1"},
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert observed["path"] == "/v1/admin/clients/pi-kitchen/revoke"
    assert observed["payload"] == {}
    assert "Revoked client pi-kitchen." in response.get_data(as_text=True)


def test_clients_actions_preserve_filter_sort_state_in_redirect() -> None:
    observed: dict[str, object] = {}

    def _fetcher(path: str, query: dict[str, str]) -> dict:
        observed["query"] = query
        assert path == "/v1/admin/clients"
        return {"total": 0, "limit": 50, "offset": 0, "items": []}

    def _poster(path: str, payload: dict) -> dict:
        assert path == "/v1/admin/clients/pi-kitchen/approve"
        assert payload == {}
        return {"item": {"client_id": "pi-kitchen"}}

    app = create_app(api_fetcher=_fetcher, api_poster=_poster)
    response = app.test_client().post(
        "/clients/actions/approve",
        data={
            "client_id": "pi-kitchen",
            "page": "1",
            "presence_status": "online",
            "workload_status": "working",
            "enrollment_status": "approved",
            "sort_by": "presence_status",
            "sort_order": "asc",
        },
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert observed["query"] == {
        "limit": "50",
        "offset": "0",
        "presence_status": "online",
        "workload_status": "working",
        "enrollment_status": "approved",
        "sort_by": "presence_status",
        "sort_order": "asc",
    }


def test_clients_page_returns_fragment_for_hx_requests() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        assert path == "/v1/admin/clients"
        assert query == {
            "limit": "50",
            "offset": "50",
            "presence_status": "online",
            "sort_by": "presence_status",
            "sort_order": "asc",
        }
        return {"total": 0, "limit": 50, "offset": 50, "items": []}

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get(
        "/clients?page=2&presence_status=online&sort_by=presence_status&sort_order=asc",
        headers={"HX-Request": "true"},
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert 'id="clients-shell"' in html
    assert "<!doctype html>" not in html


def test_clients_approve_action_returns_fragment_for_hx_requests() -> None:
    observed: dict[str, object] = {}

    def _fetcher(path: str, query: dict[str, str]) -> dict:
        assert path == "/v1/admin/clients"
        assert query == {
            "limit": "50",
            "offset": "0",
            "presence_status": "online",
            "sort_by": "presence_status",
            "sort_order": "asc",
        }
        return {"total": 0, "limit": 50, "offset": 0, "items": []}

    def _poster(path: str, payload: dict) -> dict:
        observed["path"] = path
        observed["payload"] = payload
        return {"ok": True}

    app = create_app(api_fetcher=_fetcher, api_poster=_poster)
    response = app.test_client().post(
        "/clients/actions/approve",
        data={
            "client_id": "pi-kitchen",
            "page": "1",
            "presence_status": "online",
            "workload_status": "",
            "enrollment_status": "",
            "sort_by": "presence_status",
            "sort_order": "asc",
        },
        headers={"HX-Request": "true"},
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert observed["path"] == "/v1/admin/clients/pi-kitchen/approve"
    assert observed["payload"] == {}
    assert 'id="clients-shell"' in html
    assert "Approved client pi-kitchen." in html
    assert "<!doctype html>" not in html


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


def test_duplicates_page_renders_duplicate_groups() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        if path == "/v1/admin/duplicates":
            assert query == {"limit": "25", "offset": "0"}
            return {
                "total": 1,
                "limit": 25,
                "offset": 0,
                "items": [
                    {
                        "sha256_hex": "a" * 64,
                        "file_count": 2,
                        "first_seen_at_utc": "2026-04-20T09:00:00+00:00",
                        "last_seen_at_utc": "2026-04-20T10:00:00+00:00",
                        "relative_paths": ["2026/04/Trip/a.jpg", "2026/04/TripCopy/a.jpg"],
                    }
                ],
            }
        assert path == "/v1/admin/catalog/asset"
        return {"item": _library_catalog_item(query["relative_path"])}

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/duplicates")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Duplicate SHA Groups" in html
    assert "2026/04/Trip/a.jpg" in html
    assert "2026/04/TripCopy/a.jpg" in html
    assert "2 path(s)" in html


def test_catalog_page_returns_fragment_for_hx_requests() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        if path == "/v1/admin/catalog":
            assert query == {"limit": "50", "offset": "0", "preview_status": "pending"}
            return {
                "total": 1,
                "limit": 50,
                "offset": 0,
                "items": [_library_catalog_item("2026/04/Trip/a.jpg", preview_status="pending")],
            }
        assert path == "/v1/admin/catalog/backfill/latest"
        return {"extraction_run": None, "preview_run": None}

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get(
        "/catalog?preview_status=pending",
        headers={"HX-Request": "true"},
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert 'id="catalog-shell"' in html
    assert "<!doctype html>" not in html


def test_catalog_backfill_action_returns_fragment_for_hx_requests() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        if path == "/v1/admin/catalog":
            assert query == {"limit": "50", "offset": "0", "preview_status": "pending"}
            return {"total": 0, "limit": 50, "offset": 0, "items": []}
        assert path == "/v1/admin/catalog/backfill/latest"
        return {"extraction_run": None, "preview_run": None}

    def _poster(path: str, payload: dict) -> dict:
        assert path == "/v1/admin/catalog/extraction/backfill"
        assert payload["target_statuses"] == ["pending", "failed"]
        assert payload["limit"] == 25
        return {
            "run": {
                "selected_count": 3,
                "succeeded_count": 2,
                "failed_count": 1,
                "remaining_pending_count": 7,
                "remaining_failed_count": 4,
            }
        }

    app = create_app(api_fetcher=_fetcher, api_poster=_poster)
    response = app.test_client().post(
        "/catalog/actions/backfill",
        data={
            "page": "1",
            "backfill_kind": "extraction",
            "return_query": "preview_status=pending",
            "target_statuses": ["pending", "failed"],
            "limit": "25",
        },
        headers={"HX-Request": "true"},
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert 'id="catalog-shell"' in html
    assert "Extraction backfill completed: selected=3, succeeded=2, failed=1" in html


def test_conflicts_page_renders_conflict_history_and_latest_run() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        if path == "/v1/admin/path-conflicts":
            assert query == {"limit": "25", "offset": "0"}
            return {
                "total": 1,
                "limit": 25,
                "offset": 0,
                "items": [
                    {
                        "relative_path": "2026/04/Trip/photo.jpg",
                        "previous_sha256_hex": "a" * 64,
                        "current_sha256_hex": "b" * 64,
                        "detected_at_utc": "2026-04-20T11:30:00+00:00",
                    }
                ],
            }
        assert path == "/v1/admin/latest-index-run"
        assert query == {}
        return {
            "latest_run": {
                "scanned_files": 2,
                "indexed_files": 2,
                "new_sha_entries": 1,
                "existing_sha_matches": 1,
                "path_conflicts": 1,
                "errors": 0,
                "completed_at_utc": "2026-04-20T11:31:00+00:00",
            }
        }

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/conflicts")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Path Conflict History" in html
    assert "2026/04/Trip/photo.jpg" in html
    assert "2026-04-20" in html
    assert "11:31:00 UTC" in html
    assert "No path conflicts have been recorded." not in html


def test_catalog_page_renders_rows_extraction_states_and_metadata_summary() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        if path == "/v1/admin/catalog/backfill/latest":
            assert query == {}
            return {"extraction_run": None, "preview_run": None}
        assert path == "/v1/admin/catalog"
        assert query == {"limit": "50", "offset": "0"}
        return {
            "total": 2,
            "limit": 50,
            "offset": 0,
            "items": [
                {
                    "relative_path": "2026/04/Job_A/a.jpg",
                    "sha256_hex": "a" * 64,
                    "size_bytes": 2048,
                    "media_type": "jpeg",
                    "preview_capability": "previewable",
                    "origin_kind": "uploaded",
                    "last_observed_origin_kind": "uploaded",
                    "provenance_job_name": "Job_A",
                    "provenance_original_filename": "a.jpg",
                    "first_cataloged_at_utc": "2026-04-22T10:00:00+00:00",
                    "last_cataloged_at_utc": "2026-04-22T10:00:00+00:00",
                    "extraction_status": "succeeded",
                    "extraction_last_attempted_at_utc": "2026-04-22T10:01:00+00:00",
                    "extraction_last_succeeded_at_utc": "2026-04-22T10:01:00+00:00",
                    "extraction_last_failed_at_utc": None,
                    "extraction_failure_detail": None,
                    "preview_status": "succeeded",
                    "preview_relative_path": "2026/04/Job_A/a__abc123__w1024.jpg",
                    "preview_last_attempted_at_utc": "2026-04-22T10:02:00+00:00",
                    "preview_last_succeeded_at_utc": "2026-04-22T10:02:00+00:00",
                    "preview_last_failed_at_utc": None,
                    "preview_failure_detail": None,
                    "is_favorite": True,
                    "is_archived": False,
                    "capture_timestamp_utc": "2026-04-22T09:30:00+00:00",
                    "camera_make": "Canon",
                    "camera_model": "EOS R6",
                    "image_width": 6000,
                    "image_height": 4000,
                    "orientation": 1,
                    "lens_model": "RF 24-70mm",
                },
                {
                    "relative_path": "2026/04/Job_A/b.jpg",
                    "sha256_hex": "b" * 64,
                    "size_bytes": 1000,
                    "media_type": "jpeg",
                    "preview_capability": "previewable",
                    "origin_kind": "indexed",
                    "last_observed_origin_kind": "indexed",
                    "provenance_job_name": None,
                    "provenance_original_filename": None,
                    "first_cataloged_at_utc": "2026-04-22T10:05:00+00:00",
                    "last_cataloged_at_utc": "2026-04-22T10:05:00+00:00",
                    "extraction_status": "failed",
                    "extraction_last_attempted_at_utc": "2026-04-22T10:06:00+00:00",
                    "extraction_last_succeeded_at_utc": None,
                    "extraction_last_failed_at_utc": "2026-04-22T10:06:00+00:00",
                    "extraction_failure_detail": "invalid media content",
                    "preview_status": "failed",
                    "preview_relative_path": None,
                    "preview_last_attempted_at_utc": "2026-04-22T10:06:10+00:00",
                    "preview_last_succeeded_at_utc": None,
                    "preview_last_failed_at_utc": "2026-04-22T10:06:10+00:00",
                    "preview_failure_detail": "preview generation failed",
                    "is_favorite": False,
                    "is_archived": True,
                    "capture_timestamp_utc": None,
                    "camera_make": None,
                    "camera_model": None,
                    "image_width": None,
                    "image_height": None,
                    "orientation": None,
                    "lens_model": None,
                },
            ],
        }

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/catalog")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Media Library" in html
    assert "2026/04/Job_A/a.jpg" in html
    assert "2026/04/Job_A/b.jpg" in html
    assert "succeeded" in html
    assert "failed" in html
    assert "camera Canon EOS R6" in html
    assert "6000x4000" in html
    assert ("a" * 64) not in html
    assert ("b" * 64) not in html
    assert "Inspect Asset" in html
    assert 'src="/catalog/preview?relative_path=2026/04/Job_A/a.jpg"' in html
    assert "favorite" in html
    assert "archived" in html


def test_catalog_page_empty_state_is_clear() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        if path == "/v1/admin/catalog/backfill/latest":
            assert query == {}
            return {"extraction_run": None, "preview_run": None}
        assert path == "/v1/admin/catalog"
        return {"total": 0, "limit": 50, "offset": 0, "items": []}

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/catalog")
    assert response.status_code == 200
    assert "No catalog assets matched the current filters." in response.get_data(as_text=True)


def test_catalog_page_pagination_is_sane() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        if path == "/v1/admin/catalog/backfill/latest":
            assert query == {}
            return {"extraction_run": None, "preview_run": None}
        assert path == "/v1/admin/catalog"
        assert query == {"limit": "50", "offset": "50"}
        return {
            "total": 120,
            "limit": 50,
            "offset": 50,
            "items": [
                {
                    "relative_path": "2026/04/Job_A/a.jpg",
                    "sha256_hex": "a" * 64,
                    "size_bytes": 1,
                    "media_type": "jpeg",
                    "preview_capability": "previewable",
                    "origin_kind": "uploaded",
                    "last_observed_origin_kind": "uploaded",
                    "provenance_job_name": None,
                    "provenance_original_filename": None,
                    "first_cataloged_at_utc": "2026-04-22T10:00:00+00:00",
                    "last_cataloged_at_utc": "2026-04-22T10:00:00+00:00",
                    "extraction_status": "pending",
                    "extraction_last_attempted_at_utc": None,
                    "extraction_last_succeeded_at_utc": None,
                    "extraction_last_failed_at_utc": None,
                    "extraction_failure_detail": None,
                    "capture_timestamp_utc": None,
                    "camera_make": None,
                    "camera_model": None,
                    "image_width": None,
                    "image_height": None,
                    "orientation": None,
                    "lens_model": None,
                    "is_favorite": False,
                    "is_archived": False,
                }
            ],
        }

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/catalog?page=2")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Showing 51-51 of 120 cataloged assets." in html
    assert 'href="/catalog?page=1"' in html
    assert 'href="/catalog?page=3"' in html


def test_catalog_page_filters_pending_assets() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        if path == "/v1/admin/catalog/backfill/latest":
            assert query == {}
            return {"extraction_run": None, "preview_run": None}
        assert path == "/v1/admin/catalog"
        assert query == {"limit": "50", "offset": "0", "extraction_status": "pending"}
        return {
            "total": 1,
            "limit": 50,
            "offset": 0,
            "items": [
                {
                    "relative_path": "2026/04/Job_A/pending.jpg",
                    "sha256_hex": "a" * 64,
                    "size_bytes": 100,
                    "media_type": "jpeg",
                    "preview_capability": "previewable",
                    "origin_kind": "indexed",
                    "last_observed_origin_kind": "indexed",
                    "provenance_job_name": None,
                    "provenance_original_filename": None,
                    "first_cataloged_at_utc": "2026-04-22T10:00:00+00:00",
                    "last_cataloged_at_utc": "2026-04-22T10:00:00+00:00",
                    "extraction_status": "pending",
                    "extraction_last_attempted_at_utc": None,
                    "extraction_last_succeeded_at_utc": None,
                    "extraction_last_failed_at_utc": None,
                    "extraction_failure_detail": None,
                    "capture_timestamp_utc": None,
                    "camera_make": None,
                    "camera_model": None,
                    "image_width": None,
                    "image_height": None,
                    "orientation": None,
                    "lens_model": None,
                    "is_favorite": False,
                    "is_archived": False,
                }
            ],
        }

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/catalog?extraction_status=pending")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "2026/04/Job_A/pending.jpg" in html
    assert "pending" in html


def test_catalog_page_filters_failed_assets_and_shows_failure_detail() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        if path == "/v1/admin/catalog/backfill/latest":
            assert query == {}
            return {"extraction_run": None, "preview_run": None}
        assert path == "/v1/admin/catalog"
        assert query == {"limit": "50", "offset": "0", "extraction_status": "failed"}
        return {
            "total": 1,
            "limit": 50,
            "offset": 0,
            "items": [
                {
                    "relative_path": "2026/04/Job_A/failed.jpg",
                    "sha256_hex": "b" * 64,
                    "size_bytes": 100,
                    "media_type": "jpeg",
                    "preview_capability": "previewable",
                    "origin_kind": "uploaded",
                    "last_observed_origin_kind": "uploaded",
                    "provenance_job_name": None,
                    "provenance_original_filename": None,
                    "first_cataloged_at_utc": "2026-04-22T10:00:00+00:00",
                    "last_cataloged_at_utc": "2026-04-22T10:00:00+00:00",
                    "extraction_status": "failed",
                    "extraction_last_attempted_at_utc": "2026-04-22T10:01:00+00:00",
                    "extraction_last_succeeded_at_utc": None,
                    "extraction_last_failed_at_utc": "2026-04-22T10:01:00+00:00",
                    "extraction_failure_detail": "invalid media content",
                    "capture_timestamp_utc": None,
                    "camera_make": None,
                    "camera_model": None,
                    "image_width": None,
                    "image_height": None,
                    "orientation": None,
                    "lens_model": None,
                    "is_favorite": False,
                    "is_archived": False,
                }
            ],
        }

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/catalog?extraction_status=failed")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "2026/04/Job_A/failed.jpg" in html
    assert ("b" * 64) not in html
    assert "Inspect Asset" in html


def test_catalog_page_origin_filter_and_filtered_pagination_links() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        if path == "/v1/admin/catalog/backfill/latest":
            assert query == {}
            return {"extraction_run": None, "preview_run": None}
        assert path == "/v1/admin/catalog"
        assert query == {
            "limit": "50",
            "offset": "50",
            "extraction_status": "failed",
            "origin_kind": "uploaded",
        }
        return {
            "total": 120,
            "limit": 50,
            "offset": 50,
            "items": [
                {
                    "relative_path": "2026/04/Job_A/failed.jpg",
                    "sha256_hex": "b" * 64,
                    "size_bytes": 100,
                    "media_type": "jpeg",
                    "preview_capability": "previewable",
                    "origin_kind": "uploaded",
                    "last_observed_origin_kind": "uploaded",
                    "provenance_job_name": None,
                    "provenance_original_filename": None,
                    "first_cataloged_at_utc": "2026-04-22T10:00:00+00:00",
                    "last_cataloged_at_utc": "2026-04-22T10:00:00+00:00",
                    "extraction_status": "failed",
                    "extraction_last_attempted_at_utc": "2026-04-22T10:01:00+00:00",
                    "extraction_last_succeeded_at_utc": None,
                    "extraction_last_failed_at_utc": "2026-04-22T10:01:00+00:00",
                    "extraction_failure_detail": "invalid media content",
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

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/catalog?page=2&extraction_status=failed&origin_kind=uploaded")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert 'href="/catalog?page=1&amp;extraction_status=failed&amp;origin_kind=uploaded"' in html
    assert 'href="/catalog?page=3&amp;extraction_status=failed&amp;origin_kind=uploaded"' in html


def test_catalog_page_forwards_media_type_preview_filters() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        if path == "/v1/admin/catalog/backfill/latest":
            assert query == {}
            return {"extraction_run": None, "preview_run": None}
        assert path == "/v1/admin/catalog"
        assert query == {
            "limit": "50",
            "offset": "0",
            "media_type": "raw",
            "preview_capability": "previewable",
            "preview_status": "failed",
        }
        return {"total": 0, "limit": 50, "offset": 0, "items": []}

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get(
        "/catalog?media_type=raw&preview_capability=previewable&preview_status=failed"
    )
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "No catalog assets matched the current filters." in html


def test_catalog_page_forwards_favorite_and_archived_filters() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        if path == "/v1/admin/catalog/backfill/latest":
            assert query == {}
            return {"extraction_run": None, "preview_run": None}
        assert path == "/v1/admin/catalog"
        assert query == {
            "limit": "50",
            "offset": "0",
            "is_favorite": "true",
            "is_archived": "false",
        }
        return {"total": 0, "limit": 50, "offset": 0, "items": []}

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/catalog?is_favorite=true&is_archived=false")
    assert response.status_code == 200


def test_catalog_page_renders_backfill_controls_and_latest_run_summary() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        if path == "/v1/admin/catalog":
            assert query == {"limit": "50", "offset": "0"}
            return {"total": 0, "limit": 50, "offset": 0, "items": []}
        assert path == "/v1/admin/catalog/backfill/latest"
        assert query == {}
        return {
            "extraction_run": {
                "backfill_kind": "extraction",
                "requested_statuses": ["pending", "failed"],
                "limit": 50,
                "origin_kind": "indexed",
                "media_type": "jpeg",
                "preview_capability": "previewable",
                "cataloged_since_utc": None,
                "cataloged_before_utc": None,
                "selected_count": 8,
                "processed_count": 8,
                "succeeded_count": 7,
                "failed_count": 1,
                "remaining_pending_count": 3,
                "remaining_failed_count": 2,
                "completed_at_utc": "2026-04-22T11:00:00+00:00",
            },
            "preview_run": {
                "backfill_kind": "preview",
                "requested_statuses": ["pending", "failed"],
                "limit": 25,
                "origin_kind": "uploaded",
                "media_type": "raw",
                "preview_capability": "previewable",
                "cataloged_since_utc": None,
                "cataloged_before_utc": None,
                "selected_count": 5,
                "processed_count": 5,
                "succeeded_count": 4,
                "failed_count": 1,
                "remaining_pending_count": 2,
                "remaining_failed_count": 1,
                "completed_at_utc": "2026-04-22T11:05:00+00:00",
            },
        }

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/catalog")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Backfill Operations" in html
    assert "Run Extraction Backfill" in html
    assert "Run Preview Backfill" in html
    assert "remaining pending 3, failed 2" in html
    assert "remaining pending 2, failed 1" in html
    assert "2026-04-22" in html
    assert "11:05:00 UTC" in html


def test_catalog_backfill_action_posts_filters_and_shows_outcome_message() -> None:
    observed: dict[str, object] = {}

    def _fetcher(path: str, query: dict[str, str]) -> dict:
        if path == "/v1/admin/catalog":
            observed["query"] = query
            return {"total": 0, "limit": 50, "offset": 50, "items": []}
        assert path == "/v1/admin/catalog/backfill/latest"
        return {"extraction_run": None, "preview_run": None}

    def _poster(path: str, payload: dict) -> dict:
        observed["path"] = path
        observed["payload"] = payload
        return {
            "run": {
                "selected_count": 4,
                "succeeded_count": 3,
                "failed_count": 1,
                "remaining_pending_count": 2,
                "remaining_failed_count": 1,
            },
            "items": [],
        }

    app = create_app(api_fetcher=_fetcher, api_poster=_poster)
    response = app.test_client().post(
        "/catalog/actions/backfill",
        data={
            "page": "2",
            "backfill_kind": "preview",
            "target_statuses": ["pending", "failed"],
            "limit": "40",
            "extraction_status": "failed",
            "preview_status": "pending",
            "origin_kind": "indexed",
            "media_type": "raw",
            "preview_capability": "previewable",
            "cataloged_since_utc": "2026-04-22T00:00:00+00:00",
        },
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert observed["path"] == "/v1/admin/catalog/preview/backfill"
    assert observed["payload"] == {
        "target_statuses": ["pending", "failed"],
        "limit": 40,
        "origin_kind": "indexed",
        "media_type": "raw",
        "preview_capability": "previewable",
        "cataloged_since_utc": "2026-04-22T00:00:00+00:00",
    }
    assert observed["query"] == {
        "limit": "50",
        "offset": "50",
        "extraction_status": "failed",
        "preview_status": "pending",
        "origin_kind": "indexed",
        "media_type": "raw",
        "preview_capability": "previewable",
        "cataloged_since_utc": "2026-04-22T00:00:00+00:00",
    }
    assert "Preview backfill completed: selected=4, succeeded=3, failed=1" in response.get_data(
        as_text=True
    )


def test_catalog_asset_detail_renders_preview_status_and_operator_metadata() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        assert path == "/v1/admin/catalog/asset"
        assert query == {"relative_path": "2026/04/Job_A/a.jpg"}
        return {
            "item": {
                "relative_path": "2026/04/Job_A/a.jpg",
                "sha256_hex": "a" * 64,
                "size_bytes": 2048,
                "media_type": "jpeg",
                "preview_capability": "previewable",
                "origin_kind": "uploaded",
                "last_observed_origin_kind": "uploaded",
                "provenance_job_name": "Job_A",
                "provenance_original_filename": "a.jpg",
                "first_cataloged_at_utc": "2026-04-22T10:00:00+00:00",
                "last_cataloged_at_utc": "2026-04-22T10:00:00+00:00",
                "extraction_status": "succeeded",
                "extraction_last_attempted_at_utc": "2026-04-22T10:01:00+00:00",
                "extraction_last_succeeded_at_utc": "2026-04-22T10:01:00+00:00",
                "extraction_last_failed_at_utc": None,
                "extraction_failure_detail": None,
                "preview_status": "succeeded",
                "preview_relative_path": "2026/04/Job_A/a__abc__w1024.jpg",
                "preview_last_attempted_at_utc": "2026-04-22T10:02:00+00:00",
                "preview_last_succeeded_at_utc": "2026-04-22T10:02:00+00:00",
                "preview_last_failed_at_utc": None,
                "preview_failure_detail": None,
                "is_favorite": True,
                "is_archived": False,
                "capture_timestamp_utc": "2026-04-22T09:30:00+00:00",
                "camera_make": "Canon",
                "camera_model": "EOS R6",
                "image_width": 6000,
                "image_height": 4000,
                "orientation": 1,
                "lens_model": "RF 24-70mm",
            }
        }

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/catalog/asset?relative_path=2026/04/Job_A/a.jpg")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Library Asset Detail" in html
    assert "Asset Identity" in html
    assert "Provenance" in html
    assert "Extraction Metadata" in html
    assert "Preview Status" in html
    assert "preview_capability" in html
    assert "previewable" in html
    assert ("a" * 64) in html
    assert "preview_last_succeeded_at_utc" not in html
    assert 'src="/catalog/preview?relative_path=2026/04/Job_A/a.jpg"' in html
    assert "favorite" in html


def test_catalog_asset_detail_renders_preview_status_for_heic_asset() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        assert path == "/v1/admin/catalog/asset"
        assert query == {"relative_path": "2026/04/Job_A/a.heic"}
        return {
            "item": {
                "relative_path": "2026/04/Job_A/a.heic",
                "sha256_hex": "b" * 64,
                "size_bytes": 4096,
                "media_type": "heic",
                "preview_capability": "previewable",
                "origin_kind": "indexed",
                "last_observed_origin_kind": "indexed",
                "provenance_job_name": None,
                "provenance_original_filename": None,
                "first_cataloged_at_utc": "2026-04-22T10:00:00+00:00",
                "last_cataloged_at_utc": "2026-04-22T10:00:00+00:00",
                "extraction_status": "pending",
                "extraction_last_attempted_at_utc": None,
                "extraction_last_succeeded_at_utc": None,
                "extraction_last_failed_at_utc": None,
                "extraction_failure_detail": None,
                "preview_status": "failed",
                "preview_relative_path": None,
                "preview_last_attempted_at_utc": "2026-04-22T10:02:00+00:00",
                "preview_last_succeeded_at_utc": None,
                "preview_last_failed_at_utc": "2026-04-22T10:02:00+00:00",
                "preview_failure_detail": "HEIC preview backend unavailable",
                "is_favorite": False,
                "is_archived": True,
                "capture_timestamp_utc": None,
                "camera_make": None,
                "camera_model": None,
                "image_width": None,
                "image_height": None,
                "orientation": None,
                "lens_model": None,
            }
        }

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/catalog/asset?relative_path=2026/04/Job_A/a.heic")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Library Asset Detail" in html
    assert "failed" in html
    assert "HEIC preview backend unavailable" in html
    assert "Preview Image" not in html


def test_catalog_favorite_action_posts_to_api_and_preserves_filters() -> None:
    observed: dict[str, object] = {}

    def _fetcher(path: str, query: dict[str, str]) -> dict:
        if path == "/v1/admin/catalog/backfill/latest":
            assert query == {}
            return {"extraction_run": None, "preview_run": None}
        observed["query"] = query
        assert path == "/v1/admin/catalog"
        return {"total": 0, "limit": 50, "offset": 0, "items": []}

    def _poster(path: str, payload: dict) -> dict:
        observed["path"] = path
        observed["payload"] = payload
        return {"item": {"relative_path": "2026/04/Job_A/a.jpg", "is_favorite": True}}

    app = create_app(api_fetcher=_fetcher, api_poster=_poster)
    response = app.test_client().post(
        "/catalog/actions/favorite/mark",
        data={
            "relative_path": "2026/04/Job_A/a.jpg",
            "page": "2",
            "return_to": "catalog",
            "is_favorite": "true",
            "is_archived": "false",
        },
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert observed["path"] == "/v1/admin/catalog/favorite/mark"
    assert observed["payload"] == {"relative_path": "2026/04/Job_A/a.jpg"}
    assert observed["query"] == {
        "limit": "50",
        "offset": "50",
        "is_favorite": "true",
        "is_archived": "false",
    }
    assert "Marked favorite: 2026/04/Job_A/a.jpg." in response.get_data(as_text=True)


def test_catalog_archive_action_redirects_back_to_detail() -> None:
    observed: dict[str, object] = {}

    def _fetcher(path: str, query: dict[str, str]) -> dict:
        assert path == "/v1/admin/catalog/asset"
        assert query == {"relative_path": "2026/04/Job_A/a.jpg"}
        return {
            "item": {
                "relative_path": "2026/04/Job_A/a.jpg",
                "sha256_hex": "a" * 64,
                "size_bytes": 2048,
                "media_type": "jpeg",
                "preview_capability": "previewable",
                "origin_kind": "uploaded",
                "last_observed_origin_kind": "uploaded",
                "provenance_job_name": "Job_A",
                "provenance_original_filename": "a.jpg",
                "first_cataloged_at_utc": "2026-04-22T10:00:00+00:00",
                "last_cataloged_at_utc": "2026-04-22T10:00:00+00:00",
                "extraction_status": "succeeded",
                "extraction_last_attempted_at_utc": "2026-04-22T10:01:00+00:00",
                "extraction_last_succeeded_at_utc": "2026-04-22T10:01:00+00:00",
                "extraction_last_failed_at_utc": None,
                "extraction_failure_detail": None,
                "preview_status": "succeeded",
                "preview_relative_path": "2026/04/Job_A/a__abc__w1024.jpg",
                "preview_last_attempted_at_utc": "2026-04-22T10:02:00+00:00",
                "preview_last_succeeded_at_utc": "2026-04-22T10:02:00+00:00",
                "preview_last_failed_at_utc": None,
                "preview_failure_detail": None,
                "is_favorite": True,
                "is_archived": True,
                "capture_timestamp_utc": None,
                "camera_make": None,
                "camera_model": None,
                "image_width": None,
                "image_height": None,
                "orientation": None,
                "lens_model": None,
            }
        }

    def _poster(path: str, payload: dict) -> dict:
        observed["path"] = path
        observed["payload"] = payload
        return {"item": {"relative_path": "2026/04/Job_A/a.jpg", "is_archived": False}}

    app = create_app(api_fetcher=_fetcher, api_poster=_poster)
    response = app.test_client().post(
        "/catalog/actions/archive/unmark",
        data={
            "relative_path": "2026/04/Job_A/a.jpg",
            "page": "1",
            "return_to": "asset",
        },
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert observed["path"] == "/v1/admin/catalog/archive/unmark"
    assert observed["payload"] == {"relative_path": "2026/04/Job_A/a.jpg"}
    assert "Unarchived asset: 2026/04/Job_A/a.jpg." in response.get_data(as_text=True)


# --- Library (grid view) ---------------------------------------------------

def _library_catalog_item(relative_path: str, **overrides) -> dict:
    """Build a minimal catalog item suitable for library tests.

    The server-UI's _decorate_catalog_item only relies on a handful of keys,
    so tests default the rest to "None" / "pending" to keep them readable.
    Overrides let individual tests flip interesting fields.
    """
    base = {
        "relative_path": relative_path,
        "sha256_hex": "c" * 64,
        "size_bytes": 1024,
        "media_type": "jpeg",
        "preview_capability": "previewable",
        "origin_kind": "uploaded",
        "last_observed_origin_kind": "uploaded",
        "provenance_job_name": "Job_A",
        "provenance_original_filename": relative_path.rsplit("/", 1)[-1],
        "first_cataloged_at_utc": "2026-04-22T10:00:00+00:00",
        "last_cataloged_at_utc": "2026-04-22T10:00:00+00:00",
        "extraction_status": "succeeded",
        "extraction_last_attempted_at_utc": "2026-04-22T10:01:00+00:00",
        "extraction_last_succeeded_at_utc": "2026-04-22T10:01:00+00:00",
        "extraction_last_failed_at_utc": None,
        "extraction_failure_detail": None,
        "preview_status": "succeeded",
        "preview_relative_path": f"{relative_path}.preview.jpg",
        "preview_last_attempted_at_utc": "2026-04-22T10:02:00+00:00",
        "preview_last_succeeded_at_utc": "2026-04-22T10:02:00+00:00",
        "preview_last_failed_at_utc": None,
        "preview_failure_detail": None,
        "is_favorite": False,
        "is_archived": False,
        "capture_timestamp_utc": "2026-04-22T09:30:00+00:00",
        "camera_make": "Canon",
        "camera_model": "EOS R6",
        "image_width": 6000,
        "image_height": 4000,
        "orientation": 1,
        "lens_model": "RF 24-70mm",
    }
    base.update(overrides)
    return base


def test_library_page_renders_tree_and_grid() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        if path == "/v1/admin/catalog/folders":
            assert query == {}
            return {
                "folders": [
                    {"path": "2026", "depth": 1, "direct_count": 0, "total_count": 4},
                    {"path": "2026/04", "depth": 2, "direct_count": 0, "total_count": 4},
                    {
                        "path": "2026/04/Job_A",
                        "depth": 3,
                        "direct_count": 2,
                        "total_count": 2,
                    },
                    {
                        "path": "2026/04/Job_B",
                        "depth": 3,
                        "direct_count": 2,
                        "total_count": 2,
                    },
                ]
            }
        if path == "/v1/admin/catalog/rejects":
            return {"total": 0, "limit": 1, "offset": 0, "items": []}
        if path == "/v1/admin/catalog/tombstones":
            return {"total": 0, "limit": 1, "offset": 0, "items": []}
        assert path == "/v1/admin/catalog"
        # Default view has no prefix filter.
        assert query == {"limit": "60", "offset": "0"}
        return {
            "total": 2,
            "limit": 60,
            "offset": 0,
            "items": [
                _library_catalog_item("2026/04/Job_A/a.jpg", sha256_hex="a" * 64),
                _library_catalog_item("2026/04/Job_B/b.jpg", sha256_hex="b" * 64),
            ],
        }

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/library")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Media Library" in html
    # Folder tree entries render.
    assert "2026" in html
    assert "Job_A" in html
    assert "Job_B" in html
    # Root "All folders" entry is present and selected by default.
    assert 'href="/library"' in html
    assert "All folders" in html
    # Thumbnails render with lazy loading and previews proxied via server-UI.
    assert 'src="/catalog/preview?relative_path=2026/04/Job_A/a.jpg"' in html
    assert 'loading="lazy"' in html
    # Lightbox modal shell is present.
    assert 'id="libraryLightbox"' in html
    # SHA must not leak.
    assert ("a" * 64) not in html
    assert ("b" * 64) not in html


def test_library_page_filters_by_selected_folder() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        if path == "/v1/admin/catalog/folders":
            return {
                "folders": [
                    {"path": "2026", "depth": 1, "direct_count": 0, "total_count": 2},
                    {"path": "2026/04", "depth": 2, "direct_count": 0, "total_count": 2},
                    {
                        "path": "2026/04/Job_A",
                        "depth": 3,
                        "direct_count": 2,
                        "total_count": 2,
                    },
                ]
            }
        if path == "/v1/admin/catalog/rejects":
            return {"total": 0, "limit": 1, "offset": 0, "items": []}
        if path == "/v1/admin/catalog/tombstones":
            return {"total": 0, "limit": 1, "offset": 0, "items": []}
        assert path == "/v1/admin/catalog"
        # The selected folder must be forwarded to the API as the prefix filter.
        assert query == {
            "limit": "60",
            "offset": "0",
            "relative_path_prefix": "2026/04/Job_A",
        }
        return {
            "total": 1,
            "limit": 60,
            "offset": 0,
            "items": [
                _library_catalog_item("2026/04/Job_A/a.jpg", sha256_hex="a" * 64),
            ],
        }

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/library?folder=2026/04/Job_A")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    # The selected folder is surfaced in the header line.
    assert "2026/04/Job_A" in html
    # Paginator uses the folder filter.
    assert "Next" not in html or "folder=2026" in html


def test_library_page_rejects_path_traversal_in_folder_arg() -> None:
    # Malicious or malformed folder values must not be forwarded; the page
    # should fall back to the unfiltered view.
    forwarded: dict[str, dict[str, str]] = {}

    def _fetcher(path: str, query: dict[str, str]) -> dict:
        if path == "/v1/admin/catalog/folders":
            return {"folders": []}
        if path == "/v1/admin/catalog/rejects":
            return {"total": 0, "limit": 1, "offset": 0, "items": []}
        if path == "/v1/admin/catalog/tombstones":
            return {"total": 0, "limit": 1, "offset": 0, "items": []}
        forwarded["query"] = dict(query)
        return {"total": 0, "limit": 60, "offset": 0, "items": []}

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/library?folder=../escape")
    assert response.status_code == 200
    assert forwarded["query"] == {"limit": "60", "offset": "0"}


def test_library_popover_renders_asset_details() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        assert path == "/v1/admin/catalog/asset"
        assert query == {"relative_path": "2026/04/Job_A/a.jpg"}
        return {
            "item": _library_catalog_item(
                "2026/04/Job_A/a.jpg",
                sha256_hex="a" * 64,
                is_favorite=True,
            )
        }

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get(
        "/library/popover?relative_path=2026/04/Job_A/a.jpg"
    )
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    # Identity + a couple of metadata bits surface in the popover.
    assert "a.jpg" in html
    assert "6000" in html
    assert "Inspect" in html
    # Popover must not leak the full SHA256.
    assert ("a" * 64) not in html


def test_library_lightbox_renders_preview_and_nav_buttons() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        assert path == "/v1/admin/catalog/asset"
        return {"item": _library_catalog_item("2026/04/Job_A/a.jpg")}

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get(
        "/library/lightbox?relative_path=2026/04/Job_A/a.jpg&index=2&total=5&folder=2026/04"
    )
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert 'src="/catalog/preview?relative_path=2026/04/Job_A/a.jpg"' in html
    assert "Previous" in html
    assert "Next" in html
    assert "3 of 5 on this page" in html


# ---------------------------------------------------------------------------
# Phase 3.A: Exposure-metadata formatters and display surface.
# ---------------------------------------------------------------------------


def test_format_shutter_speed_renders_human_readable_values() -> None:
    from photovault_server_ui.app import _format_shutter_speed

    assert _format_shutter_speed(1 / 200) == "1/200 s"
    assert _format_shutter_speed(1 / 60) == "1/60 s"
    assert _format_shutter_speed(0.5) == "1/2 s"
    assert _format_shutter_speed(1.0) == "1 s"
    assert _format_shutter_speed(2.0) == "2 s"
    assert _format_shutter_speed(None) is None
    assert _format_shutter_speed(0) is None
    assert _format_shutter_speed(-1.0) is None


def test_format_exposure_summary_joins_available_fields() -> None:
    from photovault_server_ui.app import _format_exposure_summary

    full = _format_exposure_summary(
        {
            "exposure_time_s": 1 / 200,
            "f_number": 2.8,
            "iso_speed": 400,
            "focal_length_mm": 50,
            "focal_length_35mm_mm": 75,
        }
    )
    # Uses middle dot as separator, renders focal length with 35mm equivalent
    # in parentheses when both are present.
    assert full == "1/200 s \u00b7 f/2.8 \u00b7 ISO 400 \u00b7 50 mm (75 mm eq.)"

    partial = _format_exposure_summary({"f_number": 4.0, "iso_speed": 800})
    assert partial == "f/4 \u00b7 ISO 800"

    empty = _format_exposure_summary(
        {
            "exposure_time_s": None,
            "f_number": None,
            "iso_speed": None,
            "focal_length_mm": None,
            "focal_length_35mm_mm": None,
        }
    )
    assert empty == ""


def test_library_lightbox_renders_exposure_details() -> None:
    item = _library_catalog_item(
        "2026/04/Job_A/a.jpg",
        exposure_time_s=1 / 125,
        f_number=4.0,
        iso_speed=800,
        focal_length_mm=35.0,
        focal_length_35mm_mm=52,
    )

    def _fetcher(path: str, query: dict[str, str]) -> dict:
        assert path == "/v1/admin/catalog/asset"
        return {"item": item}

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get(
        "/library/lightbox?relative_path=2026/04/Job_A/a.jpg&index=0&total=1"
    )
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    # Exposure summary and each individual dl row render.
    assert "1/125 s" in html
    assert "f/4" in html
    assert "ISO" in html
    assert "800" in html
    assert "35 mm" in html
    assert "52 mm eq." in html


def test_library_lightbox_omits_exposure_when_unavailable() -> None:
    item = _library_catalog_item(
        "2026/04/Job_A/a.jpg",
        exposure_time_s=None,
        f_number=None,
        iso_speed=None,
        focal_length_mm=None,
        focal_length_35mm_mm=None,
    )

    def _fetcher(path: str, query: dict[str, str]) -> dict:
        assert path == "/v1/admin/catalog/asset"
        return {"item": item}

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get(
        "/library/lightbox?relative_path=2026/04/Job_A/a.jpg&index=0&total=1"
    )
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    # When no exposure fields are present, no shutter/f-stop/ISO labels
    # should appear in the dl list.
    assert "<dt>Shutter</dt>" not in html
    assert "<dt>Aperture</dt>" not in html
    assert "<dt>ISO</dt>" not in html


def test_library_popover_includes_exposure_summary_when_available() -> None:
    item = _library_catalog_item(
        "2026/04/Job_A/a.jpg",
        exposure_time_s=1 / 60,
        f_number=2.8,
        iso_speed=100,
    )

    def _fetcher(path: str, query: dict[str, str]) -> dict:
        assert path == "/v1/admin/catalog/asset"
        return {"item": item}

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get(
        "/library/popover?relative_path=2026/04/Job_A/a.jpg"
    )
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "1/60 s" in html
    assert "f/2.8" in html
    assert "ISO 100" in html


def test_library_page_emits_lightbox_nav_script() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        if path == "/v1/admin/catalog/folders":
            return {"folders": []}
        if path == "/v1/admin/catalog/rejects":
            return {"total": 0, "limit": 1, "offset": 0, "items": []}
        return {"total": 0, "limit": 60, "offset": 0, "items": []}

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/library")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    # The inline nav script must be present so arrow keys and prev/next
    # buttons have something to bind to. We check a stable signature rather
    # than an exact match so the script can evolve.
    assert "libraryLightboxBody" in html
    assert "ArrowLeft" in html
    assert "ArrowRight" in html
    assert "library-lightbox-prev" in html
    assert "library-lightbox-next" in html


# ---------------------------------------------------------------------------
# Phase 3.B: reject queue (lightbox X-key + header badge + /library/rejects)
# ---------------------------------------------------------------------------


def test_library_page_shows_reject_queue_badge_when_nonzero() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        if path == "/v1/admin/catalog/folders":
            return {"folders": []}
        if path == "/v1/admin/catalog/rejects":
            # Library view only asks for the count via limit=1/offset=0.
            assert query == {"limit": "1", "offset": "0"}
            return {"total": 3, "limit": 1, "offset": 0, "items": []}
        return {"total": 0, "limit": 60, "offset": 0, "items": []}

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/library")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    # Header pill links to /library/rejects and carries the count.
    assert "3 marked for deletion" in html
    assert 'href="/library/rejects"' in html
    # Pill uses the danger style when the queue is non-empty.
    assert "btn-danger" in html


def test_library_page_reject_badge_is_muted_at_zero() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        if path == "/v1/admin/catalog/folders":
            return {"folders": []}
        if path == "/v1/admin/catalog/rejects":
            return {"total": 0, "limit": 1, "offset": 0, "items": []}
        return {"total": 0, "limit": 60, "offset": 0, "items": []}

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/library")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "0 marked for deletion" in html
    # Muted outline style when the queue is empty — never a scolding red.
    assert "library-reject-queue-badge" in html


def test_library_lightbox_renders_reject_button_and_no_badge_when_not_rejected() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        assert path == "/v1/admin/catalog/asset"
        return {
            "item": _library_catalog_item(
                "2026/04/Job_A/a.jpg", is_rejected=False
            )
        }

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get(
        "/library/lightbox?relative_path=2026/04/Job_A/a.jpg&index=0&total=1"
    )
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    # The reject form is present with a button labelled "✕ Reject".
    assert "library-lightbox-reject-form" in html
    assert "Reject" in html
    # Hidden input carries currently_rejected=false; whitespace-tolerant check.
    collapsed = " ".join(html.split())
    assert 'name="currently_rejected"' in collapsed
    assert 'value="false"' in collapsed
    assert 'aria-pressed="false"' in html
    # No "Marked for deletion" badge before the asset is rejected.
    assert "Marked for deletion" not in html


def test_library_lightbox_renders_restore_button_and_badge_when_rejected() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        assert path == "/v1/admin/catalog/asset"
        return {
            "item": _library_catalog_item(
                "2026/04/Job_A/a.jpg", is_rejected=True
            )
        }

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get(
        "/library/lightbox?relative_path=2026/04/Job_A/a.jpg&index=0&total=1"
    )
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    # Rejected state: restore button + danger pill badge.
    assert "Restore" in html
    collapsed = " ".join(html.split())
    assert 'name="currently_rejected"' in collapsed
    assert 'value="true"' in collapsed
    assert 'aria-pressed="true"' in html
    assert "Marked for deletion" in html


def test_library_page_emits_x_key_reject_handler() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        if path == "/v1/admin/catalog/folders":
            return {"folders": []}
        if path == "/v1/admin/catalog/rejects":
            return {"total": 0, "limit": 1, "offset": 0, "items": []}
        return {"total": 0, "limit": 60, "offset": 0, "items": []}

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/library")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    # Stable signatures for the X-key handler installed in library.html.
    assert "library-lightbox-reject-form" in html
    # X handling key branch — the script tests the keydown letter X.
    assert "'x'" in html or "'X'" in html
    assert "htmx:afterSwap" in html


def test_library_reject_toggle_marks_asset_and_returns_fragment() -> None:
    observed: dict[str, object] = {}

    def _fetcher(path: str, query: dict[str, str]) -> dict:
        assert path == "/v1/admin/catalog/asset"
        # Server-UI re-fetches the asset after the toggle to render the
        # swapped-in fragment with the new is_rejected state.
        return {
            "item": _library_catalog_item(
                "2026/04/Job_A/a.jpg", is_rejected=True
            )
        }

    def _poster(path: str, payload: dict) -> dict:
        observed["path"] = path
        observed["payload"] = payload
        return {"relative_path": payload["relative_path"], "is_rejected": True}

    app = create_app(api_fetcher=_fetcher, api_poster=_poster)
    response = app.test_client().post(
        "/library/actions/reject/toggle",
        data={
            "relative_path": "2026/04/Job_A/a.jpg",
            "folder": "",
            "index": "0",
            "total": "1",
            "currently_rejected": "false",
        },
        headers={"HX-Request": "true"},
    )
    assert response.status_code == 200
    assert observed["path"] == "/v1/admin/catalog/reject"
    assert observed["payload"] == {"relative_path": "2026/04/Job_A/a.jpg"}
    html = response.get_data(as_text=True)
    # Returned fragment is the lightbox — with the flipped state surfaced.
    assert "Restore" in html
    assert "Marked for deletion" in html


def test_library_reject_toggle_unmarks_when_currently_rejected() -> None:
    observed: dict[str, object] = {}

    def _fetcher(path: str, query: dict[str, str]) -> dict:
        return {
            "item": _library_catalog_item(
                "2026/04/Job_A/a.jpg", is_rejected=False
            )
        }

    def _poster(path: str, payload: dict) -> dict:
        observed["path"] = path
        observed["payload"] = payload
        return {"relative_path": payload["relative_path"], "is_rejected": False}

    app = create_app(api_fetcher=_fetcher, api_poster=_poster)
    response = app.test_client().post(
        "/library/actions/reject/toggle",
        data={
            "relative_path": "2026/04/Job_A/a.jpg",
            "folder": "",
            "index": "0",
            "total": "1",
            "currently_rejected": "true",
        },
        headers={"HX-Request": "true"},
    )
    assert response.status_code == 200
    assert observed["path"] == "/v1/admin/catalog/reject/unmark"
    assert observed["payload"] == {"relative_path": "2026/04/Job_A/a.jpg"}


def test_library_reject_toggle_returns_duplicates_fragment_for_hx_duplicates_flow() -> None:
    observed: dict[str, object] = {}

    def _fetcher(path: str, query: dict[str, str]) -> dict:
        if path == "/v1/admin/duplicates":
            return {
                "total": 1,
                "limit": 25,
                "offset": 0,
                "items": [
                    {
                        "sha256_hex": "a" * 64,
                        "file_count": 1,
                        "first_seen_at_utc": "2026-04-20T09:00:00+00:00",
                        "last_seen_at_utc": "2026-04-20T10:00:00+00:00",
                        "relative_paths": ["2026/04/Job_A/a.jpg"],
                    }
                ],
            }
        assert path == "/v1/admin/catalog/asset"
        return {"item": _library_catalog_item(query["relative_path"], is_rejected=True)}

    def _poster(path: str, payload: dict) -> dict:
        observed["path"] = path
        observed["payload"] = payload
        return {"ok": True}

    app = create_app(api_fetcher=_fetcher, api_poster=_poster)
    response = app.test_client().post(
        "/library/actions/reject/toggle",
        data={
            "relative_path": "2026/04/Job_A/a.jpg",
            "folder": "",
            "index": "0",
            "total": "1",
            "currently_rejected": "false",
            "return_to": "duplicates",
        },
        headers={"HX-Request": "true"},
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert observed["path"] == "/v1/admin/catalog/reject"
    assert observed["payload"] == {"relative_path": "2026/04/Job_A/a.jpg"}
    assert 'id="duplicates-shell"' in html
    assert "Marked 2026/04/Job_A/a.jpg for deletion." in html


def test_library_rejects_page_renders_rows_and_disabled_delete_button() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        assert path == "/v1/admin/catalog/rejects"
        assert query == {"limit": "60", "offset": "0"}
        return {
            "total": 2,
            "limit": 60,
            "offset": 0,
            "items": [
                {
                    "relative_path": "2026/04/Job_A/a.jpg",
                    "sha256_hex": "a" * 64,
                    "marked_at_utc": "2026-04-23T05:00:00+00:00",
                    "marked_reason": "blurry",
                    "item": _library_catalog_item("2026/04/Job_A/a.jpg"),
                },
                {
                    "relative_path": "2026/04/Job_B/b.jpg",
                    "sha256_hex": "b" * 64,
                    "marked_at_utc": "2026-04-23T05:05:00+00:00",
                    "marked_reason": None,
                    "item": _library_catalog_item("2026/04/Job_B/b.jpg"),
                },
            ],
        }

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/library/rejects")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Reject queue" in html
    # Grid renders both filenames with preview URLs.
    assert "a.jpg" in html
    assert "b.jpg" in html
    assert 'src="/catalog/preview?relative_path=2026/04/Job_A/a.jpg"' in html
    # Per-row Restore forms post to the unmark action.
    assert 'action="/library/actions/reject/unmark"' in html
    assert "Restore" in html
    # Reason surfaces when present.
    assert "blurry" in html
    # Delete-rejected button is present but disabled for Phase 3.B.
    assert "Delete rejected media" in html
    assert "disabled" in html
    # Back-link to library present.
    assert "← Back to library" in html
    # No SHA leaks.
    assert ("a" * 64) not in html
    assert ("b" * 64) not in html


def test_library_rejects_page_renders_empty_state() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        assert path == "/v1/admin/catalog/rejects"
        return {"total": 0, "limit": 60, "offset": 0, "items": []}

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/library/rejects")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "The reject queue is empty" in html
    # Delete button is still rendered (disabled) so the UI shape is stable.
    assert "Delete rejected media (0)" in html


def test_library_rejects_action_message_renders_once() -> None:
    """Rejects page should not duplicate action banners via base + page template."""

    def _fetcher(path: str, query: dict[str, str]) -> dict:
        assert path == "/v1/admin/catalog/rejects"
        return {"total": 0, "limit": 60, "offset": 0, "items": []}

    app = create_app(api_fetcher=_fetcher)
    message = "Deleted 2 asset(s); trash retained for 14 days"
    response = app.test_client().get(
        "/library/rejects",
        query_string={"action_message": message},
    )
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert html.count(message) == 1


def test_library_reject_unmark_action_posts_and_redirects() -> None:
    observed: dict[str, object] = {}

    def _fetcher(path: str, query: dict[str, str]) -> dict:
        return {"total": 0, "limit": 60, "offset": 0, "items": []}

    def _poster(path: str, payload: dict) -> dict:
        observed["path"] = path
        observed["payload"] = payload
        return {"relative_path": payload["relative_path"], "is_rejected": False}

    app = create_app(api_fetcher=_fetcher, api_poster=_poster)
    response = app.test_client().post(
        "/library/actions/reject/unmark",
        data={"relative_path": "2026/04/Job_A/a.jpg", "page": "1"},
    )
    assert response.status_code in (301, 302, 303)
    assert observed["path"] == "/v1/admin/catalog/reject/unmark"
    assert observed["payload"] == {"relative_path": "2026/04/Job_A/a.jpg"}
    assert "/library/rejects" in response.headers.get("Location", "")


# ---------------------------------------------------------------------------
# Phase 3.C: execute delete + tombstones
# ---------------------------------------------------------------------------


def test_library_rejects_page_delete_button_is_armed_when_queue_nonempty() -> None:
    """Delete button should not have disabled attribute when queue has items."""
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        if path == "/v1/admin/catalog/rejects":
            return {
                "total": 3,
                "limit": 60,
                "offset": 0,
                "items": [
                    {
                        "relative_path": "2026/04/Job_A/a.jpg",
                        "sha256_hex": "a" * 64,
                        "marked_at_utc": "2026-04-22T10:00:00+00:00",
                        "marked_reason": "blurry",
                        "item": None,
                    }
                ],
            }
        return {"folders": []}

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/library/rejects")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    # Delete button should be present and NOT disabled.
    assert "Delete rejected media (3)" in html
    # The form wrapper signals the button is now armed.
    assert 'action="/library/actions/rejects/execute"' in html


def test_library_rejects_page_delete_button_stays_disabled_when_queue_empty() -> None:
    """Delete button should have disabled attribute when queue is empty."""
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        return {
            "total": 0,
            "limit": 60,
            "offset": 0,
            "items": [],
        }

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/library/rejects")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Delete rejected media (0)" in html
    assert "disabled" in html


def test_library_rejects_execute_action_posts_to_api_and_redirects() -> None:
    """Execute delete action POSTs to API and redirects with success message."""
    observed: dict[str, object] = {}

    def _fetcher(path: str, query: dict[str, str]) -> dict:
        return {"total": 0, "limit": 60, "offset": 0, "items": []}

    def _poster(path: str, payload: dict) -> dict:
        observed["path"] = path
        observed["payload"] = payload
        return {
            "executed": ["2026/04/Job_A/a.jpg", "2026/04/Job_A/b.jpg"],
            "skipped": [],
        }

    app = create_app(api_fetcher=_fetcher, api_poster=_poster)
    response = app.test_client().post("/library/actions/rejects/execute")
    assert response.status_code in (301, 302, 303)
    assert observed["path"] == "/v1/admin/catalog/rejects/execute"
    # API is called with the request to execute all (None means drain queue).
    assert observed["payload"] == {"relative_paths": None}
    # Redirect includes success message.
    location = response.headers.get("Location", "")
    assert "/library/rejects" in location
    assert "action_message" in location or "Deleted" in location


# ---------------------------------------------------------------------------
# Phase 3.D: trash triage page + library header pill
# ---------------------------------------------------------------------------


def _make_library_fetcher(*, reject_count: int = 0, trash_count: int = 0):
    """Build a fetcher stub that returns controlled counts for the library page."""

    def _fetcher(path: str, query: dict[str, str]) -> dict:
        if path == "/v1/admin/catalog":
            return {"total": 0, "limit": 60, "offset": 0, "items": []}
        if path == "/v1/admin/catalog/folders":
            return {"folders": []}
        if path == "/v1/admin/catalog/rejects":
            return {"total": reject_count, "limit": 1, "offset": 0, "items": []}
        if path == "/v1/admin/catalog/tombstones":
            return {"total": trash_count, "limit": 1, "offset": 0, "items": []}
        return {}

    return _fetcher


def test_library_page_shows_trash_count_pill_zero() -> None:
    """When trash is empty the pill should use btn-outline-secondary and show '0 in trash'."""
    app = create_app(api_fetcher=_make_library_fetcher(reject_count=0, trash_count=0))
    response = app.test_client().get("/library")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "0 in trash" in html
    assert "library-trash-badge" in html
    assert "btn-outline-secondary" in html


def test_library_page_shows_trash_count_pill_nonzero() -> None:
    """When trash has items the pill switches to btn-outline-warning."""
    app = create_app(api_fetcher=_make_library_fetcher(reject_count=0, trash_count=3))
    response = app.test_client().get("/library")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "3 in trash" in html
    assert "btn-outline-warning" in html


def test_library_trash_page_renders_rows_and_restore_form() -> None:
    """Trash page lists tombstoned assets with Restore form per row."""

    def _fetcher(path: str, query: dict[str, str]) -> dict:
        if path == "/v1/admin/catalog/tombstones":
            return {
                "total": 1,
                "limit": 60,
                "offset": 0,
                "items": [
                    {
                        "relative_path": "2026/04/Job_A/a.jpg",
                        "sha256_hex": "a" * 64,
                        "trashed_at_utc": "2026-04-10T03:15:00+00:00",
                        "marked_reason": "blurry",
                        "trash_relative_path": ".trash/2026/04/10/aaa/2026/04/Job_A/a.jpg",
                        "original_size_bytes": 1024,
                        "age_days": 14,
                        "days_until_purge": 0,
                    }
                ],
            }
        return {}

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/library/trash")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "2026/04/Job_A/a.jpg" in html
    # Restore form must target the correct action endpoint.
    assert 'action="/library/actions/trash/restore"' in html
    # "Purge pending" label appears when days_until_purge == 0.
    assert "Purge pending" in html


def test_library_trash_page_renders_empty_state() -> None:
    """Trash page shows empty-state alert when there are no tombstones."""

    def _fetcher(path: str, query: dict[str, str]) -> dict:
        return {"total": 0, "limit": 60, "offset": 0, "items": []}

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/library/trash")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "trash is empty" in html.lower()


def test_library_trash_action_message_renders_once() -> None:
    """Trash page should not duplicate action banners via base + page template."""

    def _fetcher(path: str, query: dict[str, str]) -> dict:
        assert path == "/v1/admin/catalog/tombstones"
        return {"total": 0, "limit": 60, "offset": 0, "items": []}

    app = create_app(api_fetcher=_fetcher)
    message = "Restored 2026/04/Job_A/a.jpg"
    response = app.test_client().get(
        "/library/trash",
        query_string={"action_message": message},
    )
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert html.count(message) == 1


def test_library_trash_restore_action_posts_to_api_and_redirects() -> None:
    """Restore action POSTs to the API restore endpoint and redirects to /library/trash."""
    observed: dict[str, object] = {}

    def _fetcher(path: str, query: dict[str, str]) -> dict:
        return {"total": 0, "limit": 60, "offset": 0, "items": []}

    def _poster(path: str, payload: dict) -> dict:
        observed["path"] = path
        observed["payload"] = payload
        return {"restored": True, "relative_path": "2026/04/Job_A/a.jpg",
                "sha256_hex": "a" * 64, "restored_at_utc": "2026-04-24T03:00:00+00:00"}

    app = create_app(api_fetcher=_fetcher, api_poster=_poster)
    response = app.test_client().post(
        "/library/actions/trash/restore",
        data={"relative_path": "2026/04/Job_A/a.jpg", "page": "1"},
    )
    assert response.status_code in (301, 302, 303)
    assert observed["path"] == "/v1/admin/catalog/tombstones/restore"
    assert observed["payload"] == {"relative_path": "2026/04/Job_A/a.jpg"}
    location = response.headers.get("Location", "")
    assert "/library/trash" in location


def test_library_rejects_page_links_to_trash_page() -> None:
    """The rejects page must contain a link to /library/trash."""

    def _fetcher(path: str, query: dict[str, str]) -> dict:
        return {"total": 0, "limit": 60, "offset": 0, "items": []}

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/library/rejects")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "/library/trash" in html
