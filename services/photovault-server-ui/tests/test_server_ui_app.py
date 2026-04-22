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
    assert "Total known SHA256" in html
    assert ">3<" in html
    assert "Last indexed file" in html
    assert "2026-04-20T11:00:00+00:00" in html
    assert "Latest Index Run" in html
    assert "2026-04-20T11:05:00+00:00" in html
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
        assert path == "/v1/admin/duplicates"
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

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/duplicates")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Duplicate SHA Groups" in html
    assert "2026/04/Trip/a.jpg" in html
    assert "2026/04/TripCopy/a.jpg" in html
    assert "2 path(s)" in html


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
    assert "2026-04-20T11:31:00+00:00" in html
    assert "No path conflicts have been recorded." not in html


def test_catalog_page_renders_rows_extraction_states_and_metadata_summary() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
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
    assert "Media Catalog" in html
    assert "2026/04/Job_A/a.jpg" in html
    assert "2026/04/Job_A/b.jpg" in html
    assert "succeeded" in html
    assert "failed" in html
    assert "Failure detail" in html
    assert "invalid media content" in html
    assert "camera Canon EOS R6" in html
    assert "6000x4000" in html
    assert "2.0 KiB" in html


def test_catalog_page_empty_state_is_clear() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
        assert path == "/v1/admin/catalog"
        return {"total": 0, "limit": 50, "offset": 0, "items": []}

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/catalog")
    assert response.status_code == 200
    assert "No catalog assets are available yet." in response.get_data(as_text=True)


def test_catalog_page_pagination_is_sane() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
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


def test_catalog_retry_action_calls_existing_api_and_sets_success_message() -> None:
    observed: dict[str, object] = {}

    def _fetcher(path: str, query: dict[str, str]) -> dict:
        assert path == "/v1/admin/catalog"
        assert query == {"limit": "50", "offset": "0"}
        return {
            "total": 1,
            "limit": 50,
            "offset": 0,
            "items": [
                {
                    "relative_path": "2026/04/Job_A/a.jpg",
                    "sha256_hex": "a" * 64,
                    "size_bytes": 100,
                    "origin_kind": "uploaded",
                    "last_observed_origin_kind": "uploaded",
                    "provenance_job_name": None,
                    "provenance_original_filename": None,
                    "first_cataloged_at_utc": "2026-04-22T10:00:00+00:00",
                    "last_cataloged_at_utc": "2026-04-22T10:00:00+00:00",
                    "extraction_status": "succeeded",
                    "extraction_last_attempted_at_utc": "2026-04-22T10:01:00+00:00",
                    "extraction_last_succeeded_at_utc": "2026-04-22T10:01:00+00:00",
                    "extraction_last_failed_at_utc": None,
                    "extraction_failure_detail": None,
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

    def _poster(path: str, payload: dict) -> dict:
        observed["path"] = path
        observed["payload"] = payload
        return {"item": {"relative_path": payload["relative_path"]}}

    app = create_app(api_fetcher=_fetcher, api_poster=_poster)
    client = app.test_client()
    response = client.post(
        "/catalog/actions/retry",
        data={"relative_path": "2026/04/Job_A/a.jpg", "page": "1"},
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert observed["path"] == "/v1/admin/catalog/extraction/retry"
    assert observed["payload"] == {"relative_path": "2026/04/Job_A/a.jpg"}
    assert "Retried extraction for 2026/04/Job_A/a.jpg." in response.get_data(as_text=True)


def test_catalog_backfill_action_calls_existing_api_and_sets_summary_message() -> None:
    observed: dict[str, object] = {}

    def _fetcher(path: str, query: dict[str, str]) -> dict:
        assert path == "/v1/admin/catalog"
        return {"total": 0, "limit": 50, "offset": 0, "items": []}

    def _poster(path: str, payload: dict) -> dict:
        observed["path"] = path
        observed["payload"] = payload
        return {
            "requested_statuses": ["pending", "failed"],
            "selected_count": 5,
            "processed_count": 5,
            "succeeded_count": 4,
            "failed_count": 1,
            "items": [],
        }

    app = create_app(api_fetcher=_fetcher, api_poster=_poster)
    client = app.test_client()
    response = client.post(
        "/catalog/actions/backfill",
        data={"target_statuses": "pending,failed", "limit": "50", "page": "1"},
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert observed["path"] == "/v1/admin/catalog/extraction/backfill"
    assert observed["payload"] == {"target_statuses": ["pending", "failed"], "limit": 50}
    assert "Backfill processed 5 asset(s): 4 succeeded, 1 failed." in response.get_data(as_text=True)


def test_catalog_page_filters_pending_assets() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
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
                }
            ],
        }

    app = create_app(api_fetcher=_fetcher)
    response = app.test_client().get("/catalog?extraction_status=pending")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "2026/04/Job_A/pending.jpg" in html
    assert "Not attempted yet." in html


def test_catalog_page_filters_failed_assets_and_shows_failure_detail() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
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
    response = app.test_client().get("/catalog?extraction_status=failed")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "2026/04/Job_A/failed.jpg" in html
    assert "Failure detail" in html
    assert "invalid media content" in html


def test_catalog_page_origin_filter_and_filtered_pagination_links() -> None:
    def _fetcher(path: str, query: dict[str, str]) -> dict:
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
