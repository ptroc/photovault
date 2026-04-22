import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from photovault_clientd.app import create_app
from photovault_clientd.db import LATEST_SCHEMA_VERSION, MIGRATIONS, get_schema_version, open_db


def test_open_db_initializes_latest_schema_version(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    conn = open_db(db_path)
    version = get_schema_version(conn)
    foreign_keys = conn.execute("PRAGMA foreign_keys;").fetchone()
    conn.close()

    assert version == LATEST_SCHEMA_VERSION
    assert foreign_keys is not None
    assert foreign_keys[0] == 1


def test_open_db_upgrades_legacy_v1_schema_to_latest(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    conn = sqlite3.connect(db_path)
    MIGRATIONS[1](conn)
    conn.execute("PRAGMA user_version = 1;")
    conn.execute(
        """
        INSERT INTO ingest_jobs (media_label, status, created_at_utc, updated_at_utc)
        VALUES ('legacy-sd', 'DISCOVERING', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');
        """
    )
    conn.commit()
    conn.close()

    upgraded = open_db(db_path)
    version = get_schema_version(upgraded)
    event_table = upgraded.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='daemon_events';"
    ).fetchone()
    local_registry_table = upgraded.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='local_sha_registry';"
    ).fetchone()
    network_ap_config_table = upgraded.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='network_ap_config';"
    ).fetchone()
    detected_media_table = upgraded.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='detected_media';"
    ).fetchone()
    detected_media_events_table = upgraded.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='detected_media_events';"
    ).fetchone()
    server_auth_state_table = upgraded.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='server_auth_state';"
    ).fetchone()
    legacy_row = upgraded.execute("SELECT media_label FROM ingest_jobs WHERE id = 1;").fetchone()
    upgraded.close()

    assert version == LATEST_SCHEMA_VERSION
    assert event_table is not None
    assert local_registry_table is not None
    assert network_ap_config_table is not None
    assert detected_media_table is not None
    assert detected_media_events_table is not None
    assert server_auth_state_table is not None
    assert legacy_row is not None
    assert legacy_row[0] == "legacy-sd"


def test_open_db_rejects_newer_unknown_schema_version(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA user_version = 999;")
    conn.commit()
    conn.close()

    with pytest.raises(RuntimeError):
        _ = open_db(db_path)


def test_schema_endpoint_reports_current_and_latest_versions(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    app = create_app(db_path=db_path)

    with TestClient(app) as client:
        response = client.get("/schema")

    assert response.status_code == 200
    body = response.json()
    assert body["schema_version"] == LATEST_SCHEMA_VERSION
    assert body["latest_schema_version"] == LATEST_SCHEMA_VERSION
