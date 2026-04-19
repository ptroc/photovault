from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from photovault_clientd.app import create_app
from photovault_clientd.db import open_db, run_state_invariant_checks


def test_invariant_endpoint_reports_clean_state_for_new_db(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    app = create_app(db_path=db_path, staging_root=tmp_path / "staging")

    with TestClient(app) as client:
        response = client.get("/diagnostics/invariants")

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["issue_count"] == 0
    assert body["issues"] == []


def test_run_state_invariant_checks_detects_unknown_file_status(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    conn = open_db(db_path)
    now = datetime.now(UTC).isoformat()
    conn.execute(
        """
        INSERT INTO ingest_jobs (id, media_label, status, created_at_utc, updated_at_utc)
        VALUES (1, 'sd-corrupt', 'DISCOVERING', ?, ?);
        """,
        (now, now),
    )
    conn.execute(
        """
        INSERT INTO ingest_files (
            id, job_id, source_path, staged_path, sha256_hex, size_bytes,
            status, retry_count, last_error, created_at_utc, updated_at_utc
        )
        VALUES (1, 1, '/media/sd/a.jpg', NULL, NULL, NULL, 'CORRUPT_STATUS', 0, NULL, ?, ?);
        """,
        (now, now),
    )
    conn.commit()

    issues = run_state_invariant_checks(conn)
    conn.close()

    assert any("unknown status" in issue for issue in issues)


def test_run_state_invariant_checks_detects_unknown_job_status(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    conn = open_db(db_path)
    now = datetime.now(UTC).isoformat()
    conn.execute(
        """
        INSERT INTO ingest_jobs (id, media_label, status, created_at_utc, updated_at_utc)
        VALUES (1, 'sd-bad-job', 'NOT_A_STATE', ?, ?);
        """,
        (now, now),
    )
    conn.commit()

    issues = run_state_invariant_checks(conn)
    conn.close()

    assert any("ingest_jobs contains unknown status values" in issue for issue in issues)


def test_startup_fails_closed_when_invariants_are_broken(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    conn = open_db(db_path)
    now = datetime.now(UTC).isoformat()
    conn.execute(
        """
        INSERT INTO ingest_jobs (id, media_label, status, created_at_utc, updated_at_utc)
        VALUES (1, 'sd-invalid', 'HASHING', ?, ?);
        """,
        (now, now),
    )
    conn.execute(
        """
        INSERT INTO ingest_files (
            id, job_id, source_path, staged_path, sha256_hex, size_bytes,
            status, retry_count, last_error, created_at_utc, updated_at_utc
        )
        VALUES (1, 1, '/media/sd/b.jpg', '/staging/b.jpg', NULL, 1, 'HASHED', 0, NULL, ?, ?);
        """,
        (now, now),
    )
    conn.commit()
    conn.close()

    app = create_app(db_path=db_path, staging_root=tmp_path / "staging")
    with pytest.raises(RuntimeError):
        with TestClient(app):
            pass

    conn = open_db(db_path)
    state_row = conn.execute("SELECT current_state FROM daemon_state WHERE id = 1;").fetchone()
    categories = conn.execute(
        "SELECT category FROM daemon_events ORDER BY id DESC LIMIT 10;"
    ).fetchall()
    conn.close()

    assert state_row is not None
    assert state_row[0] == "ERROR_DAEMON"
    category_values = {row[0] for row in categories}
    assert "INVARIANT_VIOLATION" in category_values
    assert "BOOTSTRAP_FAILURE" in category_values
