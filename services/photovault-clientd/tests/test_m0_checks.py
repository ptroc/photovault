from datetime import UTC, datetime
from pathlib import Path

from fastapi.testclient import TestClient
from photovault_clientd.app import create_app
from photovault_clientd.db import open_db
from photovault_clientd.m0_checks import run_m0_foundation_checks


def test_m0_diagnostics_endpoint_reports_ok_for_clean_state(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    app = create_app(db_path=db_path, staging_root=tmp_path / "staging")

    with TestClient(app) as client:
        response = client.get("/diagnostics/m0")

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["resume_map_complete"] is True
    assert body["resume_map_terminal_clean"] is True
    assert body["invariants_ok"] is True


def test_run_m0_foundation_checks_surfaces_invariant_issues(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    conn = open_db(db_path)
    now = datetime.now(UTC).isoformat()

    conn.execute(
        """
        INSERT INTO ingest_jobs (id, media_label, status, created_at_utc, updated_at_utc)
        VALUES (1, 'sd-invalid-hash', 'HASHING', ?, ?);
        """,
        (now, now),
    )
    conn.execute(
        """
        INSERT INTO ingest_files (
            id, job_id, source_path, staged_path, sha256_hex, size_bytes,
            status, retry_count, last_error, created_at_utc, updated_at_utc
        )
        VALUES (1, 1, '/media/sd/x.jpg', '/staging/x.jpg', NULL, 100, 'HASHED', 0, NULL, ?, ?);
        """,
        (now, now),
    )
    conn.commit()

    checks = run_m0_foundation_checks(conn)
    conn.close()

    assert checks["resume_map_complete"] is True
    assert checks["resume_map_terminal_clean"] is True
    assert checks["invariants_ok"] is False
    assert checks["invariant_issue_count"] > 0
    assert any(
        "HASHED/READY_TO_UPLOAD files missing valid sha256_hex" in issue
        for issue in checks["invariant_issues"]
    )
