"""Tests for scripts/purge_trash.py.

The script exposes a ``run(store, storage_root_path, ...)`` function that
accepts any object satisfying the ``PurgeStore`` protocol.  Tests inject a
small in-memory store so no real Postgres connection or filesystem I/O is
required by the happy-path tests.

We also import the real ``InMemoryUploadStateStore`` from
``photovault_api.state_store`` to test the full store integration.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Ensure the purge script is importable from the repository root.
# The PYTHONPATH in the test runner already includes the API src/, but the
# script itself lives under scripts/.  We add it here so pytest can find it
# without modifying setup.cfg.
# ---------------------------------------------------------------------------
import importlib
import importlib.util
from dataclasses import dataclass, field
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).parent.parent.parent.parent  # photovault root
_SCRIPTS_DIR = _REPO_ROOT / "scripts"


def _import_purge_script():
    """Import purge_trash.py from the scripts/ directory."""
    import sys

    script_path = _SCRIPTS_DIR / "purge_trash.py"
    spec = importlib.util.spec_from_file_location("purge_trash", script_path)
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    # Register in sys.modules so @dataclass can resolve __module__ correctly.
    sys.modules["purge_trash"] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


purge_trash = _import_purge_script()
run = purge_trash.run
TombstoneRecord = purge_trash.TombstoneRecord


# ---------------------------------------------------------------------------
# Minimal in-memory stub for the PurgeStore protocol
# ---------------------------------------------------------------------------

@dataclass
class _StubStore:
    """Minimal store stub that tracks calls and returns pre-configured rows."""

    tombstones: list[TombstoneRecord] = field(default_factory=list)
    purge_called: bool = False
    raise_on_purge: Exception | None = None

    def purge_tombstones(
        self, *, older_than_days: int, max_batch: int
    ) -> list[TombstoneRecord]:
        if self.raise_on_purge is not None:
            raise self.raise_on_purge
        self.purge_called = True
        eligible = [t for t in self.tombstones][:max_batch]
        self.tombstones = [t for t in self.tombstones if t not in eligible]
        return eligible

    def list_tombstones(
        self, *, limit: int, offset: int, older_than_days: int | None = None
    ) -> tuple[int, list[TombstoneRecord]]:
        """Satisfy hasattr check in dry-run mode."""
        rows = self.tombstones[:limit]
        return len(self.tombstones), rows


def _make_tombstone(relative_path: str, trash_rel: str) -> TombstoneRecord:
    return TombstoneRecord(
        relative_path=relative_path,
        sha256_hex="a" * 64,
        trashed_at_utc="2026-04-01T00:00:00+00:00",
        marked_reason=None,
        trash_relative_path=trash_rel,
        original_size_bytes=100,
    )


# ---------------------------------------------------------------------------
# Test: dry-run reports counts without touching anything
# ---------------------------------------------------------------------------


def test_purge_script_dry_run_reports_counts_without_touching_anything(
    tmp_path: Path,
) -> None:
    """In dry-run mode no files are deleted and purge_tombstones is not called."""
    trash_rel = ".trash/2026/04/01/aaa/photo.jpg"
    trash_path = tmp_path / trash_rel
    trash_path.parent.mkdir(parents=True, exist_ok=True)
    trash_path.write_bytes(b"data")

    store = _StubStore(
        tombstones=[_make_tombstone("2026/04/Job_A/photo.jpg", trash_rel)]
    )

    result = run(store, tmp_path, retention_days=14, dry_run=True, log_json=False)

    # File must still exist — dry-run must not touch anything.
    assert trash_path.is_file()
    # purge_tombstones must NOT have been called.
    assert not store.purge_called
    # But counters still reflect what would have been purged.
    assert result.scanned == 1
    assert result.purged_rows == 1  # dry-run counts "would purge"
    assert result.errors == 0


# ---------------------------------------------------------------------------
# Test: normal run deletes expired rows and files
# ---------------------------------------------------------------------------


def test_purge_script_deletes_expired_rows_and_files(tmp_path: Path) -> None:
    """Normal run removes physical files and the purge_tombstones is called."""
    trash_rel = ".trash/2026/04/01/aaa/photo.jpg"
    trash_path = tmp_path / trash_rel
    trash_path.parent.mkdir(parents=True, exist_ok=True)
    trash_path.write_bytes(b"real-data")

    store = _StubStore(
        tombstones=[_make_tombstone("2026/04/Job_A/photo.jpg", trash_rel)]
    )

    result = run(store, tmp_path, retention_days=14, dry_run=False, log_json=False)

    # Physical file must be gone.
    assert not trash_path.exists()
    # purge_tombstones was called (and emptied the stub store).
    assert store.purge_called
    assert len(store.tombstones) == 0
    assert result.scanned == 1
    assert result.purged_files == 1
    assert result.purged_rows == 1
    assert result.missing_files == 0
    assert result.errors == 0


# ---------------------------------------------------------------------------
# Test: tolerates missing files (already-gone is success)
# ---------------------------------------------------------------------------


def test_purge_script_tolerates_missing_files(tmp_path: Path) -> None:
    """If the trash file was removed out-of-band, the script still removes the
    row and exits cleanly (missing_files counter incremented, exit code 0)."""
    trash_rel = ".trash/2026/04/01/aaa/gone.jpg"
    # Deliberately do NOT create the file.

    store = _StubStore(
        tombstones=[_make_tombstone("2026/04/Job_A/gone.jpg", trash_rel)]
    )

    result = run(store, tmp_path, retention_days=14, dry_run=False, log_json=False)

    # Row still removed from the store.
    assert store.purge_called
    assert result.scanned == 1
    assert result.purged_files == 0
    assert result.missing_files == 1
    assert result.purged_rows == 1  # tombstone row removed despite missing file
    assert result.errors == 0


# ---------------------------------------------------------------------------
# Test: max_batch is respected
# ---------------------------------------------------------------------------


def test_purge_script_respects_max_batch(tmp_path: Path) -> None:
    """Only up to max_batch tombstones are processed per run."""
    # Create 5 tombstones but only process 2.
    tombstones = []
    for i in range(5):
        trash_rel = f".trash/2026/04/01/aaa/photo_{i}.jpg"
        trash_path = tmp_path / trash_rel
        trash_path.parent.mkdir(parents=True, exist_ok=True)
        trash_path.write_bytes(b"data")
        tombstones.append(
            _make_tombstone(f"2026/04/Job_A/photo_{i}.jpg", trash_rel)
        )

    store = _StubStore(tombstones=tombstones)

    result = run(
        store,
        tmp_path,
        retention_days=14,
        max_batch=2,
        dry_run=False,
        log_json=False,
    )

    assert result.scanned == 2
    assert result.purged_rows == 2
    # The remaining 3 tombstones are untouched in the stub.
    assert len(store.tombstones) == 3


# ---------------------------------------------------------------------------
# Test: exit code is non-zero on DB error
# ---------------------------------------------------------------------------


def test_purge_script_exit_code_is_nonzero_on_db_error(tmp_path: Path) -> None:
    """If purge_tombstones raises an exception, run() re-raises and main()
    returns exit code 1."""
    store = _StubStore(raise_on_purge=RuntimeError("connection refused"))

    # run() should propagate the exception.
    with pytest.raises(RuntimeError, match="connection refused"):
        run(store, tmp_path, retention_days=14, dry_run=False, log_json=False)

    # main() should catch the exception and return 1.
    result = purge_trash.main(
        [
            "--storage-root",
            str(tmp_path),
            "--database-url",
            "postgres://bad-url",
            "--retention-days",
            "14",
        ]
    )
    # main() calls _PostgresPurgeStore which will fail to import/connect —
    # this validates the exit-code-1 path.
    assert result == 1
