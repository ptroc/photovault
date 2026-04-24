"""Standalone trash-purge script for photovault.

Scans api_catalog_tombstones for rows older than ``--retention-days`` (default
14), hard-deletes the corresponding file from ``.trash/`` under the storage
root, and removes the tombstone row.  Designed to be invoked by cron once per
day — it is single-shot, idempotent, and safe to run concurrently with itself
(the Postgres implementation uses ``SELECT … FOR UPDATE SKIP LOCKED`` so two
overlapping cron jobs cannot double-purge the same row).

Exit codes
----------
0  All rows processed (including rows whose trash file was already gone).
1  At least one unrecoverable error occurred (DB connection failure, etc.).

Sample crontab line
-------------------
15 3 * * * root /opt/photovault/.venv/bin/python \\
    /opt/photovault/scripts/purge_trash.py \\
    --storage-root /data/photovault \\
    --database-url $DATABASE_URL \\
    --log-json \\
    >> /var/log/photovault/purge.log 2>&1
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

# ---------------------------------------------------------------------------
# Minimal store protocol — only the two methods the purge script needs
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TombstoneRecord:
    relative_path: str
    sha256_hex: str
    trashed_at_utc: str
    marked_reason: str | None
    trash_relative_path: str
    original_size_bytes: int


class PurgeStore(Protocol):
    def purge_tombstones(
        self, *, older_than_days: int, max_batch: int
    ) -> list[TombstoneRecord]: ...


# ---------------------------------------------------------------------------
# Core run() function — injectable store for clean testing
# ---------------------------------------------------------------------------

@dataclass
class PurgeResult:
    scanned: int = 0
    purged_files: int = 0
    purged_rows: int = 0
    missing_files: int = 0
    errors: int = 0
    duration_seconds: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "scanned": self.scanned,
            "purged_files": self.purged_files,
            "purged_rows": self.purged_rows,
            "missing_files": self.missing_files,
            "errors": self.errors,
            "duration_seconds": round(self.duration_seconds, 3),
        }


def run(
    store: PurgeStore,
    storage_root_path: Path,
    *,
    retention_days: int = 14,
    max_batch: int = 500,
    dry_run: bool = False,
    log_json: bool = False,
) -> PurgeResult:
    """Execute one purge pass.

    Parameters
    ----------
    store:
        A store implementation that exposes ``purge_tombstones``.
    storage_root_path:
        Absolute path to the photovault storage root (files live at
        ``storage_root_path/<trash_relative_path>``).
    retention_days:
        Tombstones older than this many days are eligible for purge.
    max_batch:
        Maximum number of tombstones to process in a single run.
    dry_run:
        When True, report counts without touching the filesystem or DB.
    log_json:
        When True, emit one NDJSON line per purged/missing row plus a
        summary line; otherwise write plain-text lines to stdout.

    Returns
    -------
    PurgeResult with counters.
    """
    result = PurgeResult()
    started_at = time.monotonic()

    def _emit(record: dict[str, Any]) -> None:
        if log_json:
            print(json.dumps(record), flush=True)
        else:
            print(
                " | ".join(f"{k}={v}" for k, v in record.items()),
                flush=True,
            )

    # Fetch (and in non-dry-run mode: atomically lock+delete) the eligible rows.
    if dry_run:
        # In dry-run mode we use list_tombstones if available, or fall back to
        # purge_tombstones on a throw-away in-memory store.  Since the Protocol
        # only exposes purge_tombstones (the real deletion path), dry-run must
        # read without deleting.  We achieve this by calling list_tombstones
        # when the store supports it (InMemory always does; Postgres does too
        # after 3.D), otherwise we call purge_tombstones and re-insert rows —
        # but that's only in tests with mocks.  The cleanest approach: check
        # for list_tombstones via hasattr and fall back gracefully.
        if hasattr(store, "list_tombstones"):
            _total, candidates = store.list_tombstones(  # type: ignore[attr-defined]
                limit=max_batch,
                offset=0,
                older_than_days=retention_days,
            )
        else:
            # Last resort: pretend zero candidates so dry-run is safe.
            candidates = []  # type: ignore[assignment]
    else:
        candidates = store.purge_tombstones(
            older_than_days=retention_days, max_batch=max_batch
        )

    result.scanned = len(candidates)

    for tombstone in candidates:
        trash_path = storage_root_path / tombstone.trash_relative_path
        row_log: dict[str, Any] = {
            "event": "purge",
            "relative_path": tombstone.relative_path,
            "sha256_hex": tombstone.sha256_hex[:12],
            "trash_relative_path": tombstone.trash_relative_path,
            "trashed_at_utc": tombstone.trashed_at_utc,
            "dry_run": dry_run,
        }
        if dry_run:
            row_log["status"] = "would_purge"
            _emit(row_log)
            result.purged_rows += 1
            continue

        # Hard-delete the physical file.
        try:
            trash_path.unlink()
            result.purged_files += 1
            row_log["status"] = "file_deleted"
        except FileNotFoundError:
            # Already gone — treat as success, row still removed.
            result.missing_files += 1
            row_log["status"] = "file_already_gone"
        except OSError as exc:
            result.errors += 1
            row_log["status"] = "error"
            row_log["error"] = str(exc)
            _emit(row_log)
            # Row was already deleted by purge_tombstones; log and continue.
            continue

        result.purged_rows += 1
        _emit(row_log)

    result.duration_seconds = time.monotonic() - started_at

    summary: dict[str, Any] = {
        "event": "summary",
        **result.to_dict(),
        "retention_days": retention_days,
        "max_batch": max_batch,
        "dry_run": dry_run,
        "completed_at_utc": datetime.now(UTC).isoformat(),
    }
    _emit(summary)
    return result


# ---------------------------------------------------------------------------
# Real Postgres store adapter (thin wrapper so purge_trash.py is self-contained)
# ---------------------------------------------------------------------------

@dataclass
class _PostgresPurgeStore:
    """Minimal Postgres adapter used by the CLI entry-point.

    Opens a fresh connection per operation so it stays friendly to
    pgbouncer-style pools.
    """

    database_url: str

    def _connect(self):  # type: ignore[return]
        try:
            import psycopg2  # type: ignore[import]
        except ImportError as exc:
            raise RuntimeError(
                "psycopg2 is required for the Postgres purge store: "
                "pip install psycopg2-binary"
            ) from exc
        return psycopg2.connect(self.database_url)

    def list_tombstones(
        self,
        *,
        limit: int,
        offset: int,
        older_than_days: int | None = None,
    ) -> tuple[int, list[TombstoneRecord]]:
        """Read-only variant used by dry-run."""
        cutoff = (datetime.now(UTC) - timedelta(days=older_than_days or 0)).isoformat()
        where_sql = "WHERE trashed_at_utc <= %s" if older_than_days is not None else ""
        params: list[Any] = [cutoff] if older_than_days is not None else []

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*) FROM api_catalog_tombstones {where_sql};",
                    params,
                )
                total = int(cur.fetchone()[0])
                cur.execute(
                    f"""
                    SELECT relative_path, sha256_hex, trashed_at_utc, marked_reason,
                           trash_relative_path, original_size_bytes
                    FROM api_catalog_tombstones
                    {where_sql}
                    ORDER BY trashed_at_utc ASC
                    LIMIT %s OFFSET %s;
                    """,
                    [*params, limit, offset],
                )
                rows = cur.fetchall()
        return total, [_row_to_record(r) for r in rows]

    def purge_tombstones(
        self,
        *,
        older_than_days: int,
        max_batch: int,
    ) -> list[TombstoneRecord]:
        cutoff = (datetime.now(UTC) - timedelta(days=older_than_days)).isoformat()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT relative_path, sha256_hex, trashed_at_utc, marked_reason,
                           trash_relative_path, original_size_bytes
                    FROM api_catalog_tombstones
                    WHERE trashed_at_utc <= %s
                    ORDER BY trashed_at_utc ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT %s;
                    """,
                    (cutoff, max_batch),
                )
                rows = cur.fetchall()
                if rows:
                    relative_paths = [str(r[0]) for r in rows]
                    cur.execute(
                        "DELETE FROM api_catalog_tombstones"
                        " WHERE relative_path = ANY(%s);",
                        (relative_paths,),
                    )
            conn.commit()
        return [_row_to_record(r) for r in rows]


def _row_to_record(row: tuple[Any, ...]) -> TombstoneRecord:
    return TombstoneRecord(
        relative_path=str(row[0]),
        sha256_hex=str(row[1]),
        trashed_at_utc=str(row[2]),
        marked_reason=str(row[3]) if row[3] is not None else None,
        trash_relative_path=str(row[4]),
        original_size_bytes=int(row[5]),
    )


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Hard-purge photovault trash files older than retention_days.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--storage-root",
        default=os.environ.get("STORAGE_ROOT", ""),
        help="Absolute path to the photovault storage root "
             "(env: STORAGE_ROOT).",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL", ""),
        help="Postgres connection URL "
             "(env: DATABASE_URL).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Report what would be purged without touching anything.",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=int(
            os.environ.get("PHOTOVAULT_PURGE_RETENTION_DAYS", "14")
        ),
        help="Tombstones older than this many days are eligible for purge "
             "(env: PHOTOVAULT_PURGE_RETENTION_DAYS, default: 14).",
    )
    parser.add_argument(
        "--max-batch",
        type=int,
        default=500,
        help="Maximum number of tombstones to process per run (default: 500).",
    )
    parser.add_argument(
        "--log-json",
        action="store_true",
        default=False,
        help="Emit NDJSON lines instead of plain-text key=value pairs.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:  # noqa: D401
    """CLI entry-point. Returns an exit code."""
    args = _parse_args(argv)

    storage_root = args.storage_root.strip()
    database_url = args.database_url.strip()

    if not storage_root:
        print(
            "ERROR: --storage-root (or STORAGE_ROOT env var) is required.",
            file=sys.stderr,
        )
        return 1
    if not database_url:
        print(
            "ERROR: --database-url (or DATABASE_URL env var) is required.",
            file=sys.stderr,
        )
        return 1

    storage_root_path = Path(storage_root)
    if not storage_root_path.is_dir():
        print(
            f"ERROR: storage root {storage_root!r} does not exist or is not a directory.",
            file=sys.stderr,
        )
        return 1

    try:
        store = _PostgresPurgeStore(database_url=database_url)
        result = run(
            store,
            storage_root_path,
            retention_days=args.retention_days,
            max_batch=args.max_batch,
            dry_run=args.dry_run,
            log_json=args.log_json,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    return 1 if result.errors > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
