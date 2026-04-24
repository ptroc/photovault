# Phase 3.D handoff prompt (paste into a new Claude Code thread)

```
You are picking up Phase 3.D of the photovault v1.5-D server-UI library rollout.
Phases 3.A, 3.B, and 3.C are complete and green. 3.D is the cleanup phase:
retire the 14-day trash, give reviewers a restore surface for mistakes caught
before the purge, and make the purge itself a boring, observable cronjob.

## Start by reading

- /Users/ptroc/IdeaProjects/photovault/AGENTS.md — project-wide rules
  (additive-only schema, Protocol pattern, codex_log.md append required).
- /Users/ptroc/IdeaProjects/photovault/docs/proposals/server_ui_catalog_improvements.md
  — the Phase 3 design doc.
- /Users/ptroc/IdeaProjects/photovault/codex_log.md — tail the last three
  entries (3.A, 3.B, 3.C) for ground truth on what shipped.
- /Users/ptroc/IdeaProjects/photovault/services/photovault-api/src/photovault_api/state_store.py
  — look at `api_catalog_tombstones` table + the four tombstone Protocol
  methods. `remove_tombstone` is already implemented as a plumbing stub;
  `delete_media_asset` on the Postgres store is already in place for the
  restore path to reassert a catalog row.
- /Users/ptroc/IdeaProjects/photovault/services/photovault-api/src/photovault_api/app.py
  — `/v1/admin/catalog/rejects/execute`, `/v1/client/tombstone-report`, and
  the tombstone check inside /v1/upload/verify.
- /Users/ptroc/IdeaProjects/photovault/services/photovault-server-ui/src/photovault_server_ui/templates/library_rejects.html
  — armed "Delete rejected media" form from 3.C. This is where the restore
  page link will hang off.
- /Users/ptroc/IdeaProjects/photovault/scripts/ — pattern for existing
  helper scripts (deploy_rpi.sh etc.). New purge script lives here.

## Locked defaults from earlier phases (do NOT relitigate)

- 14-day trash retention.
- Strict tombstone permanence: restoring a tombstoned file un-tombstones it
  (row removed from api_catalog_tombstones), since restore is an explicit
  operator decision to reverse the soft-delete.
- Global scope (no per-job partitioning).
- Cheap batch client handshake at /v1/client/tombstone-report is the only
  client-facing tombstone surface.

## What 3.D must deliver

### Part 1 — Restore-from-trash UI

A new triage surface at /library/trash mirroring the shape of
/library/rejects. Reviewers open it to see the last 14 days of soft-deleted
assets and can either restore (move the file out of .trash/ back to its
original relative_path, remove the tombstone, re-insert the api_media_assets
row) or leave it to auto-purge.

API endpoints:

- GET /v1/admin/catalog/tombstones
  - Query: limit (1..500, default 50), offset (>=0), older_than_days
    (optional — filter the "expiring soonest" tail so the UI can highlight
    the ones about to be purged).
  - Response: { total, limit, offset, items: [TombstoneListItem] } where
    TombstoneListItem has { relative_path, sha256_hex, trashed_at_utc,
    marked_reason, trash_relative_path, original_size_bytes,
    age_days, days_until_purge }.
  - Sort by trashed_at_utc ASC (oldest first — those are the ones about to
    be purged and need operator attention).

- POST /v1/admin/catalog/tombstones/restore
  - Request: { relative_path: str }
  - Per-path atomic:
    1. Read the tombstone row (404 if absent).
    2. Move the file from <storage_root>/.trash/.../<trash_relative_path>
       back to <storage_root>/<relative_path>. Use os.replace inside the
       same filesystem; fall back to copy+unlink with fsync across
       filesystems. If the source is missing, return 409 with
       { code: "trash_gone", relative_path } — do NOT remove the tombstone
       (operator needs to know the physical file is actually lost).
    3. Call store.delete_media_asset on the (possibly-still-present?) row
       defensively, then re-insert the api_media_assets row with the
       original sha + size + origin_kind=restored + observed_at_utc=now.
       The Postgres store already has delete_media_asset in place; use it.
       Extraction/preview rows do NOT need to be recreated — they'll
       rebuild on first re-read via the existing backfill path.
    4. store.remove_tombstone(relative_path).
  - Response: { restored: true, relative_path, sha256_hex,
    restored_at_utc }.
  - All paths go through `_require_safe_relative_path`.

Server-UI wiring:

- POST /library/actions/trash/restore — thin proxy; redirects back to
  /library/trash with a flash message.
- GET /library/trash — new template library_trash.html, 60-per-page grid
  reusing .library-grid + a new .library-tile--trashed class (subtle
  slate-grey border instead of .library-tile--rejected's red). Per-row
  Restore form. A muted "Expires in N days" line on each tile.
- library.html header gets a second persistent pill next to the existing
  "N marked for deletion": "N in trash" linking to /library/trash. Muted
  when zero, btn-outline-warning when non-empty (distinct from the reject
  queue's danger pill so reviewers don't confuse "queued to delete" with
  "already deleted").
- library_rejects.html gets a small link under the page hero: "Need to
  undo a recent delete? → View trash".

### Part 2 — Purge worker (standalone cron script)

Per user preference: NOT a long-running daemon. A standalone Python script
invoked by cron.

Script location: /Users/ptroc/IdeaProjects/photovault/scripts/purge_trash.py

Design:

- Single-shot: run, do the work, exit.
- CLI: `python scripts/purge_trash.py --storage-root /path --database-url
  postgres://... [--dry-run] [--retention-days 14] [--max-batch 500]
  [--log-json]`.
- Fall back to env vars: STORAGE_ROOT, DATABASE_URL, PHOTOVAULT_PURGE_RETENTION_DAYS.
  Mirror the env/CLI discipline the existing photovault-api process uses.
- For each tombstone older than retention_days:
  1. Hard-delete the file at <storage_root>/<trash_relative_path>. Log and
     continue on FileNotFoundError (already-gone is success).
  2. DELETE FROM api_catalog_tombstones WHERE relative_path = %s.
- Report counters at the end (stdout): { scanned, purged_files, purged_rows,
  missing_files, errors, duration_seconds }. In --log-json mode, emit one
  NDJSON line per purge plus a summary line so cron mail + log aggregators
  stay tidy.
- Exit code: 0 on success, 1 on any error during the run (so cron sends
  mail). Never exit non-zero for already-gone files.
- Idempotent and safe to run concurrently with itself (use SELECT ... FOR
  UPDATE SKIP LOCKED per-row in the Postgres path so two overlapping cron
  invocations can't double-purge).
- No long-running connection: open the DB, pull the batch, close, do the
  filesystem work, reopen to delete rows. Keeps the script friendly to
  pgbouncer-style pools.

Cron integration:

- Add a sample crontab line to the script's docstring:
  `15 3 * * * root /opt/photovault/.venv/bin/python /opt/photovault/scripts/purge_trash.py --storage-root /data/photovault --database-url $DATABASE_URL --log-json >> /var/log/photovault/purge.log 2>&1`
- Do NOT add systemd timer / cron entry to any deploy script in this phase.
  Operator sets that up once manually per install; the docstring sample is
  enough.
- Add a scripts/README.md entry (or extend the existing one if present)
  describing: what the script does, how to dry-run it, what the expected
  output shape is, and the sample cron line.

### Part 3 — Store layer housekeeping

- In-memory store: confirm `remove_tombstone` already implements the full
  shape (delete row, return True/False). It was stubbed in 3.C; 3.D uses
  it for real.
- Add `list_tombstones(limit, offset, older_than_days=None)` Protocol
  method + both implementations. Return tuple[int, list[TombstoneRecord]]
  like the reject-queue shape. `older_than_days` is the hot filter for the
  purge worker and the "expiring soonest" tail in the UI.
- Add `purge_tombstones(older_than_days, max_batch)` Protocol method that
  returns the list of TombstoneRecord it selected and marked for deletion.
  The purge script is the ONLY caller. Postgres impl uses
  `SELECT ... FOR UPDATE SKIP LOCKED LIMIT %s` + batch DELETE in the same
  transaction.
- `delete_media_asset` is already present on the Postgres store. Verify
  the in-memory store has a matching implementation; add it if missing.
  Restore calls this before re-inserting so CASCADE-orphaned extraction
  rows are clean.

### Tests

In services/photovault-api/tests/test_api_app.py:
- admin_catalog_tombstones_list_returns_rows_sorted_oldest_first
- admin_catalog_tombstones_list_filters_by_older_than_days
- admin_catalog_tombstones_restore_round_trip — tombstone a file via the
  execute endpoint, then restore, verify file is back at original path,
  api_media_assets row present with origin_kind=restored, tombstone row
  gone.
- admin_catalog_tombstones_restore_returns_409_when_trash_file_missing
- admin_catalog_tombstones_restore_rejects_unsafe_relative_paths
- upload_verify_accepts_sha_again_after_restore — the strict-permanence
  rule is explicitly relaxed by restore; document this in a comment inside
  the test.

In services/photovault-api/tests/test_state_store.py:
- In-memory: list_tombstones sort + filter; purge_tombstones returns
  selected rows + deletes; remove_tombstone happy / absent.
- Postgres fake-cursor: list SQL carries older_than_days filter + ORDER
  BY trashed_at_utc ASC; purge SQL uses FOR UPDATE SKIP LOCKED.

In services/photovault-server-ui/tests/test_server_ui_app.py:
- library_page_shows_trash_count_pill — zero and non-zero variants.
- library_trash_page_renders_rows_and_restore_form
- library_trash_page_renders_empty_state
- library_trash_restore_action_posts_to_api_and_redirects
- library_rejects_page_links_to_trash_page

In services/photovault-api/tests/test_purge_trash_script.py (new file):
- purge_script_dry_run_reports_counts_without_touching_anything
- purge_script_deletes_expired_rows_and_files
- purge_script_tolerates_missing_files — pre-delete a file out of band,
  script still removes the row and exits 0.
- purge_script_respects_max_batch
- purge_script_exit_code_is_nonzero_on_db_error

Tests should invoke the script via subprocess with a fake DATABASE_URL
pointed at an in-memory test harness, OR refactor the script so its core
`run()` function takes a store + storage_root_path and can be called
directly. Prefer the latter — cleaner to test, easier to reason about.
The CLI entrypoint in the script is then a thin argparse shim over
`run()`.

### Verification expectations

- All of services/photovault-api/tests pass (currently ~100 post-3.C,
  expect ~110 after 3.D).
- All of services/photovault-server-ui/tests pass (currently ~49 post-3.C,
  expect ~54 after 3.D).
- `python3 -m ruff check services/photovault-api/src services/photovault-api/tests services/photovault-server-ui/src services/photovault-server-ui/tests scripts` returns "All checks passed!" (note the new `scripts` path).
- Append a codex_log.md entry dated in UTC, following the format of the
  3.C entry.

### Environment notes

- Host is macOS; project requires Python 3.11+. In a sandbox with only
  3.10, use /tmp/compat310 (sitecustomize that backports datetime.UTC +
  enum.StrEnum); flag this in the log entry.
- Run tests via `PYTHONPATH=/tmp/compat310:src:tests python3 -m pytest -q`
  from inside each service directory, or host-native via
  `source .venv/bin/activate && pytest services/photovault-api/tests services/photovault-server-ui/tests`.
- Purge script tests need access to the photovault_api package — add its
  src to PYTHONPATH in the test harness the same way the existing API
  tests do.

### Do NOT

- Do NOT add a background thread / asyncio task / systemd service for
  purge. The user explicitly chose "simple cronjob with separate python
  script". A daemon is out of scope.
- Do NOT auto-restore tombstones. Restore is always operator-initiated.
- Do NOT loosen strict tombstone permanence except via the explicit
  restore endpoint (which removes the tombstone row as part of the
  same atomic operation).
- Do NOT move the "Delete rejected media" button anywhere. 3.C's wiring
  stays as-is.
- Do NOT introduce non-additive schema changes. No new columns or tables
  without CREATE ... IF NOT EXISTS + ADD COLUMN IF NOT EXISTS.
- Do NOT rebuild extraction/preview rows in the restore path. They'll
  backfill on demand via the existing backfill endpoints.
- Do NOT write a GUI log viewer for the purge script. Operators read
  /var/log/photovault/purge.log directly; that's sufficient.

When everything is green and the log is appended, summarize what you
shipped, which tests are new, the sample cron line the operator should
install, and confirm Phase 3 is closed.
```
