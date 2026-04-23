# Phase 3.C handoff prompt (paste into a new Claude Code thread)

```
You are picking up Phase 3.C of the photovault v1.5-D server-UI library rollout.
Phase 3.A (exposure metadata + lightbox keyboard nav) and Phase 3.B (reject queue,
X-key hotkey, /library/rejects triage page) are complete and green. Phase 3.C is
the destructive path: execute the delete from the reject queue, with a 14-day
soft-delete trash, SHA tombstones so we never re-accept a file we just deleted,
and a client handshake so clients with the file locally drop it on next contact.

## Start by reading

- /Users/ptroc/IdeaProjects/photovault/AGENTS.md — project-wide rules (must
  follow): additive-only schema discipline, Protocol-pattern for state store,
  test-before-claim-done, codex_log.md append required after every change.
- /Users/ptroc/IdeaProjects/photovault/docs/proposals/server_ui_catalog_improvements.md
  — the Phase 3 design doc. Read the "Phase 3.C (deferred)" section if present;
  otherwise the "Reject queue" locked defaults live in the Phase 3.B section.
- /Users/ptroc/IdeaProjects/photovault/codex_log.md — tail the last two entries
  (2026-04-23T05:28:43Z for 3.A, 2026-04-23T20:03:49Z for 3.B) for ground truth
  on what shipped.
- /Users/ptroc/IdeaProjects/photovault/services/photovault-api/src/photovault_api/state_store.py
  — look for `RejectedAssetRecord`, `api_catalog_reject_queue` DDL,
  `add_catalog_reject` / `remove_catalog_reject` / `list_catalog_rejects`. The
  queue row intentionally carries sha256_hex in its own column (not via FK) so
  it survives ON DELETE CASCADE from api_media_assets — that is the hook for
  3.C's tombstone path.
- /Users/ptroc/IdeaProjects/photovault/services/photovault-api/src/photovault_api/app.py
  — `_require_safe_relative_path`, the three reject endpoints
  (/v1/admin/catalog/reject, /reject/unmark, /rejects), and the existing upload
  verify flow (for how client handshake is currently structured).
- /Users/ptroc/IdeaProjects/photovault/services/photovault-server-ui/src/photovault_server_ui/templates/library_rejects.html
  — the "Delete rejected media ({{ total }})" button is currently rendered
  `disabled` with `title="Destructive action ships in Phase 3.C"`. Flipping
  that is 3.C's only mandatory UI change to this page.

## Locked defaults (do NOT relitigate these)

1. Global reject-queue scope (no per-job partitioning).
2. 14-day soft-delete trash. Deleted files move to a trash subtree
   (proposed: <storage_root>/.trash/YYYY/MM/DD/<sha>/<original-relative-path>)
   with the original relative_path stored on a tombstone row. A purge worker
   (out of scope for 3.C; 3.D territory) will harden against permanent delete
   after 14 days. For 3.C, the trash move + tombstone is enough.
3. Strict tombstone permanence: once a sha256_hex is tombstoned, uploads of
   that sha must fail closed (HTTP 409 Conflict "tombstoned") regardless of
   the target relative_path. Restore-from-trash is a separate 3.D feature.
4. Client handshake: the existing upload heartbeat/verify surface gains a
   tombstone-report channel so a client can cheaply ask "is any of these
   shas tombstoned?" and drop local copies. Keep the path cheap: batch
   lookup, no per-file round-trip.
5. X-key behavior unchanged. Only lightbox; already done in 3.B.

## What 3.C must deliver

### Store layer (additive schema)

- New table `api_catalog_tombstones`:
  - relative_path TEXT PRIMARY KEY (the original path at delete time)
  - sha256_hex TEXT NOT NULL (indexed — this is the hot lookup for uploads)
  - trashed_at_utc TEXT NOT NULL
  - marked_reason TEXT (copied from the queue row)
  - trash_relative_path TEXT NOT NULL (where the file now lives inside .trash/)
  - original_size_bytes BIGINT
- CREATE TABLE IF NOT EXISTS + CREATE INDEX IF NOT EXISTS idx on sha256_hex.
- Protocol methods on UploadStateStore (same discipline as 3.B):
  - add_tombstone(...) -> TombstoneRecord
  - is_sha_tombstoned(sha256_hex: str) -> bool
  - list_sha_tombstones(shas: list[str]) -> list[TombstoneRecord]  (batch)
  - remove_tombstone(relative_path: str) -> bool  (for 3.D restore, not used
    in 3.C but add the shape now so 3.D is a no-op plumbing change)
- In-memory impl + Postgres impl, Lock-guarded, same split as 3.B.

### Delete execute endpoint (API)

POST /v1/admin/catalog/rejects/execute

- Request: { relative_paths: list[str] | None } — when None, executes ALL
  currently-queued rejects. When present, only the listed paths (must
  intersect the queue; silently skip paths not in the queue so two reviewers
  racing a partial selection can't 409 each other).
- For each target path in one transaction per path (not one giant txn — we
  want per-file atomicity so a half-way failure leaves a consistent state):
  1. Read the reject_queue row (for sha + reason).
  2. Move the file from <storage_root>/<relative_path> to
     <storage_root>/.trash/YYYY/MM/DD/<sha>/<relative_path>. Use os.replace
     inside the same filesystem; fall back to copy+unlink with fsync across
     filesystems. Skip with a warning if the source is already gone (idempotent
     across double-clicks).
  3. INSERT into api_catalog_tombstones.
  4. DELETE from api_media_assets WHERE relative_path = %s — which cascades
     the queue row and extraction/preview rows automatically.
- Response: { executed: list[{relative_path, sha256_hex, trash_relative_path}],
  skipped: list[{relative_path, reason}] }
- All paths go through `_require_safe_relative_path`.

### Upload verify: tombstone check

In /v1/upload/verify (and any upload heartbeat that checks sha), call
store.is_sha_tombstoned(sha) BEFORE ingestion completes; if true, return
HTTP 409 with a typed error body { code: "sha_tombstoned", sha256_hex,
trashed_at_utc }. Existing tests on the verify path will need a new case.

### Client handshake endpoint

POST /v1/client/tombstone-report

- Request: { sha256_hex: list[str] } (cap at 500 per call; validate)
- Response: { tombstoned: list[{sha256_hex, relative_path, trashed_at_utc}] }
- This is the "cheap batch lookup" clients use to drop local copies. Auth:
  same client-auth header check as the other /v1/client/ endpoints (mirror
  an existing one — e.g. the heartbeat — to stay consistent).

### Server-UI wiring

- POST /library/actions/rejects/execute — posts to the new API endpoint,
  then redirects to /library/rejects with a flash message
  ("Deleted N assets; trash retained for 14 days"). Non-HTMX only; the
  triage page is a full reload surface, not a fragment.
- library_rejects.html: remove the `disabled` + `title="..."` attributes on
  the "Delete rejected media" button; change it to a form submit. Keep the
  button red (btn-danger). Show a confirm dialog via a small inline
  <script> (native confirm()) so a stray click doesn't wipe the queue.
- Keep /library/rejects reachable when the queue is empty — the button just
  becomes `disabled` naturally when {{ total }} == 0.

### Tests (follow the 3.B pattern exactly)

In services/photovault-api/tests/test_api_app.py:
- tombstone_created_by_execute_move_and_row_inserted — happy path, single
  path, verifies the file left the catalog subtree and appeared in .trash,
  api_media_assets row gone, tombstone row present with correct sha.
- execute_is_idempotent_for_missing_source_file — file pre-deleted out of
  band; endpoint still marks the row gone + writes tombstone.
- execute_rejects_unsafe_relative_paths — traversal etc., same shape as 3.B.
- execute_without_request_body_drains_whole_queue.
- upload_verify_returns_409_for_tombstoned_sha — uploads same sha after
  execute; must fail closed.
- client_tombstone_report_returns_matches_only_for_reported_shas —
  batch-in, batch-out, extra shas aren't leaked.
- client_tombstone_report_requires_client_auth — mirror existing heartbeat
  auth test shape.

In services/photovault-api/tests/test_state_store.py:
- In-memory: add_tombstone round-trip; is_sha_tombstoned; batch list;
  remove_tombstone shape (future-proof for 3.D).
- Postgres fake-cursor: execute SQL carries the expected params tuple and
  the ON CONFLICT / index creation discipline (same pattern as the 3.B
  reject-queue tests).

In services/photovault-server-ui/tests/test_server_ui_app.py:
- rejects_page_delete_button_is_armed_when_queue_nonempty — no `disabled`,
  form posts to /library/actions/rejects/execute.
- rejects_page_delete_button_stays_disabled_when_queue_empty.
- rejects_execute_action_posts_to_api_and_redirects — fake poster captures
  the call; redirect target is /library/rejects.

### Verification expectations (match 3.B)

- All of services/photovault-api/tests pass (currently 92 → expect ~100).
- All of services/photovault-server-ui/tests pass (currently 46 → expect ~49).
- `python3 -m ruff check services/photovault-api/src services/photovault-api/tests services/photovault-server-ui/src services/photovault-server-ui/tests` returns "All checks passed!"
- Append a codex_log.md entry dated in UTC, following the exact format of the
  2026-04-23T20:03:49Z entry (Summary / Files / Verification sections).

### Environment notes

- Host is macOS; project requires Python 3.11+ (pyproject.toml). If you are
  in a sandbox with only Python 3.10, use the same compat shim approach —
  /tmp/compat310/sitecustomize.py that backports datetime.UTC and
  enum.StrEnum — and flag that in the log entry.
- Run tests via `PYTHONPATH=/tmp/compat310:src:tests python3 -m pytest -q`
  from inside each service directory, or host-native via
  `source .venv/bin/activate && pytest services/photovault-api/tests services/photovault-server-ui/tests`.

### Do NOT

- Do NOT touch Phase 3.A exposure fields, lightbox keyboard nav script, or
  the 3.B reject-queue Protocol methods. They are stable.
- Do NOT introduce non-additive schema changes. Every new column or table
  MUST use CREATE TABLE IF NOT EXISTS / ADD COLUMN IF NOT EXISTS.
- Do NOT build a 3.D restore UI. Add the `remove_tombstone` Protocol method
  for plumbing only; no endpoint, no template change.
- Do NOT wire a purge worker. 14-day auto-purge is 3.D.
- Do NOT enable the delete execute action for non-authenticated traffic.
  Mirror the auth shape of /v1/admin/catalog/backfill.

When everything is green and the log is appended, summarize what you shipped,
which tests are new, and what was deliberately left for 3.D.
```
