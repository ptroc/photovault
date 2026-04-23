```
Milestone: V1.5-D (Preview Pipeline & Media Library)
Contract compliance: confirmed
Docs impacted: none (proposal only — no code changes yet)
```

# Proposal: Catalog Browse Page Improvements

**Surface**: `photovault-server-ui` → `/catalog`
**Template**: `services/photovault-server-ui/src/photovault_server_ui/templates/catalog.html`
**View**: `services/photovault-server-ui/src/photovault_server_ui/app.py` — `catalog()` at line 436
**Status**: **Proposal, awaiting approval**. No code yet.

---

## 1. Why this page

The catalog browse page is the operator's main entry point into stored media. v1.5-D in `docs/photovault_v1_5_roadmap.md` names "Read-only media library browse/search/filter/detail UI" and "practical browse/filter/detail workflows" as explicit exit criteria. The page is functional today but has several friction points that make it feel more like a diagnostic dump than a library.

The rest of this doc is: (a) observations from the current template, (b) a three-phase improvement plan, (c) the exact diffs I'd propose for Phase 1, (d) non-goals, (e) open questions.

---

## 2. Observations about the current page

Reading `catalog.html` and the `catalog()` view, the page today has these specific pain points:

1. **No text search.** There is no way to find an asset by filename, camera make, or SHA prefix — only dropdown/date filters.
2. **No sort.** The API returns assets in a fixed order; the operator cannot sort by capture time, size, or recency.
3. **Verbose row layout.** Each asset is a full-width row with thumbnail + name + seven status badges + three metadata/provenance/cataloged chips. At 50/page this is ~15 scrolls for a single page.
4. **No grid view.** For a visual library, a dense thumbnail grid is often what the operator wants first; the detail-row layout is good for diagnostics but wrong as the default for "what's in the library".
5. **Every action is a full POST+redirect.** Favorite and Archive each trigger a full page reload even though the toggle is trivial. This is expensive on a slow network and loses scroll position.
6. **Filter form is wide and intimidating.** Nine filter fields all visible at once, with no grouping between common (extraction status, media type) and rare (preview capability, cataloged-before UTC).
7. **Datetime inputs are raw text.** `cataloged_since_utc` / `cataloged_before_utc` expect `2026-04-22T10:00:00+00:00` typed by hand. Error-prone.
8. **Verbose hidden-input duplication.** Each per-asset Favorite/Archive form re-declares ~10 hidden fields (every filter) to preserve query state. This is fragile and increases page size.
9. **No "active filters" indicator.** Once the operator submits filters, there is no visual reminder of what's active besides the select dropdowns — and no one-click way to remove a single filter.
10. **Pager is prev/next only.** No page-of-N indicator on the list itself (the filter card shows counts, but the pager doesn't).

None of these are bugs; the page works. They're UX debt that is worth paying down now that the API side is stable.

---

## 3. Proposed plan (three phases)

I'd like to split the work so each phase lands independently, in order of risk.

### Phase 1 — HTMX quick wins + visual polish

**No API changes. No schema changes. Only `catalog.html`, `app.css`, and possibly one small `/catalog/actions/favorite/toggle` HTMX-friendly endpoint.**

1. **Inline favorite / archive via HTMX.** Swap the button and badge in place on click. Keeps the scroll position and makes the page feel responsive.
2. **Active filters chip bar.** A single row near the top showing `Extraction: pending ✕ | Media type: raw ✕ | Since: 2026-04-01 ✕`, each chip being a link that removes just that filter. Replaces the "scan the dropdowns to remember what's filtered" step.
3. **Primary / Advanced filter split.** Show Extraction, Preview, Media type, Favorite, Archived inline. Collapse Origin, Preview capability, both date ranges behind an "Advanced" disclosure. Reduces initial visual weight by ~half.
4. **Native datetime-local inputs.** Replace `type="text"` with `type="datetime-local"` and convert to UTC ISO-8601 in the view before forwarding to the API.
5. **Compact card layout by default.** Reduce the three summary chips (Metadata / Provenance / Cataloged) into a single two-line summary row per asset. Keep the long-form chips on the asset detail page where they belong.
6. **Action cluster simplified.** Primary action "Inspect" is a button. Favorite / Archive become icon-style toggles (star / archive-box) with `aria-label`, not full-width buttons.
7. **Consolidated `return_query` hidden input.** Instead of repeating ~10 hidden filter fields in each form, emit one `<input type="hidden" name="return_query" value="{{ request.query_string }}">` and have the action handlers redirect back to `/catalog?<return_query>`.

### Phase 2 — New functionality (small API additions)

**Requires adding parameters to `/v1/admin/catalog`. Coordinated change across `photovault-api` and `photovault-server-ui`.**

1. **Text search `q`.** Matches on filename / camera make / camera model / provenance_original_filename. Case-insensitive, prefix or substring.
2. **Sort dropdown `sort_by`.** Options: `capture_date_desc`, `capture_date_asc`, `cataloged_desc`, `size_desc`, `filename_asc`. Default remains current order.
3. **Grid view toggle.** URL param `view=grid|list`. Grid shows just thumbnail + filename + star, sized ~160px.
4. **Keyboard shortcuts.** `/` focuses the search box; `g` / `l` toggle grid / list view. Purely client-side, ~20 lines of vanilla JS.
5. **Page-of-N pager.** Show `Page 3 of 47` and a jump-to-page input in the pager macro.

### Phase 3 — Bulk selection (revisit after Phase 2 lands)

**Deferred. Including here for roadmap visibility, not for immediate approval.**

1. Row-level checkbox; sticky "N selected" toolbar at the bottom.
2. Bulk actions: Favorite / Unfavorite / Archive / Unarchive / Backfill preview / Backfill extraction.
3. Requires new bulk API endpoints; real design doc needed before starting.

---

## 4. Phase 1 — concrete mockup (the diffs I'd write)

I am showing the shape of the changes, not asking for approval of exact markup. These are illustrative.

### 4.1 Active filters chip bar (new block near top of `catalog.html`)

```jinja
{% set active_filters = [] %}
{% if extraction_status_filter %}{% set _ = active_filters.append(("extraction_status", extraction_status_filter, "Extraction")) %}{% endif %}
{% if preview_status_filter %}{% set _ = active_filters.append(("preview_status", preview_status_filter, "Preview")) %}{% endif %}
{% if media_type_filter %}{% set _ = active_filters.append(("media_type", media_type_filter, "Media")) %}{% endif %}
{# ... and so on for each filter ... #}

{% if active_filters %}
<div class="active-filters d-flex flex-wrap gap-2 mb-3">
  {% for key, value, label in active_filters %}
    <a class="badge rounded-pill bg-light text-dark border"
       href="{{ url_for('catalog', **(catalog_query_state | rejectattr('0', 'eq', key) | list | items2dict)) }}">
      {{ label }}: {{ value }} <span aria-hidden="true">×</span>
      <span class="visually-hidden">remove filter</span>
    </a>
  {% endfor %}
  <a class="small" href="/catalog">Clear all</a>
</div>
{% endif %}
```

### 4.2 Inline favorite toggle via HTMX

Replace the 14-line favorite form with:

```jinja
<button
  class="btn btn-sm btn-icon {% if asset.is_favorite %}btn-warning{% else %}btn-outline-secondary{% endif %}"
  hx-post="/catalog/actions/favorite/toggle"
  hx-vals='{"relative_path": "{{ asset.relative_path }}"}'
  hx-target="closest .asset-card"
  hx-swap="outerHTML"
  aria-label="{% if asset.is_favorite %}Unfavorite{% else %}Favorite{% endif %}"
>★</button>
```

And a new handler in `app.py`:

```python
@app.post("/catalog/actions/favorite/toggle")
def catalog_favorite_toggle_action():
    relative_path = request.form.get("relative_path", "").strip()
    if not relative_path:
        abort(400)
    # decide mark vs unmark by calling /v1/admin/catalog/asset first, or by
    # sending the client's current state as a hidden input
    current = fetcher("/v1/admin/catalog/asset", {"relative_path": relative_path})
    path = "/v1/admin/catalog/favorite/unmark" if current.get("is_favorite") else "/v1/admin/catalog/favorite/mark"
    poster(path, {"relative_path": relative_path})
    asset = fetcher("/v1/admin/catalog/asset", {"relative_path": relative_path})
    # re-render just this card
    return render_template("_asset_card.html", asset=_decorate(asset), page=...)
```

This implies extracting the current `<article class="card asset-card">` block into a `_asset_card.html` partial so HTMX can swap a single card. That partial extraction is a small prerequisite refactor.

### 4.3 Native datetime-local + UTC conversion

In `catalog.html`:

```html
<input id="cataloged_since_utc" type="datetime-local" class="form-control"
       name="cataloged_since_utc_local" value="{{ cataloged_since_local }}">
```

In `app.py`:

```python
from datetime import datetime, timezone
raw = request.args.get("cataloged_since_utc_local", "").strip()
if raw:
    cataloged_since_filter = (
        datetime.fromisoformat(raw).replace(tzinfo=timezone.utc).isoformat()
    )
    cataloged_since_local = raw
else:
    cataloged_since_filter = ""
    cataloged_since_local = ""
```

(Operator types in their local clock; we store and filter in UTC. No API contract change.)

### 4.4 Primary / Advanced filter split

Wrap the current six rare fields in `<details class="mt-3"><summary>Advanced filters</summary>...</details>`. The four primary fields (Extraction, Preview, Media type, Favorite) stay visible by default.

### 4.5 Consolidated return_query hidden input

Add a helper in `app.py`:

```python
def _return_query() -> str:
    return request.query_string.decode("utf-8")
```

In the template:

```jinja
<input type="hidden" name="return_query" value="{{ request.query_string.decode('utf-8') }}">
```

In the action handlers, replace the big `_catalog_query_state_from_form()` block with:

```python
return_query = request.form.get("return_query", "").strip()
target = "/catalog?" + return_query if return_query else "/catalog"
return redirect(target)
```

This is purely internal cleanup — no behavior change visible to the operator — but it removes ~80 lines of repeated hidden-field markup from the template and makes future filter additions a one-line change.

---

## 5. Non-goals for this first pass

- **No schema changes.** v1 constraint: SHA256 is the only dedup truth; not touching it.
- **No new client states, no changes to offline/online guarantees.** The catalog browse page is server-UI only; client state machine is untouched.
- **No SPA.** Phase 1 uses HTMX for two specific toggles only. The page remains SSR-first with Jinja templates and full-page navigation for filtering / paging.
- **No bulk actions in Phase 1.** Parked for Phase 3.
- **No changes to `/catalog/asset` detail page in this proposal.** That's a separate doc when you're ready.
- **No API changes in Phase 1.** Phase 2 gets its own go/no-go review.

---

## 6. Open questions (please answer before I start)

1. **HTMX is listed as current stack but minimally used — are you OK with introducing it for real here?** The alternative is a tiny bit of `fetch()`-based vanilla JS. I'd recommend HTMX since `docs/photovault_tech_stack_current.md` already names it.
2. **Is extracting `_asset_card.html` into a partial acceptable?** It's the cleanest way to do inline swaps. Very small refactor, no behavior change.
3. **For the "Advanced filters" split, is my grouping right?** Proposed primary: Extraction, Preview, Media type, Favorite. Advanced: Origin, Preview capability, Archived, date ranges. Happy to swap.
4. **Default layout going forward: list or grid?** I'd default to keeping the current list (low risk, matches operator mental model from M3 diagnostics), and make grid a toggle that arrives in Phase 2. OK?
5. **Scope of this PR:** do you want Phase 1 as a single PR, or should I split it further (e.g., just the HTMX toggle first, then the filter bar, then datetime)?

---

## 7. If you approve, the PR plan

**Phase 1 PR, in order:**

1. Extract asset card to `_asset_card.html` partial. Zero behavior change. Tests: existing template renders unchanged.
2. Add `/catalog/actions/favorite/toggle` and `/catalog/actions/archive/toggle` endpoints returning the rendered partial. HTMX-swap the card on click.
3. Replace the two big filter forms' per-row hidden fields with a single `return_query` hidden input. Update the action handlers.
4. Split filter form into primary + `<details>` advanced group.
5. Convert datetime inputs to `type="datetime-local"` with UTC conversion in the view.
6. Add active-filters chip bar.
7. Compact asset card: collapse three summary chips into two lines; icon-style favorite / archive.
8. Update `codex_log.md` with a single entry summarizing the change and the verification commands run.

**Verification per AGENTS.md:** existing pytest suite + manual smoke on `/catalog` with the full filter matrix. If there is a Playwright/HTMX integration test harness, I'll add coverage for the toggle endpoints.

**Rough size estimate:** ~200 lines of template change, ~120 lines of `app.py` change, ~30 lines of CSS. No new deps beyond HTMX (already in the stack).

---

## 9. Phase 3 — Grid-view review workflow (planned, not yet implemented)

Phase 2 (grid view at `/library` with folder tree, thumbnail grid, HTMX popover, Bootstrap modal lightbox) shipped alongside Phase 1. Phase 3 turns that surface from a browse tool into a **triage tool** and closes the loop with the client so rejected content stays rejected.

**Status**: approved 2026-04-22. Defaults locked; implementation split into three independently-commit-able sub-phases.

### Locked defaults

| Decision | Choice |
| --- | --- |
| Reject-queue scope | Global server-wide (v1 is single-operator) |
| Delete strategy | Move into `trash/YYYY-MM-DD/` first, hard-unlink after N days via periodic cleanup |
| Tombstone permanence | Strict — stays active forever unless an admin explicitly clears it |
| `X` keyboard shortcut | Lightbox-only (review flow), `Shift+X` unrejects |
| EXIF fields to add | `shutter`, `f-number`, `ISO`, `focal length`, `35mm focal length` |

### Phase 3.A — Keyboard nav + EXIF in lightbox

Pure UX polish + additive extraction-column expansion. No new endpoints.

**Keyboard nav**

- Grid renders an ordered JSON slice of the current page's `relative_path`s into `data-library-slice` on `#libraryLightbox`.
- Prev/next buttons in `_library_lightbox.html` issue a fresh HTMX `GET /library/lightbox?relative_path=<neighbor>&index=<new>&folder=<...>&total=<...>`.
- A small JS handler (inline in `library.html`) listens for `keydown` while the modal has `.show`, maps `ArrowLeft`/`ArrowRight` → prev/next button clicks, ignores keys when `<input>`/`<textarea>` is focused. `Escape` remains handled by Bootstrap.

**EXIF expansion**

- Schema: additive ALTER on `api_media_asset_extractions`:
  - `exposure_time_s REAL NULL`
  - `f_number REAL NULL`
  - `iso_speed INTEGER NULL`
  - `focal_length_mm REAL NULL`
  - `focal_length_35mm_mm REAL NULL`
- Extraction reads Pillow EXIF tags `33434` (`ExposureTime`), `33437` (`FNumber`), `34855` (`ISOSpeedRatings`), `37386` (`FocalLength`), `41989` (`FocalLengthIn35mmFilm`). `ShutterSpeedValue` (37377) is an APEX value; prefer `ExposureTime` when both are present.
- `MediaAssetRecord`, `AdminCatalogItem`, both store implementations, and `_to_admin_catalog_item` surface the new fields.
- Server-UI helper `_format_exposure_summary(item) -> str` renders the compact form `ƒ/2.8 · 1/200s · ISO 400 · 50mm (75mm eq.)`; empty-field elision keeps it graceful.
- Existing extraction-backfill job fills in old rows.

### Phase 3.B — Mark-for-delete queue

Adds the data model and the UI affordances without wiring a destructive action.

**Schema**

```sql
CREATE TABLE api_catalog_reject_queue (
  relative_path   TEXT PRIMARY KEY,
  sha256_hex      TEXT NOT NULL,
  marked_at_utc   TEXT NOT NULL,
  marked_reason   TEXT NULL
);
```

Additive-only migration. SHA duplicated from `api_media_assets` so execute-phase still has it after the source row is deleted.

**Store API (both InMemory and Postgres)**

- `add_catalog_reject(relative_path, *, marked_at_utc) -> RejectedAssetRecord` — idempotent upsert.
- `remove_catalog_reject(relative_path) -> bool`
- `list_catalog_rejects(*, limit, offset) -> tuple[int, list[RejectedAssetRecord]]`
- `count_catalog_rejects() -> int`
- `is_catalog_reject(relative_path) -> bool`

**HTTP endpoints**

- `POST /v1/admin/catalog/reject` — body `{relative_path}`. 400 on path-traversal. Idempotent.
- `DELETE /v1/admin/catalog/reject/{relative_path}` — idempotent unmark.
- `GET /v1/admin/catalog/rejects?limit=&offset=` — paginated list, returns full `AdminCatalogItem` per row plus `marked_at_utc`.

**UI**

- Tile: add an "×" button next to the existing "…" overflow, visible on hover/focus. Rejected tiles get a red outline (`.library-tile--rejected`) that's visible at rest.
- Lightbox: "Reject" / "Unreject" toggle button. `X` keyboard shortcut triggers the same action; on reject, auto-advance to the next tile (reviewer keeps their hands on the keyboard).
- Grid header: persistent badge `{N} marked for deletion` linking to `/library/rejects`.
- New `/library/rejects` page: scoped thumbnail grid + per-row Unreject + the destructive **"Delete rejected media"** button (disabled when queue is empty; wired in 3.C).

**Tests**

- Store: round-trip add/list/remove/count; unknown path unmark is a no-op.
- API: add/list/remove including idempotency and path-traversal rejection.
- Server-UI: mark-via-HTMX swaps the card into the rejected state; header badge counts correctly; `/library/rejects` renders the queue with an empty state when the queue is empty.

### Phase 3.C — Execute delete, SHA tombstone, handshake contract

The destructive, irreversible commit. Kept separate so review attention concentrates here.

**Schema**

```sql
CREATE TABLE api_catalog_tombstones (
  sha256_hex               TEXT PRIMARY KEY,
  last_relative_path       TEXT NOT NULL,
  deleted_at_utc           TEXT NOT NULL,
  origin_kind_at_deletion  TEXT NOT NULL,
  file_size_bytes          INTEGER NOT NULL
);
```

Additive-only. Audit fields included for future forensics.

**Store API**

- `record_tombstone(*, sha256_hex, last_relative_path, deleted_at_utc, origin_kind_at_deletion, file_size_bytes) -> None`
- `is_tombstoned(sha256_hex) -> bool`
- `list_tombstones(*, limit, offset) -> tuple[int, list[TombstoneRecord]]`
- `clear_tombstone(sha256_hex) -> bool`

**Delete strategy**

Soft-delete with retention:

1. Move the file into `<storage_root>/trash/<YYYY-MM-DD>/<sha_prefix>/<relative_path>`.
2. Record tombstone.
3. Delete the `api_media_assets` row (FK `ON DELETE CASCADE` removes extraction/preview rows — verify the cascade before rollout; add one if missing).
4. Remove the reject-queue row.

Retention: a periodic cleanup (either admin endpoint `POST /v1/admin/catalog/trash/purge?older_than_days=N`, or a small script invoked from cron) hard-unlinks trash folders older than N days. Default retention: **14 days**.

**HTTP endpoints**

- `POST /v1/admin/catalog/rejects/execute` — body `{confirm: true, dry_run?: false}`. Anything else is 400. Returns `{executed: N, failed: [{relative_path, reason}], still_pending: M, trash_directory: "..."}`. `dry_run=true` returns the same shape without touching the filesystem or DB.
- `GET /v1/admin/catalog/tombstones?limit=&offset=` — paginated list.
- `DELETE /v1/admin/catalog/tombstones/{sha256_hex}` — admin override.
- `POST /v1/admin/catalog/trash/purge?older_than_days=14` — idempotent, returns byte/file counts.

**Client handshake contract change**

Adds a third decision to `/v1/upload/metadata-handshake` per-file: `PREVIOUSLY_DELETED`.

- API: when the incoming SHA is in `api_catalog_tombstones`, return `PREVIOUSLY_DELETED` instead of `NEEDS_UPLOAD` / `ALREADY_EXISTS`.
- `photovault-clientd`: persist `remote_status = 'previously_deleted'` on the local upload plan row and suppress further upload attempts for that content. Subsequent SD-card rescans mark the same local file satisfied.
- Older clients default to treating unknown decisions as "no upload needed" (safer fallback), preserving a graceful-degradation path.

**UI**

- `/library/rejects` "Delete rejected media" button opens a confirmation modal stating the count and total size, requires typing the count to enable the final destructive button, and supports `dry_run` preview before final execute.
- Post-execute: inline success/failure report before redirect.
- New `/library/tombstones` admin page: paginated list of tombstoned SHAs with per-row "Clear tombstone" action.

**Tests**

- Store: tombstone round-trip, cascade deletes, clear.
- API: execute happy path, partial failure (one file missing on disk), dry_run mode, tombstone list/clear, handshake returns `PREVIOUSLY_DELETED` for a tombstoned sha, handshake returns `NEEDS_UPLOAD` after the tombstone is cleared.
- clientd: handshake `PREVIOUSLY_DELETED` persisted and honored; unknown decision treated as safe default.
- Server-UI: confirm-typed gating on the execute form; dry_run preview; tombstone admin page renders + clears.

### Cross-cutting

- **Auth**: reuses existing `/v1/admin/*` boundary. No change.
- **Audit**: structured log lines per reject/unreject/execute/clear-tombstone. No dedicated audit table in this round; logs are enough for v1.
- **Docs**: 3.C requires a Codex-contract note about the new handshake decision (update `docs/codex_contract_v1.md` if present, plus `docs/photovault_v1_roadmap.md` checklist). 3.A and 3.B touch only the `codex_log.md` entries.
- **Rollout**: 3.A and 3.B are safe for the Pi. 3.C deploys only after manual triage on a staging reject queue and at least one `dry_run=true` execute pass.
- **Retention cleanup job**: add to the same cron/systemd-timer surface used by other periodic jobs. If none exists today, invoke manually from an admin endpoint and document it in the install guide.

### Rough sizes

- 3.A: ~300 LOC (schema + 2 stores + Pydantic + extraction + lightbox template + JS handler + tests).
- 3.B: ~500 LOC (new table + 2 stores + 3 endpoints + `/library/rejects` page + tile/lightbox UI + tests).
- 3.C: ~600 LOC (new table + store + 4 endpoints + cascade verification + client handshake + clientd tests + 2 admin pages + confirm-typed modal + tests).
