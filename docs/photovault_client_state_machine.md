# Photovault – Client State Machine (v1)

This document defines the **client-side state machine** for photovault, aligned with:
- `photovault_project_overview.md`
- `photovault_tech_stack_current.md`

It is **offline-first**, **SQLite-persisted**, **reboot-safe**, and reflects **v1 (non-resumable) upload semantics**.

---

## Core principles

- Single long-running daemon: `photovault-clientd`
- Client UI is required and control-plane only (start ingest, show status, network setup)
- All job + file state persisted locally (SQLite)
- Ingest, copy, hashing, dedup work **without Internet**
- Network-dependent states explicitly gated by `WAIT_NETWORK`
- Upload v1 = **non-resumable**, retry restarts full upload
- SHA256 is the **single source of truth** for deduplication

---

## File terminal states

A file is terminal when its status is one of:
- `VERIFIED_REMOTE`
- `DUPLICATE_SHA_GLOBAL`
- `DUPLICATE_SHA_LOCAL`
- `DUPLICATE_SESSION_SHA`
- `ERROR_FILE`
- `QUARANTINED_LOCAL`

Only non-terminal files are resumed after reboot.

---

## Client state machine – transition table

| Current state | Trigger / event | Guard / condition | Actions (idempotent) | Next state | Failure / retry |
|---|---|---|---|---|---|
| **BOOTSTRAP** | daemon start / reboot | always | Open SQLite; run recovery scan; re-enqueue unfinished file states; select highest-priority resume phase from queued work | **IDLE** if no queued work, otherwise first queued phase (STAGING_COPY > HASHING > DEDUP_SESSION_SHA > DEDUP_LOCAL_SHA > QUEUE_UPLOAD > WAIT_NETWORK > SERVER_VERIFY > VERIFY_HASH) | DB fatal → **ERROR_DAEMON** |
| **IDLE** | SD inserted / ingest requested | SD mounted | Create Job; enumerate SD files; persist file records (DISCOVERED) | **DISCOVERING** | SD missing → **WAIT_MEDIA** |
| **WAIT_MEDIA** | media detected | SD mounted | Same as above | **DISCOVERING** | Remain with backoff |
| **DISCOVERING** | enumeration finished | files found | Persist snapshot; mark job DISCOVERED | **STAGING_COPY** | I/O error → retry → **ERROR_JOB** |
| **STAGING_COPY** | next file | status=DISCOVERED or NEEDS_RETRY_COPY | Copy file to staging; fsync; mark STAGED | **HASHING** | Copy fail → retry; SD removed → **WAIT_MEDIA** |
| **HASHING** | staged file ready | status=STAGED or NEEDS_RETRY_HASH | Compute SHA256; persist; mark HASHED | **DEDUP_SESSION_SHA** | Hash fail → retry / **ERROR_FILE** |
| **DEDUP_SESSION_SHA** | SHA available | same job | Deduplicate by SHA within current job; mark duplicates DUPLICATE_SESSION_SHA | **DEDUP_LOCAL_SHA** | DB error → **ERROR_JOB** |
| **DEDUP_LOCAL_SHA** | SHA available | history enabled | Check local SHA registry; mark DUPLICATE_SHA_LOCAL if known | **QUEUE_UPLOAD** or **JOB_COMPLETE_LOCAL** if no unique files remain | DB error → **ERROR_JOB** |
| **QUEUE_UPLOAD** | unique file | status=HASHED | Mark READY_TO_UPLOAD; enqueue and persist in local SHA registry | **WAIT_NETWORK**, **JOB_COMPLETE_LOCAL**, or **QUEUE_UPLOAD** | DB error → **ERROR_JOB** |
| **WAIT_NETWORK** | connectivity change / tick | online? | If offline do nothing; if online continue | **UPLOAD_PREPARE** | None |
| **UPLOAD_PREPARE** | upload cycle start | online | Ensure remote job exists; send metadata+SHA | **UPLOAD_FILE** or **SERVER_VERIFY** | Network fail → **WAIT_NETWORK** |
| **UPLOAD_FILE** | server requests upload | online | Upload full file to temp (non-resumable); mark UPLOADED | **SERVER_VERIFY** | Upload fail → retry → **WAIT_NETWORK** |
| **SERVER_VERIFY** | upload done / server dedup | online | Server verifies SHA+size; returns OK / EXISTS / FAIL | OK/EXISTS → **POST_UPLOAD_VERIFY**; FAIL → **REUPLOAD_OR_QUARANTINE** | Network fail → **WAIT_NETWORK** |
| **POST_UPLOAD_VERIFY** | policy enabled | optional | Optional local re-hash; compare to stored SHA | **CLEANUP_STAGING** | Mismatch → **QUARANTINED_LOCAL** |
| **REUPLOAD_OR_QUARANTINE** | verify failed | retries left? | Retry upload from scratch or quarantine | retry → **WAIT_NETWORK** | Exhausted → **ERROR_FILE** |
| **CLEANUP_STAGING** | file terminal | policy | Delete staged copy or retain | **UPLOAD_PREPARE** or **JOB_COMPLETE_REMOTE** | FS error → **PAUSED_STORAGE** |
| **JOB_COMPLETE_REMOTE** | all files terminal | always | Mark job DONE_REMOTE; emit event | **JOB_COMPLETE_LOCAL** | DB error → **ERROR_JOB** |
| **JOB_COMPLETE_LOCAL** | finalize | always | Cleanup, unmount SD, rotate logs | **IDLE** | Warn and continue |
| **VERIFY_IDLE** | scheduled verify | idle | Select files for integrity check | **VERIFY_HASH** | Backoff |
| **VERIFY_HASH** | verify task | file readable | Recompute SHA; compare | OK → **VERIFY_IDLE** | Mismatch → **QUARANTINED_LOCAL** |
| **PAUSED_STORAGE** | disk/fs unhealthy | storage bad | Pause ingest+upload; alert | resume → **IDLE** | Operator action |
| **ERROR_FILE** | permanent file error | fatal | Isolate file; continue job if policy allows | **UPLOAD_PREPARE** | Manual intervention |
| **ERROR_JOB** | job fatal | cannot continue | Mark FAILED; keep data | **IDLE** | Manual retry |
| **ERROR_DAEMON** | daemon fatal | unrecoverable | Stop processing | none | Operator action |

---

## Offline / online boundary

**Offline-safe states:**
DISCOVERING, STAGING_COPY, HASHING, DEDUP_SESSION_SHA, DEDUP_LOCAL_SHA, QUEUE_UPLOAD, VERIFY_*

**Online-required states:**
UPLOAD_PREPARE, UPLOAD_FILE, SERVER_VERIFY

All online-required states fall back to **WAIT_NETWORK** on connectivity loss.

---

## Reboot recovery rules

On BOOTSTRAP:
- DISCOVERED → STAGING_COPY
- NEEDS_RETRY_COPY → STAGING_COPY
- STAGED → HASHING
- HASHED → DEDUP_SESSION_SHA
- persisted job phase DEDUP_LOCAL_SHA → DEDUP_LOCAL_SHA
- persisted job phase QUEUE_UPLOAD → QUEUE_UPLOAD
- READY_TO_UPLOAD → WAIT_NETWORK
- UPLOADED → SERVER_VERIFY
- VERIFY_RUNNING → VERIFY_HASH

No terminal file is reprocessed.

After selecting the highest-priority resume phase, the daemon immediately runs the corresponding
single-threaded phase handler for implemented phases (currently STAGING_COPY, HASHING,
DEDUP_SESSION_SHA, DEDUP_LOCAL_SHA, QUEUE_UPLOAD, and JOB_COMPLETE_LOCAL) until a boundary state
is reached or a visible failure is recorded.

---

## Worklist Counters (Copy/Hash Phases)

For deterministic operator visibility and transition decisions:

- pending_copy = count(status in {DISCOVERED, NEEDS_RETRY_COPY})
- staged = count(status == STAGED)
- hash_pending = count(status in {STAGED, NEEDS_RETRY_HASH})

---

## Notes

- Fingerprint-based pre-hash dedup is intentionally **not part of v1** to keep SHA as the only truth.
- Resumable uploads (tus/rsync) are explicitly deferred to v2.

---

**Status:** aligned with current photovault docs (v1)
