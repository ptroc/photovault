# photovault – Codex Agent Contract (v1)

## Purpose

This document defines **hard behavioral constraints** for Codex and other automated agents working on photovault v1.

Violating this contract is a **defect**, not a suggestion.

Scope: **photovault v1 only**

---

## DO (Mandatory)

### DO-1: Treat the client state machine as executable specification
- Every client-side behavior must map to an explicit state or transition
- No implicit background behavior
- No logic bypassing state transitions

If behavior does not fit the state machine → **STOP and escalate**

---

### DO-2: Preserve offline-first guarantees
- Ingest, hashing, and dedup must work without network
- Network usage must be explicitly gated (`WAIT_NETWORK`)
- Offline behavior must be deterministic and resumable

---

### DO-3: Persist all meaningful state
- SQLite is the source of truth
- No in-memory-only job, file, or retry state
- Retry counters, timestamps, and errors must be persisted

---

### DO-4: Prefer explicit over clever
- Explicit states over inferred behavior
- Explicit errors over silent retries
- Explicit operator actions over magic automation

---

### DO-5: Fail loudly and visibly
- Errors must surface in UI or logs
- Silent failure is forbidden
- “Best effort” must still be observable

---

### DO-6: Keep v1 boring
- Use the simplest mechanism that satisfies guarantees
- Prefer linear workflows
- Prefer single-threaded logic

---

## DON’T (Hard Prohibitions)

### DON’T-1: No resumable or chunked uploads
Retries restart from zero. v2+ only.

### DON’T-2: No new concurrency models
No workers, queues, fan-out, or parallel upload pipelines.

### DON’T-3: No networking outside NetworkManager
No custom Wi-Fi, sockets, or AP logic.

### DON’T-4: No alternative dedup mechanisms
SHA256 is the only dedup truth.

### DON’T-5: No SPA or JS-heavy UI
SSR (Flask + Jinja + HTMX) only.

### DON’T-6: No premature optimization
Correctness and debuggability first.

### DON’T-7: No silent scope expansion
New guarantees, states, or background behavior require escalation.

---

## Required Escalation Triggers

Codex must halt and ask before proceeding if:
- a new client state is needed
- behavior diverges offline vs online
- persistent schema changes are required
- a v2-only feature is implied

---

## Compliance Header (Required)

Every non-trivial Codex output must include:

```
Milestone: M0 | M1 | M2 | M3
Contract compliance: confirmed
Docs impacted: none | <list>
```

Missing header = non-compliant output.

---

**Status:** Authoritative for photovault v1
