# AGENTS.md

## Purpose
This file defines mandatory guidance for any coding agent operating in this repository.

## Canonical Docs (Read First)
1. `docs/photovault_v1_roadmap.md`
2. `docs/codex_contract_v1.md`
3. `docs/photovault_project_overview.md`
4. `docs/photovault_tech_stack_current.md`
5. `docs/photovault_client_state_machine.md`

If guidance conflicts, follow this precedence:
1. `docs/codex_contract_v1.md`
2. `docs/photovault_v1_roadmap.md`
3. State machine and architecture docs

## Non-Negotiable v1 Constraints
- Treat the client state machine as executable specification.
- Preserve offline-first behavior for ingest/hash/dedup.
- Persist all meaningful client state in SQLite (no in-memory-only truth).
- Use SHA256 as the only deduplication truth.
- v1 uploads are non-resumable (retry from zero).
- Keep implementation simple and explicit; no hidden behavior.

## Prohibited Changes (v1)
- No resumable/chunked uploads.
- No new concurrency models (workers, queues, fan-out pipelines).
- No networking stack bypassing NetworkManager.
- No SPA/JS-heavy UI patterns.
- No silent scope expansion.

## Required Escalation
Stop and ask for explicit scope confirmation if a request:
- Requires adding or changing client states.
- Changes guarantees between offline and online behavior.
- Implies schema changes with unclear migration impact.
- Introduces v2+ features or deferred roadmap items.

## Architectural Boundaries
- Client daemon (`photovault-clientd`) owns business logic and persistence.
- Client UI (`photovault-client-ui`) is control-plane only and talks to daemon via local HTTP.
- Server API (`photovault-api`) owns server-side verification and global dedup.
- Server UI (`photovault-server-ui`) is SSR and does not own business logic.

## Tech Stack Expectations
- Language: Python.
- OS: Linux (client/server).
- Client DB: SQLite.
- Server DB: PostgreSQL.
- Client/server APIs and UIs: FastAPI/Flask + Jinja2 + HTMX (SSR-first).
- Service management: systemd.
- Provisioning: Ansible.

## Implementation Style
- Prioritize correctness, observability, and idempotency over speed.
- Favor deterministic, explicit state transitions and visible failures.
- Keep v1 changes minimal, boring, and auditable.
- Prefer integration-style tests for state transitions, retries, dedup, and recovery behavior.

## Output Contract for Non-Trivial Agent Responses
Include this header in substantial design/implementation responses:

```
Milestone: M0 | M1 | M2 | M3
Contract compliance: confirmed
Docs impacted: none | <list>
```

## Local Environment
- Use a project-local virtual environment at `.venv/`.
- Preferred create command: `python3 -m venv .venv`
- Preferred activation command: `source .venv/bin/activate`
- Install Python dependencies into this local venv only.

## Raspberry Pi Deploy Preference
- When a task requires deploying to the Raspberry Pi at `10.100.1.95`, prefer using `scripts/deploy_rpi.sh`.
- In future Codex prompts that require Raspberry Pi validation, explicitly instruct Codex to use `scripts/deploy_rpi.sh` after local verification unless the task specifically requires a different deploy path.

## Codex Action Logging (Mandatory)
- Maintain an append-only log file at `codex_log.md` in the repository root.
- For every substantive Codex action, append one entry with:
  - UTC timestamp
  - brief action summary
  - files created/modified (if any)
  - verification commands run (if any)
- Do not rewrite or delete historical log entries unless explicitly requested by the user.
