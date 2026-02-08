# photovault – Tech Stack (Current)

## Scope
This document defines the **current, agreed technical stack** for photovault, incorporating all recent decisions. It is the authoritative reference for implementation choices unless explicitly revised.

---

## Operating Systems
- **Client OS:** Linux
- **Server OS:** Linux

---

## Programming Language
- **Python** (client and server)

Rationale:
- Strong standard library for files, hashing, HTTP
- Excellent ecosystem for web APIs, databases, testing
- Fits long-running daemons and admin-style UIs well

---

## Databases

### Server
- **PostgreSQL**
- Managed via **Alembic** migrations

Used for:
- Global SHA registry
- Job and file state tracking
- Client metadata and history
- Server-side verification state

### Client
- **SQLite**

Used for:
- Local state machine persistence
- Ingest session tracking
- Local SHA registry (historical deduplication)
- Upload queue and retry state

Testing strategy:
- Default: **in-memory SQLite** for fast integration tests
- Additional coverage: **file-backed SQLite** for WAL/locking/path edge cases

---

## Provisioning & Service Management

### Provisioning
- **Ansible** (local execution playbooks)

Responsibilities:
- OS package installation
- NetworkManager configuration
- photovault service deployment
- Configuration templating
- systemd unit installation and enablement

### Runtime Services
- **systemd**

Client services:
- `NetworkManager.service`
- `photovault-clientd.service`
- `photovault-client-ui.service`

Server services:
- `photovault-api.service`
- `photovault-server-ui.service`

---

## Server Architecture

### API
- **FastAPI**

Responsibilities:
- Client-facing JSON API
- Metadata handshake (SHA dedupe)
- File upload finalization
- Integrity verification
- Job and client state management

Characteristics:
- OpenAPI schema
- Typed request/response models
- Async-friendly for uploads

### Server UI
- **Flask** (separate app)
- **Jinja2** templates
- **Bootstrap** for styling
- **HTMX** for dynamic updates (polling job states, partial refreshes)
- Minimal **vanilla JavaScript** (modals, confirmations, small interactions)

Interaction model:
- UI communicates with API over HTTP
- UI does not own business logic

---

## Client Architecture

### Client Daemon
- **`photovault-clientd`** (Python, systemd service)

Responsibilities:
- SD card detection
- Ingest and local staging
- SHA256 calculation
- Client-side deduplication (session + historical)
- Upload queue management
- Upload execution
- Retry and backoff
- On-demand and scheduled verification

### Client UI
- **Flask** (only framework on client)
- **Jinja2** templates
- **Bootstrap** for layout
- **HTMX** for live status updates
- Minimal **vanilla JavaScript** for modals and confirmations

Additional responsibility:
- Networking configuration via **NetworkManager** (`nmcli` / DBus)
- Captive portal handling

The client UI is control-plane only; all business logic lives in the daemon.

### Client UI ↔ Daemon Interaction

- The client UI communicates with `photovault-clientd` exclusively via a local HTTP API.
- The UI does not access SQLite or any other persistence layer directly.
- All state mutation and validation logic resides in the daemon.

---

## Upload Strategy

### v1 Transport
- **HTTP-based uploads**
- v1 uploads are **non-resumable**; retries restart the upload from the beginning

Flow:
1. Client sends metadata (SHA, size)
2. Server responds with `ALREADY_EXISTS` or `UPLOAD_REQUIRED`
3. Client uploads file to temporary location
4. Server verifies SHA
5. Server finalizes or rejects

### Design Constraint
- Upload layer must be **modular**

Allows future replacement with:
- Chunked / resumable uploads
- tus-like protocol
- rsync-over-SSH (optional)

---

## Networking
- Managed exclusively by **NetworkManager**
- AP + STA mode for local access
- Captive portal workflows supported

photovault does not reimplement networking logic.

---

## Coding Standards

- PEP 8 naming:
  - `snake_case` for functions and variables
  - `PascalCase` for classes and models
  - `SCREAMING_SNAKE_CASE` for constants
- **Max line length:** 110 characters

---

## Testing

- **pytest**
- Prefer **integration tests** over mocks
- Real databases whenever feasible
- Target **≥ 80% coverage**

Focus areas:
- State transitions
- Retry and idempotency
- Deduplication correctness
- Corruption detection and recovery

---

## Explicit Non-Goals (v1)
- SPA frontend
- Distributed task queues (Celery, brokers)
- Custom networking stacks
- Always-on Internet assumptions

---

## Summary

photovault uses a conservative, well-understood stack:
- Linux + Python
- PostgreSQL (server), SQLite (client)
- FastAPI for APIs, Flask for UIs
- systemd + Ansible for operations

The focus is on reliability, debuggability, and long-term maintainability.

