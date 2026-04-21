# photovault v1 Roadmap

## Purpose

This document defines the **photovault v1 execution roadmap**.

It is used to:
- guide implementation sequencing
- align PM / TPM / EM decisions
- constrain scope creep
- steer Codex and other agents toward correct priorities

This roadmap is **authoritative for v1 scope**.  
Anything not listed here is either **explicitly deferred** or **out of scope**.

For the next planned server-focused follow-on release, see:
- `docs/photovault_v1_5_roadmap.md`

---

## v1 Product Definition

**photovault v1 delivers:**

> A reliable, offline-first photo ingestion system that safely ingests photos from removable media, deduplicates them using SHA256, and eventually uploads them to a central server when connectivity is available — with full observability and reboot safety.

### Core guarantees
- Offline ingest and hashing always work
- SHA256 is the single source of truth
- All operations are idempotent and retry-safe
- Reboots and power loss do not corrupt state
- Multiple autonomous clients are supported
- Uploads are correct, not necessarily fast

---

## Milestone Overview

| Milestone | Name | Goal |
|---------|-----|-----|
| M0 | Foundation | Trustworthy state, recovery, lifecycle |
| M1 | Offline Ingest | SD → staging → hash → local dedup |
| M2 | Network Upload | Eventual upload with retries |
| M3 | Observability | Operator trust without SSH |
| M4 | Server File Storage | Filesystem-backed storage layout and indexability |
| M5 | Appliance Networking | Reliable local access and captive-portal connectivity |

Milestones are **sequential**.  
Later milestones must not weaken guarantees from earlier ones.

---

## Milestone M0 – Foundation (Blocking)

### Goal
Establish a **correct, reboot-safe, inspectable foundation**.

Nothing else matters if state cannot be trusted.

### Epics
- systemd-managed client daemon lifecycle
- SQLite schema (jobs, files, states)
- Bootstrap recovery logic
- Deterministic state transitions
- Logging and error classification

### Must-have outcomes
- Client survives reboot at any point
- No in-memory-only state
- All non-terminal files resume correctly
- Terminal states are never reprocessed

### Explicit non-goals
- Performance optimization
- Parallelism
- Upload logic

---

## Milestone M1 – Offline Ingest & Local Dedup

### Goal
Offline ingest is **boring, reliable, and complete**.

SD cards can be ingested with **zero network**.

### Epics
- SD detection and job creation
- File enumeration and snapshotting
- Staging copy with fsync
- SHA256 calculation
- Session deduplication
- Historical deduplication (local registry)
- Job-level visibility in client UI

### Must-have outcomes
- Removing SD mid-copy is safe
- Partial files are never marked valid
- Duplicate files are detected deterministically
- Jobs complete offline

### Explicit non-goals
- Pre-hash fingerprinting
- Server communication
- Upload preparation

---

## Milestone M2 – Network-Gated Upload Loop

### Goal
Uploads **eventually succeed or fail clearly**, never corrupt data, and never create duplicates.

### Epics
- WAIT_NETWORK gating
- Metadata handshake (SHA-based dedup)
- Non-resumable file upload
- Server-side verification
- Retry + backoff logic
- Terminal file classification

### Explicit non-goals
- Resumable / chunked uploads
- Parallel upload workers
- Transport optimization

---

## Milestone M3 – Observability & Operator Trust

### Goal
The system is **operable without SSH**.

### Epics
- Client UI: job status, file counters, last errors
- Server UI: per-client status, stuck jobs, dedup stats

### Explicit non-goals
- UI polish
- Mobile-first design
- SPA frameworks

---

## Milestone M4 – Server File Storage & Indexing

### Goal
Store uploaded files directly in a deterministic filesystem layout on the server and make that storage
auditable by indexing files that already exist in those folders.

### Epics
- Filesystem-backed server storage instead of DB-backed binary storage
- Deterministic folder layout using `./year/month/Name_of_the_job`
- Server-side file write/finalize flow targeting the filesystem layout
- Server endpoint to index files that already exist in the storage tree
- Reconciliation between indexed filesystem files and the server SHA registry

### Must-have outcomes
- Uploaded files are persisted as normal files on disk, not inside the database
- Files are organized under `./year/month/Name_of_the_job`
- Server verification and dedup still use SHA256 as the source of truth
- Existing files already present in those folders can be indexed through a dedicated server endpoint
- Indexed files populate or reconcile with the server-side SHA registry without re-uploading content

### Explicit non-goals
- Object storage backends
- Content-addressed storage redesign beyond the required folder layout
- Full media-library management features
- Background distributed indexing systems

---

## Milestone M5 – Appliance Networking & Connectivity

### Goal
The client behaves like a self-contained appliance that is always reachable locally and can be moved onto
unfamiliar Wi-Fi environments, including captive-portal hotel networks, without SSH.

### Epics
- Default client Wi-Fi access point (AP) on first boot / no-network state
- UI controls for AP SSID and password customization
- AP + STA operating model using NetworkManager
- Captive-portal Wi-Fi join flow for hotel and similar networks
- Clear UI guidance for local access vs upstream Internet connectivity

### Must-have outcomes
- A default photovault Wi-Fi AP is available when the device is not yet joined to another usable network
- Operator can customize AP SSID and password from the client UI
- Client remains locally reachable while configuring upstream Wi-Fi
- Hotel / captive-portal Wi-Fi can be joined through an operator-friendly UI flow
- Network state and next required operator action are explainable from the UI

### Explicit non-goals
- Custom networking stack outside NetworkManager
- Enterprise Wi-Fi policy management
- Advanced mesh / multi-radio orchestration
- Cloud-managed provisioning

---

## Explicitly Deferred (Not v1)

- Resumable / chunked uploads
- Multiple concurrent upload workers
- Fingerprint-based pre-hash dedup
- Object storage backends
- Server-driven orchestration
- SPA / React / mobile UI
- Distributed task queues

---

## Acceptance Criteria

photovault v1 is shippable only if:
- Ingest survives reboot
- Offline ingest works indefinitely
- Duplicates are never uploaded twice
- Corruption is detected
- Upload retries are deterministic
- System state is explainable via UI
- Server-side files are stored in the deterministic filesystem layout and can be re-indexed
- Local device access remains available through the appliance networking model
- Captive-portal / hotel Wi-Fi setup is possible without SSH

---

## Guidance for Codex / Agents

- Treat the client state machine as executable spec
- Prefer explicit states
- Reject convenience-driven complexity
- Optimize for debuggability
- Surface trade-offs explicitly
