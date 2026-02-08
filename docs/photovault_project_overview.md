# photovault – Project Overview

## Purpose
photovault is a distributed, offline-first photo ingestion and backup system designed primarily for camera workflows (SD cards), with strong guarantees around integrity, deduplication, and retry-safe delivery.

The system consists of multiple autonomous **clients** (e.g. Raspberry Pi devices) and a central **server**. Clients ingest photos locally, calculate cryptographic hashes, deduplicate files, and upload them to the server when connectivity is available. The server validates integrity, performs global deduplication, and provides a global overview UI.

---

## Core Design Principles
- **Offline-first** – ingest and hashing work without Internet
- **Idempotent** – safe to retry, reboot, reinsert media
- **Hash-based integrity** – SHA256 is the source of truth
- **Client autonomy** – no central coordination required
- **Multi-client capable** – many clients uploading concurrently
- **Separation of concerns** – ingest, verify, upload are distinct stages

---

## High-Level Architecture

## Components & Naming

- **photovault-api**  
  Server-side API (FastAPI). Handles uploads, global deduplication, verification, and job tracking.

- **photovault-server-ui**  
  Server-side web UI (Flask). Used for monitoring, administration, and inspection.

- **photovault-clientd**  
  Client daemon. Owns ingestion, hashing, local deduplication, persistence, and upload retries.

- **photovault-client-ui**  
  Client-side web UI (Flask). A required component used for networking configuration,
  operational control, and status visibility. Communicates exclusively with
  `photovault-clientd` via local HTTP.

### Clients
- Raspberry Pi–based devices
- Ingest files from removable media (SD cards via USB reader)
- Copy files to local staging storage (SSD recommended)
- Calculate SHA256 hashes
- Perform client-side deduplication (session + historical)
- Upload files when Internet is available
- Expose a local control UI
- Persist all state locally (SQLite)

### Server
- Centralized service receiving uploads
- Maintains a global SHA registry
- Rejects duplicate uploads early
- Verifies file integrity after upload
- Tracks job and file states across all clients
- Exposes a global admin / overview UI

---

## Deduplication Model

### Client-side
- **Session deduplication**: duplicates detected within a single SD card ingest
- **Historical deduplication**: duplicates detected against the client’s local SHA registry

### Server-side
- **Global deduplication** across all clients based on SHA256

---

## Integrity Verification

### Client
- On-demand or scheduled SHA recalculation
- Detects local storage corruption
- Can trigger re-ingest or re-upload

### Server
- On-demand or periodic verification (scrubbing)
- Detects server-side storage corruption
- Can request retransfer from clients that still cache the file

---

## Client Runtime Model (Summary)
- Networking handled by **NetworkManager** (AP + STA, captive portal)
- Single long-running daemon: `photovault-clientd`
- Required separate Flask-based UI for control and networking
- systemd-managed services

---

## UI Model

### Client UI
- Server-side rendered (Flask + Jinja2)
- Used for:
  - Wi-Fi configuration
  - Ingest / upload status
  - Manual actions (retry, verify)

### Server UI
- Server-side rendered (Flask + Jinja2)
- Used for:
  - Global job overview
  - Client status
  - Error and verification monitoring

---

## Future-Proofing
- Long-term offline clients
- Resumable / chunked uploads
- Alternative transport layers
- Object storage backends
- Camera tethering (no SD card)

---

## Canonical Naming

This file is the canonical overview document for the project:

- `photovault_project_overview.md`

Other documents should reference this filename explicitly.

---

## Project Name
**photovault**

Components:
- photovault-clientd
- photovault-client-ui
- photovault-api
- photovault-server-ui

---

This document serves as the architectural foundation for all other design and implementation documents in the photovault project.
