# photovault – Client Runtime & Networking Model

## Scope
This document summarizes how the **photovault client** runs on a device (e.g. Raspberry Pi), how responsibilities are split between services, and how networking and ingest/upload workflows interact.

---

## High-level Principle

The client must function as a **self-contained appliance**:
- ingest and hashing must work without Internet
- uploads are retried automatically when connectivity appears (v1 retries restart the upload from the beginning)
- client UI is required for operation (configuration, control, and status visibility)

To achieve this, responsibilities are split between **system services** and **photovault-owned logic**.

---

## Networking Model

### NetworkManager as the networking daemon
- **NetworkManager** is responsible for all networking
- Handles:
  - Wi-Fi client connections (home / hotel)
  - AP + STA mode for local UI access
  - Captive portal workflows

photovault **does not implement networking itself**.
Instead, the client UI interacts with NetworkManager via:
- `nmcli`
- or DBus (later, if needed)

This avoids reinventing Wi-Fi logic and keeps the system robust.

---

## photovault Client Service (`photovault-clientd`)

### Role
A single long-running **systemd service** that owns all client-side business logic:

- SD card detection (via udev trigger or polling)
- Ingesting files to local staging storage
- SHA256 calculation
- Client-side deduplication (session + historical)
- Managing upload queue
- Uploading files when Internet is available
- Retrying safely on failure
- Running on-demand or scheduled verification jobs

### Key Properties
- Runs independently of the UI
- Persists all state in SQLite
- Safe across reboots and power loss
- Automatically resumes queued work

This daemon is the **source of truth** for client state.

---

## Client UI (`photovault-client-ui`)

### Role
A **Flask-based server-side rendered UI** responsible for:

- Device access via local AP
- Wi-Fi configuration (scan, connect, captive portal support)
- Displaying ingest / upload / verify status
- Triggering actions (retry, verify, upload now)

### Characteristics
- Uses Jinja2 templates + Bootstrap
- Uses HTMX for dynamic updates (polling job state)
- Minimal JavaScript (modals, confirmations)

### Interaction Model
- UI **does not implement business logic**
- UI communicates with `photovault-clientd` exclusively via a local HTTP API
- UI does not access the client database directly
- The SQLite database is a private implementation detail of the client daemon
---

## Service Layout (Recommended)

### Required system services
- `NetworkManager.service`
- `photovault-clientd.service`
- `photovault-client-ui.service`

In a minimal setup, the UI may be embedded in `photovault-clientd`, but separation is recommended for clarity and resilience.

---

## Why Not Multiple photovault Daemons?

- Networking is already handled by NetworkManager
- Splitting ingest/upload/verify into separate daemons adds complexity without clear benefit
- A **single state-machine-driven daemon** is easier to reason about and test

---

## Summary

- Networking: **NetworkManager** (system daemon)
- photovault logic: **one client daemon** (`photovault-clientd`)
- UI: **Flask SSR app**, required component (may run as separate service)
- No Celery or external brokers required (v1)
- systemd ensures appliance-like behavior

This model keeps the client simple, reliable, and easy to evolve.

