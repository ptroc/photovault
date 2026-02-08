# photovault

**photovault** is an offline-first, self-hosted photo ingestion and backup system designed primarily for photographers and camera-based workflows.

It focuses on:
- safe ingestion from SD cards
- strong integrity guarantees (SHA256-based)
- deduplication (client-side and server-side)
- non-resumable, retry-safe uploads (v1)
- long-term reliability over convenience

photovault is designed to run on small, autonomous devices (e.g. Raspberry Pi clients) that upload to a central server when connectivity is available.

---

## Project Status

🚧 **Early-stage / active design & development**

The architecture, state machines, and technical foundations are being actively defined.
Expect changes until a first stable release is announced.

---

## Key Characteristics

- **Offline-first**: ingest and hashing work without Internet
- **Client autonomy**: clients can operate independently for long periods
- **Hash-based integrity**: SHA256 is the source of truth
- **Deduplication**: avoids storing or transferring duplicate files
- **Self-hosted**: no external cloud dependency
- **Operationally boring**: Linux, Python, systemd, PostgreSQL/SQLite

---

## Documentation

Canonical project documents (see `docs/`):

- `photovault_project_overview.md`  
  High-level architecture and design principles

- `photovault_tech_stack_current.md`  
  Agreed technical stack and constraints

- `photovault_client_runtime_networking_model.md`  
  Client runtime model and networking responsibilities

- `photovault_client_state_machine.md`  
  Client state machine and transitions

These documents are treated as sources of truth.

---

## Repository Skeleton

The repository now includes a minimal implementation skeleton for all core components:

- `services/photovault-clientd` (client daemon, local state + control API)
- `services/photovault-client-ui` (client control-plane SSR UI)
- `services/photovault-api` (server API)
- `services/photovault-server-ui` (server SSR monitoring UI)
- `deploy/systemd` (service unit skeletons)
- `ansible/playbooks` (bootstrap playbook skeleton)
- `scripts/dev` (local developer helper scripts)

This is intentionally a scaffold: explicit entrypoints, placeholder endpoints, and basic tests.
Business logic for ingest, hashing, deduplication, and upload state transitions should be implemented
incrementally against the v1 state machine and roadmap docs.

---

## Dev Workflow (Root Makefile)

Use root-level commands for consistent local setup and checks:

- `make venv`
- `make install` (uses `requirements.txt`)
- `make install-dev`
- `make lint`
- `make test`
- `make check`

---

## Intended Use

photovault is intended for:
- individual photographers
- studios
- small teams
- companies managing their own photo workflows

It is **not intended** to be used as the basis for a hosted or subscription photo backup service.

---

## License

photovault is licensed under the **photovault Community License**.

### What this means
- Free for personal and non-commercial use
- Free for internal commercial use (e.g. photographers, studios, companies)
- You may modify the software for your own internal workflows

### What is not allowed
- Offering photovault as a hosted, managed, or subscription service
- Reselling access to photovault or its functionality
- Using photovault as the basis of a SaaS or managed backup product

photovault is **source-available**, not open source.  
The software is provided **as-is**, without warranty or liability.

See the `LICENSE` file for full terms.

---

## Non-goals (v1)

- Cloud/SaaS offering
- Turnkey consumer UI
- Vendor lock-in
- Always-on Internet assumptions

---

## Contributing

At this stage, the project is design-driven.
Contributions, discussions, and feedback are welcome, but major changes should be aligned with the existing documentation and goals.
