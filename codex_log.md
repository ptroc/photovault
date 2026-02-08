# codex_log.md

Append-only log of substantive Codex actions in this repository.

## Entry format
- Timestamp (UTC):
- Summary:
- Files changed:
- Verification:

## Entries
- Timestamp (UTC): 2026-02-08T17:48:38Z
- Summary: Created local Python virtual environment at `.venv`.
- Files changed: `.venv/` (environment directory)
- Verification: `python3 -m venv .venv` completed successfully.

- Timestamp (UTC): 2026-02-08T17:48:38Z
- Summary: Added AGENTS guidance for local venv usage and mandatory Codex action logging.
- Files changed: `AGENTS.md`, `codex_log.md`
- Verification: none

- Timestamp (UTC): 2026-02-08T17:49:52Z
- Summary: Added root development workflow with unified setup/lint/test commands.
- Files changed: `Makefile`, `pyproject.toml`, `README.md`
- Verification: `make help`

- Timestamp (UTC): 2026-02-08T17:53:02Z
- Summary: Switched root dependency workflow to requirements files with pip install.
- Files changed: `requirements.txt`, `requirements-dev.txt`, `Makefile`, `README.md`
- Verification: `make help`

- Timestamp (UTC): 2026-02-08T18:37:43Z
- Summary: Ran unified checks, fixed Ruff import-order issues, and resolved pytest collection collisions by using unique test module names.
- Files changed: `services/photovault-clientd/src/photovault_clientd/app.py`, `services/photovault-clientd/src/photovault_clientd/db.py`, `services/photovault-clientd/src/photovault_clientd/main.py`, `services/photovault-clientd/tests/test_state_machine_contract.py`, `services/photovault-client-ui/src/photovault_client_ui/app.py`, `services/photovault-client-ui/src/photovault_client_ui/main.py`, `services/photovault-api/src/photovault_api/main.py`, `services/photovault-api/tests/test_api_app.py`, `services/photovault-client-ui/tests/test_client_ui_app.py`, `services/photovault-server-ui/src/photovault_server_ui/main.py`, `services/photovault-server-ui/tests/test_server_ui_app.py`, `codex_log.md`
- Verification: `.venv/bin/ruff check --fix ...`, `make clean && make check` (4 passed)
