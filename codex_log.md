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

- Timestamp (UTC): 2026-02-08T18:40:59Z
- Summary: Implemented M0 bootstrap persistence schema and deterministic recovery queue in photovault-clientd, with integration-style tests for reboot mapping and terminal-state exclusion.
- Files changed: `services/photovault-clientd/src/photovault_clientd/state_machine.py`, `services/photovault-clientd/src/photovault_clientd/db.py`, `services/photovault-clientd/src/photovault_clientd/app.py`, `services/photovault-clientd/tests/test_state_machine_contract.py`, `services/photovault-clientd/tests/test_bootstrap_recovery.py`, `codex_log.md`
- Verification: `make check` (7 passed, 2 FastAPI deprecation warnings)

- Timestamp (UTC): 2026-02-08T18:49:47Z
- Summary: Implemented persisted ingest flow for IDLE -> DISCOVERING -> STAGING_COPY with explicit daemon-state guards and staging bookkeeping endpoint.
- Files changed: `services/photovault-clientd/src/photovault_clientd/db.py`, `services/photovault-clientd/src/photovault_clientd/app.py`, `services/photovault-clientd/tests/test_ingest_flow.py`, `codex_log.md`
- Verification: `make check` (11 passed, FastAPI startup deprecation warnings only)

- Timestamp (UTC): 2026-02-08T19:09:50Z
- Summary: Implemented STAGING_COPY execution with real file copy + fsync and persisted retry/error metadata for copy failures.
- Files changed: `services/photovault-clientd/src/photovault_clientd/app.py`, `services/photovault-clientd/src/photovault_clientd/db.py`, `services/photovault-clientd/src/photovault_clientd/storage.py`, `services/photovault-clientd/tests/test_ingest_flow.py`, `codex_log.md`
- Verification: `.venv/bin/ruff check --fix services/photovault-clientd/tests/test_ingest_flow.py`, `make check` (12 passed, FastAPI startup deprecation warnings)

- Timestamp (UTC): 2026-02-08T20:50:50Z
- Summary: Aligned recovery and counters with copy-candidate model: {DISCOVERED, NEEDS_RETRY_COPY}; added pending_copy/staged/hash_pending metrics and staging unknown-job guard.
- Files changed: `services/photovault-clientd/src/photovault_clientd/db.py`, `services/photovault-clientd/src/photovault_clientd/app.py`, `services/photovault-clientd/tests/test_bootstrap_recovery.py`, `services/photovault-clientd/tests/test_ingest_flow.py`, `docs/photovault_client_state_machine.md`, `codex_log.md`
- Verification: `make check` (13 passed, FastAPI startup deprecation warnings)

- Timestamp (UTC): 2026-02-08T20:55:01Z
- Summary: Implemented bootstrap recovery queue consumption with deterministic resume-state selection and persisted daemon-state transitions; aligned staging endpoint to persist next daemon state.
- Files changed: `services/photovault-clientd/src/photovault_clientd/db.py`, `services/photovault-clientd/src/photovault_clientd/app.py`, `services/photovault-clientd/tests/test_bootstrap_recovery.py`, `services/photovault-clientd/tests/test_ingest_flow.py`, `docs/photovault_client_state_machine.md`, `codex_log.md`
- Verification: `make check` (14 passed, FastAPI startup deprecation warnings)

- Timestamp (UTC): 2026-02-08T20:57:43Z
- Summary: Reviewed canonical v1 docs and extracted the offline SD-card insertion state-machine flow for user-facing summary.
- Files changed: `codex_log.md`
- Verification: none

- Timestamp (UTC): 2026-02-08T21:04:43Z
- Summary: Added deterministic daemon transition engine and persistent daemon event logging/error classification; wired app state changes through validated transitions and exposed `/events` inspectability endpoint.
- Files changed: `services/photovault-clientd/src/photovault_clientd/transitions.py`, `services/photovault-clientd/src/photovault_clientd/db.py`, `services/photovault-clientd/src/photovault_clientd/app.py`, `services/photovault-clientd/tests/test_transitions_and_events.py`, `codex_log.md`
- Verification: `.venv/bin/ruff check --fix ...`, `make check` (16 passed, FastAPI startup deprecation warnings)

- Timestamp (UTC): 2026-02-08T21:07:01Z
- Summary: Added explicit SQLite schema versioning/migration flow (PRAGMA user_version), schema inspectability endpoint, and migration tests for fresh init/upgrade/future-version rejection.
- Files changed: `services/photovault-clientd/src/photovault_clientd/db.py`, `services/photovault-clientd/src/photovault_clientd/app.py`, `services/photovault-clientd/tests/test_schema_migrations.py`, `codex_log.md`
- Verification: `.venv/bin/ruff check --fix services/photovault-clientd/tests/test_schema_migrations.py`, `make check` (20 passed, FastAPI startup deprecation warnings)

- Timestamp (UTC): 2026-02-08T21:22:56Z
- Summary: Implemented single-thread recovery/tick dispatcher so startup executes resumed STAGING_COPY work until boundary; added manual daemon tick endpoint and integration tests.
- Files changed: `services/photovault-clientd/src/photovault_clientd/engine.py`, `services/photovault-clientd/src/photovault_clientd/db.py`, `services/photovault-clientd/src/photovault_clientd/app.py`, `services/photovault-clientd/tests/test_bootstrap_recovery.py`, `services/photovault-clientd/tests/test_transitions_and_events.py`, `docs/photovault_client_state_machine.md`, `codex_log.md`
- Verification: `make check` (22 passed, FastAPI startup deprecation warnings)

- Timestamp (UTC): 2026-02-08T21:27:34Z
- Summary: Migrated client daemon bootstrap from deprecated FastAPI `on_event` startup hook to lifespan handler while preserving recovery/transition/error semantics.
- Files changed: `services/photovault-clientd/src/photovault_clientd/app.py`, `codex_log.md`
- Verification: `make check` (22 passed)

- Timestamp (UTC): 2026-02-08T21:32:03Z
- Summary: Added shared daemon event taxonomy + copy error classification and wired classified retry events through staging/copy paths; expanded tests for taxonomy and emitted event categories.
- Files changed: `services/photovault-clientd/src/photovault_clientd/events.py`, `services/photovault-clientd/src/photovault_clientd/db.py`, `services/photovault-clientd/src/photovault_clientd/engine.py`, `services/photovault-clientd/src/photovault_clientd/app.py`, `services/photovault-clientd/tests/test_event_taxonomy.py`, `services/photovault-clientd/tests/test_ingest_flow.py`, `codex_log.md`
- Verification: `make check` (25 passed)

- Timestamp (UTC): 2026-02-08T21:34:15Z
- Summary: Hardened systemd lifecycle configuration and Ansible provisioning (dedicated service user, env files, persistent dirs, unit installation/reload/enable) with deployment contract tests.
- Files changed: `deploy/systemd/photovault-clientd.service`, `deploy/systemd/photovault-client-ui.service`, `deploy/systemd/photovault-api.service`, `deploy/systemd/photovault-server-ui.service`, `ansible/playbooks/bootstrap.yml`, `services/photovault-clientd/tests/test_deploy_lifecycle_contract.py`, `codex_log.md`
- Verification: `make check` (28 passed)

- Timestamp (UTC): 2026-02-08T21:36:01Z
- Summary: Added explicit recovery-policy validation and full reboot recovery matrix tests to guarantee all non-terminal file statuses are resumed deterministically and terminal statuses stay excluded.
- Files changed: `services/photovault-clientd/src/photovault_clientd/db.py`, `services/photovault-clientd/tests/test_recovery_matrix.py`, `codex_log.md`
- Verification: `make check` (31 passed)

- Timestamp (UTC): 2026-02-08T21:51:38Z
- Summary: Extended deterministic dispatcher to execute HASHING phase (SHA256 computation, persisted hash retries/errors, transition to DEDUP_SESSION_SHA boundary) and expanded recovery/event taxonomy tests.
- Files changed: `services/photovault-clientd/src/photovault_clientd/hashing.py`, `services/photovault-clientd/src/photovault_clientd/events.py`, `services/photovault-clientd/src/photovault_clientd/db.py`, `services/photovault-clientd/src/photovault_clientd/engine.py`, `services/photovault-clientd/src/photovault_clientd/transitions.py`, `services/photovault-clientd/tests/test_bootstrap_recovery.py`, `services/photovault-clientd/tests/test_event_taxonomy.py`, `services/photovault-clientd/tests/test_transitions_and_events.py`, `docs/photovault_client_state_machine.md`, `codex_log.md`
- Verification: `.venv/bin/ruff check --fix ...`, `make check` (36 passed)

- Timestamp (UTC): 2026-02-08T22:14:16Z
- Summary: Hardened reboot-safety observability: recovery dispatcher now logs explicit boundary/error stop events, with startup recovery tests for copy/hash retry paths and classified error persistence.
- Files changed: `services/photovault-clientd/src/photovault_clientd/events.py`, `services/photovault-clientd/src/photovault_clientd/engine.py`, `services/photovault-clientd/tests/test_bootstrap_recovery.py`, `codex_log.md`
- Verification: `make check` (38 passed)

- Timestamp (UTC): 2026-02-08T22:20:46Z
- Summary: Fixed startup/reboot BOOTSTRAP transition allowance for persisted non-idle states and added process-boundary reboot tests for copy/hash retry recovery completion across daemon restarts.
- Files changed: `services/photovault-clientd/src/photovault_clientd/transitions.py`, `services/photovault-clientd/tests/test_transitions_and_events.py`, `services/photovault-clientd/tests/test_bootstrap_recovery.py`, `codex_log.md`
- Verification: `make check` (41 passed)

- Timestamp (UTC): 2026-02-08T22:24:13Z
- Summary: Added startup fail-closed invariant checks and diagnostics endpoint for persisted state integrity (status validity, queue reference/terminal checks, hash-shape/retry invariants) with integration tests.
- Files changed: `services/photovault-clientd/src/photovault_clientd/events.py`, `services/photovault-clientd/src/photovault_clientd/db.py`, `services/photovault-clientd/src/photovault_clientd/app.py`, `services/photovault-clientd/tests/test_invariants.py`, `codex_log.md`
- Verification: `.venv/bin/ruff check --fix services/photovault-clientd/tests/test_invariants.py`, `make check` (44 passed)

- Timestamp (UTC): 2026-02-08T22:28:34Z
- Summary: Added M0 foundation diagnostics endpoint/check helper (`/diagnostics/m0`) to surface resume-map completeness, invariant health, and bootstrap queue state; added tests for clean and violated foundation checks.
- Files changed: `services/photovault-clientd/src/photovault_clientd/m0_checks.py`, `services/photovault-clientd/src/photovault_clientd/app.py`, `services/photovault-clientd/tests/test_m0_checks.py`, `codex_log.md`
- Verification: `.venv/bin/ruff check --fix services/photovault-clientd/tests/test_m0_checks.py`, `make check` (46 passed)

- Timestamp (UTC): 2026-04-19T16:04:10Z
- Summary: Reviewed canonical docs, current client daemon implementation, and test status to summarize project progress, milestone position, and recommended next steps.
- Files changed: `codex_log.md`
- Verification: `make check` (46 passed)

- Timestamp (UTC): 2026-04-19T16:40:00Z
- Summary: Implemented remaining M1 offline-ingest daemon phases (session dedup, local dedup, queue upload, local completion), added local SHA registry persistence and inspectability endpoints, extended recovery coverage, and attempted Raspberry Pi validation.
- Files changed: `services/photovault-clientd/src/photovault_clientd/app.py`, `services/photovault-clientd/src/photovault_clientd/db.py`, `services/photovault-clientd/src/photovault_clientd/engine.py`, `services/photovault-clientd/src/photovault_clientd/events.py`, `services/photovault-clientd/src/photovault_clientd/transitions.py`, `services/photovault-clientd/tests/test_bootstrap_recovery.py`, `services/photovault-clientd/tests/test_event_taxonomy.py`, `services/photovault-clientd/tests/test_invariants.py`, `services/photovault-clientd/tests/test_m0_checks.py`, `services/photovault-clientd/tests/test_m1_flow.py`, `services/photovault-clientd/tests/test_recovery_matrix.py`, `services/photovault-clientd/tests/test_schema_migrations.py`, `docs/photovault_client_state_machine.md`, `codex_log.md`
- Verification: `.venv/bin/pytest services/photovault-clientd/tests` (50 passed), `make check` (53 passed), `ssh -o BatchMode=yes -o StrictHostKeyChecking=no root@10.100.1.95 'hostname && systemctl is-active photovault-clientd'` (`No route to host`)

- Timestamp (UTC): 2026-04-19T16:51:53Z
- Summary: Bootstrapped the Raspberry Pi with Ansible, synced the photovault repo and runtime venv to `/opt/photovault`, started `photovault-clientd`, and validated on-device M1 ingest through `WAIT_NETWORK`; confirmed `PrivateTmp=true` hides host `/tmp` from the daemon during manual tests.
- Files changed: `codex_log.md`
- Verification: `.venv/bin/ansible-playbook -i '10.100.1.95,' -u root --private-key ~/.ssh/id_rsa_theworlt_bitbucket_key ansible/playbooks/bootstrap.yml`, `rsync -az ... /Users/ptroc/IdeaProjects/photovault/ root@10.100.1.95:/opt/photovault/`, `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'cd /opt/photovault && /opt/photovault/.venv/bin/pip install -r requirements.txt'`, `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'systemctl restart photovault-clientd && systemctl status photovault-clientd --no-pager -n 60'`, `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'curl -fsS http://127.0.0.1:9101/healthz && ... && curl -fsS http://127.0.0.1:9101/ingest/jobs/1'`

- Timestamp (UTC): 2026-04-19T17:03:10Z
- Summary: Exposed the client daemon and client UI on LAN interfaces, cleaned the misleading loopback-only endpoint label from the UI, redeployed the updated files to the Raspberry Pi, and verified access from this machine.
- Files changed: `services/photovault-clientd/src/photovault_clientd/main.py`, `services/photovault-client-ui/src/photovault_client_ui/main.py`, `services/photovault-client-ui/src/photovault_client_ui/app.py`, `services/photovault-client-ui/src/photovault_client_ui/templates/index.html`, `services/photovault-client-ui/tests/test_client_ui_app.py`, `codex_log.md`
- Verification: `make check` (53 passed), `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'systemctl restart photovault-clientd photovault-client-ui && ss -ltnp | egrep ":(9101|9201)\\s"'`, `curl -fsS http://10.100.1.95:9101/healthz`, `curl -fsS http://10.100.1.95:9201/`

- Timestamp (UTC): 2026-04-19T17:13:28Z
- Summary: Replaced the client UI skeleton with a live SSR dashboard backed by the local daemon API, added graceful daemon-error rendering and UI tests, redeployed the UI to the Raspberry Pi, and verified that it shows live state, diagnostics, jobs, and events.
- Files changed: `services/photovault-client-ui/src/photovault_client_ui/app.py`, `services/photovault-client-ui/src/photovault_client_ui/templates/index.html`, `services/photovault-client-ui/tests/test_client_ui_app.py`, `codex_log.md`
- Verification: `make check` (54 passed), `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'systemctl restart photovault-client-ui && systemctl status photovault-client-ui --no-pager -n 30'`, `curl -fsS http://10.100.1.95:9201/`

- Timestamp (UTC): 2026-04-19T17:16:29Z
- Summary: Added a read-only client UI job detail route with file-level status, retry, SHA, and error visibility, linked it from the overview dashboard, redeployed the UI to the Raspberry Pi, and verified the live `/jobs/1` page.
- Files changed: `services/photovault-client-ui/src/photovault_client_ui/app.py`, `services/photovault-client-ui/src/photovault_client_ui/templates/index.html`, `services/photovault-client-ui/tests/test_client_ui_app.py`, `codex_log.md`
- Verification: `make check` (56 passed), `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'systemctl restart photovault-client-ui && systemctl status photovault-client-ui --no-pager -n 20'`, `curl -fsS http://10.100.1.95:9201/jobs/1`

- Timestamp (UTC): 2026-04-19T17:31:54Z
- Summary: Added NetworkManager-backed network management to the client UI with live status, device and Wi-Fi listings, and a connect form wired to `nmcli`; redeployed the UI to the Raspberry Pi and verified the network section renders live data.
- Files changed: `services/photovault-client-ui/src/photovault_client_ui/app.py`, `services/photovault-client-ui/src/photovault_client_ui/templates/index.html`, `services/photovault-client-ui/tests/test_client_ui_app.py`, `codex_log.md`
- Verification: `make check` (60 passed), `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'nmcli general status && runuser -u photovault -- nmcli dev wifi list'`, `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'systemctl restart photovault-client-ui && systemctl status photovault-client-ui --no-pager -n 20'`, `curl -fsS http://10.100.1.95:9201/`

- Timestamp (UTC): 2026-04-19T17:41:26Z
- Summary: Split the client UI into dedicated overview and network subpages, moved network management to `/network`, added an explicit Wi-Fi scan action, redeployed the new templates and routes to the Raspberry Pi, and verified the dedicated network page renders live device and SSID data.
- Files changed: `services/photovault-client-ui/src/photovault_client_ui/app.py`, `services/photovault-client-ui/src/photovault_client_ui/templates/_base.html`, `services/photovault-client-ui/src/photovault_client_ui/templates/overview.html`, `services/photovault-client-ui/src/photovault_client_ui/templates/network.html`, `services/photovault-client-ui/tests/test_client_ui_app.py`, `codex_log.md`
- Verification: `make check` (63 passed), `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'systemctl restart photovault-client-ui && systemctl status photovault-client-ui --no-pager -n 20'`, `curl -fsS http://10.100.1.95:9201/network`

- Timestamp (UTC): 2026-04-19T17:44:54Z
- Summary: Improved client UI NetworkManager error handling to show a friendly authorization/config message, added a Raspberry Pi polkit rule so the `photovault` service user can manage Wi-Fi through `nmcli`, redeployed the UI, and verified `/network/scan` succeeds over HTTP.
- Files changed: `services/photovault-client-ui/src/photovault_client_ui/app.py`, `services/photovault-client-ui/tests/test_client_ui_app.py`, `codex_log.md`
- Verification: `.venv/bin/pytest services/photovault-client-ui/tests/test_client_ui_app.py`, `make check` (64 passed), `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'runuser -u photovault -- nmcli device wifi rescan'`, `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'systemctl restart photovault-client-ui && systemctl is-active photovault-client-ui'`, `curl -i -X POST http://10.100.1.95:9201/network/scan`, `curl -fsS http://10.100.1.95:9201/network`

- Timestamp (UTC): 2026-04-19T18:04:02Z
- Summary: Fixed the client UI `nmcli` multiline parser to split records when the same field key repeats without blank lines, which restored correct rendering of multiple visible Wi-Fi networks including SSIDs like `:)`, `:))`, and `:(`; redeployed the UI to the Raspberry Pi and verified the live page shows the expected SSIDs.
- Files changed: `services/photovault-client-ui/src/photovault_client_ui/app.py`, `services/photovault-client-ui/tests/test_client_ui_app.py`, `codex_log.md`
- Verification: `.venv/bin/pytest services/photovault-client-ui/tests/test_client_ui_app.py`, `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'runuser -u photovault -- nmcli -m multiline -f IN-USE,SSID,SIGNAL,SECURITY,CHAN,RATE device wifi list'`, `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'systemctl restart photovault-client-ui && systemctl is-active photovault-client-ui'`, `curl -fsS http://10.100.1.95:9201/network | grep -o ':))\\|:)\\|:(' | sort | uniq -c`

- Timestamp (UTC): 2026-04-19T18:25:22Z
- Summary: Added progressive AJAX-style operability to the client overview using partial SSR responses and lightweight fetch-based enhancement, including in-place ingest creation, in-place job-detail navigation, and a safe daemon tick action that still proxies all state changes through `photovault-clientd`; redeployed the updated UI to the Raspberry Pi and verified the live page serves the new controls and partial responses.
- Files changed: `services/photovault-client-ui/src/photovault_client_ui/app.py`, `services/photovault-client-ui/src/photovault_client_ui/templates/_base.html`, `services/photovault-client-ui/src/photovault_client_ui/templates/overview.html`, `services/photovault-client-ui/src/photovault_client_ui/templates/_overview_content.html`, `services/photovault-client-ui/tests/test_client_ui_app.py`, `codex_log.md`
- Verification: `.venv/bin/pytest services/photovault-client-ui/tests/test_client_ui_app.py`, `make check` (68 passed), `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'systemctl restart photovault-client-ui && systemctl is-active photovault-client-ui'`, `curl -fsS http://10.100.1.95:9201/ | rg -n "Operator Actions|data-ajax-target|Run daemon tick|button-link|window.history.pushState"`, `curl -fsS -H 'X-Requested-With: XMLHttpRequest' http://10.100.1.95:9201/jobs/1 | rg -n "Job #1 Detail|overview-shell|Run daemon tick|Back to overview"`

- Timestamp (UTC): 2026-04-19T21:09:21Z
- Summary: Validated the new operator flow end to end on the Raspberry Pi using an isolated temporary daemon/UI pair with a separate SQLite database and ports, so the real appliance state at `WAIT_NETWORK` was not disturbed; created a three-file ingest through the UI, advanced it via the UI `Run daemon tick` action through all M1 phases to `WAIT_NETWORK`, confirmed one same-session duplicate and two `READY_TO_UPLOAD` files, then cleaned up the temporary listeners.
- Files changed: `codex_log.md`
- Verification: `curl -fsS http://10.100.1.95:9101/state`, `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'mkdir -p /var/lib/photovault-ui-demo/staging /var/lib/photovault-ui-demo/media /var/lib/photovault-ui-demo/logs && printf alpha > /var/lib/photovault-ui-demo/media/001.txt && cp /var/lib/photovault-ui-demo/media/001.txt /var/lib/photovault-ui-demo/media/002.txt && printf beta > /var/lib/photovault-ui-demo/media/003.txt'`, `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'curl -i -X POST -H "X-Requested-With: XMLHttpRequest" --data-urlencode "media_label=ui-demo-sd" --data-urlencode "source_paths=/var/lib/photovault-ui-demo/media/001.txt\n/var/lib/photovault-ui-demo/media/002.txt\n/var/lib/photovault-ui-demo/media/003.txt" http://127.0.0.1:9312/ingest/jobs'`, `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 "for i in 1 2 3 4 5 6 7 8 9 10 11 12; do curl -fsS -X POST -H 'X-Requested-With: XMLHttpRequest' --data 'selected_job_id=1' http://127.0.0.1:9312/actions/daemon/tick >/dev/null; state=\$(curl -fsS http://127.0.0.1:9311/state | sed -n 's/.*\\\"current_state\\\":\\\"\\([^\\\"]*\\)\\\".*/\\1/p'); echo tick=\$i state=\$state; if [ \"\$state\" = \"WAIT_NETWORK\" ]; then break; fi; done; echo FINAL_STATE=\$(curl -fsS http://127.0.0.1:9311/state); echo FINAL_JOB=\$(curl -fsS http://127.0.0.1:9311/ingest/jobs/1)"`, `curl -fsS http://10.100.1.95:9201/ | rg -n "photovault client UI|Operator Actions|Network"`, `nc -vz 10.100.1.95 9311`, `nc -vz 10.100.1.95 9312`

- Timestamp (UTC): 2026-04-20T05:21:08Z
- Summary: Added a dependency overview section to the client UI home page showing local SQLite, staging storage, `NetworkManager.service`, and `photovault-api.service` state, with deterministic UI tests for the new overview data.
- Files changed: `services/photovault-client-ui/src/photovault_client_ui/app.py`, `services/photovault-client-ui/src/photovault_client_ui/templates/_overview_content.html`, `services/photovault-client-ui/tests/test_client_ui_app.py`, `codex_log.md`
- Verification: `.venv/bin/pytest services/photovault-client-ui/tests/test_client_ui_app.py`, `.venv/bin/ruff check services/photovault-client-ui/src/photovault_client_ui services/photovault-client-ui/tests`, `make check` (95 passed)

- Timestamp (UTC): 2026-04-19T21:19:40Z
- Summary: Implemented the first M2 metadata-handshake slice by adding a SHA256-based server handshake endpoint in `photovault-api` and wiring `photovault-clientd` `WAIT_NETWORK`/`UPLOAD_PREPARE` ticks to classify `READY_TO_UPLOAD` files as `DUPLICATE_SHA_GLOBAL` or `UPLOAD_REQUIRED` (stays `READY_TO_UPLOAD`), including persisted retry/error handling and restart-safe tests.
- Files changed: `services/photovault-api/src/photovault_api/app.py`, `services/photovault-api/tests/test_api_app.py`, `services/photovault-clientd/src/photovault_clientd/app.py`, `services/photovault-clientd/src/photovault_clientd/db.py`, `services/photovault-clientd/src/photovault_clientd/engine.py`, `services/photovault-clientd/src/photovault_clientd/events.py`, `services/photovault-clientd/tests/test_event_taxonomy.py`, `services/photovault-clientd/tests/test_m2_handshake.py`, `codex_log.md`
- Verification: `.venv/bin/pytest services/photovault-api/tests/test_api_app.py`, `.venv/bin/pytest services/photovault-clientd/tests/test_m2_handshake.py services/photovault-clientd/tests/test_event_taxonomy.py`, `.venv/bin/pytest services/photovault-clientd/tests/test_m1_flow.py services/photovault-clientd/tests/test_bootstrap_recovery.py`, `.venv/bin/ruff check --fix services/photovault-clientd/src/photovault_clientd/engine.py`, `make check` (75 passed)

- Timestamp (UTC): 2026-04-19T21:27:21Z
- Summary: Implemented the next M2 thin slice for non-resumable `UPLOAD_FILE` and `SERVER_VERIFY` after handshake: added server upload/verify endpoints, added client daemon tick handlers for upload + verify with persisted retry-safe behavior, and expanded integration-heavy tests for success, retry determinism, and restart recovery.
- Files changed: `services/photovault-api/src/photovault_api/app.py`, `services/photovault-api/tests/test_api_app.py`, `services/photovault-clientd/src/photovault_clientd/db.py`, `services/photovault-clientd/src/photovault_clientd/engine.py`, `services/photovault-clientd/src/photovault_clientd/events.py`, `services/photovault-clientd/tests/test_event_taxonomy.py`, `services/photovault-clientd/tests/test_m2_handshake.py`, `codex_log.md`
- Verification: `.venv/bin/pytest services/photovault-api/tests/test_api_app.py`, `.venv/bin/pytest services/photovault-clientd/tests/test_m2_handshake.py services/photovault-clientd/tests/test_event_taxonomy.py`, `.venv/bin/pytest services/photovault-clientd/tests/test_m1_flow.py services/photovault-clientd/tests/test_bootstrap_recovery.py`, `.venv/bin/ruff check --fix services/photovault-api/src/photovault_api/app.py services/photovault-clientd/src/photovault_clientd/engine.py`, `make check` (81 passed)

- Timestamp (UTC): 2026-04-19T21:32:11Z
- Summary: Redeployed current workspace to Raspberry Pi `10.100.1.95`, wiped the client SQLite state database as requested, restarted client services, and verified clean startup (`IDLE`, zero jobs) and live health/UI endpoints.
- Files changed: `codex_log.md`
- Verification: `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 hostname`, `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'systemctl is-active photovault-clientd'`, `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'systemctl is-active photovault-client-ui'`, `rsync -az --delete --exclude='.git' --exclude='.venv' --exclude='.pytest_cache' --exclude='.ruff_cache' /Users/ptroc/IdeaProjects/photovault/ root@10.100.1.95:/opt/photovault/`, `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'cd /opt/photovault && /opt/photovault/.venv/bin/pip install -r requirements.txt'`, `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'systemctl stop photovault-clientd photovault-client-ui'`, `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'rm -f /var/lib/photovault-clientd/state.sqlite3 /var/lib/photovault-clientd/state.sqlite3-wal /var/lib/photovault-clientd/state.sqlite3-shm'`, `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'systemctl start photovault-clientd photovault-client-ui'`, `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'systemctl is-active photovault-clientd photovault-client-ui'`, `curl -fsS http://10.100.1.95:9101/healthz`, `curl -fsS http://10.100.1.95:9101/state`, `curl -fsS http://10.100.1.95:9101/ingest/jobs`, `curl -fsS http://10.100.1.95:9101/schema`, `curl -fsS http://10.100.1.95:9201/`

- Timestamp (UTC): 2026-04-19T21:50:10Z
- Summary: Completed M2 closure validation on Raspberry Pi by driving a clean ingest job through the full remote terminal path (`WAIT_NETWORK -> UPLOAD_PREPARE -> UPLOAD_FILE -> SERVER_VERIFY -> POST_UPLOAD_VERIFY -> CLEANUP_STAGING -> JOB_COMPLETE_REMOTE -> JOB_COMPLETE_LOCAL -> IDLE`) and confirming deterministic final statuses with retained staged paths per v1 policy.
- Files changed: `codex_log.md`
- Verification: `ssh -o BatchMode=yes -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'curl -fsS http://127.0.0.1:9101/state && echo && curl -fsS http://127.0.0.1:9101/ingest/jobs && echo && curl -fsS http://127.0.0.1:9101/ingest/jobs/1'`, `ssh -o BatchMode=yes -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'set -e; for i in $(seq 1 80); do state=$(curl -fsS http://127.0.0.1:9101/state | sed -E "s/.*\\\"current_state\\\":\\\"([^\\\"]+)\\\".*/\\1/"); echo "tick=$i state=$state"; curl -fsS -X POST http://127.0.0.1:9101/daemon/tick >/dev/null; sleep 0.2; new_state=$(curl -fsS http://127.0.0.1:9101/state | sed -E "s/.*\\\"current_state\\\":\\\"([^\\\"]+)\\\".*/\\1/"); echo " -> $new_state"; if [ "$new_state" = "IDLE" ]; then break; fi; done; curl -fsS http://127.0.0.1:9101/state && echo; curl -fsS http://127.0.0.1:9101/ingest/jobs/1 && echo;'`

- Timestamp (UTC): 2026-04-19T21:55:26Z
- Summary: Hardened M2 deployment path by adding explicit `PHOTOVAULT_API_DATABASE_URL` bootstrap wiring in Ansible and added API coverage to verify env-driven Postgres state-store selection without changing endpoint contracts; re-ran full repository checks.
- Files changed: `ansible/playbooks/bootstrap.yml`, `services/photovault-api/tests/test_api_app.py`, `codex_log.md`
- Verification: `source .venv/bin/activate && pytest services/photovault-api/tests/test_api_app.py`, `source .venv/bin/activate && pytest services/photovault-clientd/tests/test_m2_handshake.py`, `source .venv/bin/activate && make check` (84 passed)

- Timestamp (UTC): 2026-04-19T22:05:01Z
- Summary: Applied the updated bootstrap playbook to Raspberry Pi `10.100.1.95` so `/etc/photovault/photovault-api.env` now explicitly contains `PHOTOVAULT_API_DATABASE_URL=`, then re-verified daemon/API/UI service health and local API/client state endpoints.
- Files changed: `codex_log.md`
- Verification: `/bin/zsh -lc "ANISBLE_HOST_KEY_CHECKING=False ANSIBLE_HOST_KEY_CHECKING=False .venv/bin/ansible-playbook -i '10.100.1.95,' -u root --private-key ~/.ssh/id_rsa_theworlt_bitbucket_key ansible/playbooks/bootstrap.yml"`, `ssh -o BatchMode=yes -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 "grep -n '^PHOTOVAULT_API_DATABASE_URL=' /etc/photovault/photovault-api.env"`, `ssh -o BatchMode=yes -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 "systemctl is-active photovault-api photovault-clientd photovault-client-ui && curl -fsS http://127.0.0.1:9301/healthz && echo && curl -fsS http://127.0.0.1:9101/state"`

- Timestamp (UTC): 2026-04-19T22:06:20Z
- Summary: Aligned client M2 verify-failure flow with the state machine by routing `SERVER_VERIFY` `VERIFY_FAILED` outcomes through `REUPLOAD_OR_QUARANTINE` before deterministic return to `WAIT_NETWORK` (no new states, non-resumable behavior preserved), and updated integration tests accordingly.
- Files changed: `services/photovault-clientd/src/photovault_clientd/engine.py`, `services/photovault-clientd/tests/test_m2_handshake.py`, `codex_log.md`
- Verification: `source .venv/bin/activate && pytest services/photovault-clientd/tests/test_m2_handshake.py services/photovault-clientd/tests/test_transitions_and_events.py`, `source .venv/bin/activate && ruff check services/photovault-clientd/src/photovault_clientd/engine.py services/photovault-clientd/tests/test_m2_handshake.py`, `source .venv/bin/activate && make check` (84 passed)

- Timestamp (UTC): 2026-04-20T04:07:08Z
- Summary: Implemented explicit retry-exhaustion handling for M2 remote verify failures: `REUPLOAD_OR_QUARANTINE` now deterministically moves files to `ERROR_FILE` after `max_upload_retries` (default `3`) and transitions daemon/job to `ERROR_FILE`; non-exhausted retries still return to `WAIT_NETWORK` as non-resumable retry policy.
- Files changed: `services/photovault-clientd/src/photovault_clientd/db.py`, `services/photovault-clientd/src/photovault_clientd/engine.py`, `services/photovault-clientd/tests/test_m2_handshake.py`, `codex_log.md`
- Verification: `source .venv/bin/activate && pytest services/photovault-clientd/tests/test_m2_handshake.py`, `source .venv/bin/activate && ruff check --fix services/photovault-clientd/src/photovault_clientd/engine.py`, `source .venv/bin/activate && make check` (85 passed)

- Timestamp (UTC): 2026-04-20T04:07:58Z
- Summary: Redeployed latest workspace to Raspberry Pi `10.100.1.95`, restarted client/API/UI services, and re-validated service and endpoint health after a brief restart warm-up to keep appliance behavior aligned with the new M2 retry-exhaustion logic.
- Files changed: `codex_log.md`
- Verification: `rsync -az --delete --exclude='.git' --exclude='.venv' --exclude='.pytest_cache' --exclude='.ruff_cache' /Users/ptroc/IdeaProjects/photovault/ root@10.100.1.95:/opt/photovault/`, `ssh -o BatchMode=yes -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'systemctl restart photovault-clientd photovault-api photovault-client-ui && systemctl is-active photovault-clientd photovault-api photovault-client-ui && curl -fsS http://127.0.0.1:9101/healthz && echo && curl -fsS http://127.0.0.1:9301/healthz && echo && curl -fsS http://127.0.0.1:9101/state'`, `sleep 3 && ssh -o BatchMode=yes -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'systemctl is-active photovault-clientd photovault-api photovault-client-ui && curl -fsS http://127.0.0.1:9101/healthz && echo && curl -fsS http://127.0.0.1:9301/healthz && echo && curl -fsS http://127.0.0.1:9101/state'`

- Timestamp (UTC): 2026-04-20T04:29:10Z
- Summary: Configured Raspberry Pi `photovault-api` to use the new PostgreSQL database (`photo/photo`) via `PHOTOVAULT_API_DATABASE_URL=postgresql://photo:photo@127.0.0.1/photo`, validated DB connectivity, and confirmed restart-persistent API dedup behavior end-to-end (upload temp -> restart -> verify -> restart -> handshake ALREADY_EXISTS) with persisted SHA row in `api_known_sha256`.
- Files changed: `codex_log.md`
- Verification: `ssh -o BatchMode=yes -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 "sed/grep update for /etc/photovault/photovault-api.env + systemctl restart photovault-api"`, `ssh -o BatchMode=yes -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 "PGPASSWORD=photo psql -h 127.0.0.1 -U photo -d photo -c 'SELECT current_database(), current_user;'"`, `ssh -o BatchMode=yes -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'localhost API handshake/upload/restart/verify/restart/handshake sequence'`, `ssh -o BatchMode=yes -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 "PGPASSWORD=photo psql -h 127.0.0.1 -U photo -d photo -c \"SELECT sha256_hex FROM api_known_sha256 WHERE sha256_hex = 'c40ef4ed6c7c76398bfd267edeeab056dee77fe5c6bbbac3820a13fed422a563';\""`

- Timestamp (UTC): 2026-04-20T04:39:41Z
- Summary: Implemented M2 retry/backoff hardening in `photovault-clientd` by adding deterministic `WAIT_NETWORK` retry gating for both `READY_TO_UPLOAD` and `UPLOADED` work (explicit `next_retry_at_utc`), plus an operator control-plane endpoint `POST /ingest/files/{file_id}/retry-upload` to requeue `ERROR_FILE` uploads without adding new states; validated locally and on Pi with a mixed dedup/upload job against PostgreSQL-backed API.
- Files changed: `services/photovault-clientd/src/photovault_clientd/app.py`, `services/photovault-clientd/src/photovault_clientd/db.py`, `services/photovault-clientd/src/photovault_clientd/engine.py`, `services/photovault-clientd/tests/test_m2_handshake.py`, `codex_log.md`
- Verification: `source .venv/bin/activate && pytest services/photovault-clientd/tests/test_m2_handshake.py services/photovault-clientd/tests/test_transitions_and_events.py`, `source .venv/bin/activate && pytest services/photovault-clientd/tests/test_m1_flow.py services/photovault-clientd/tests/test_bootstrap_recovery.py`, `source .venv/bin/activate && make check` (88 passed), `rsync -az --delete ... /Users/ptroc/IdeaProjects/photovault/ root@10.100.1.95:/opt/photovault/`, `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'restart services + localhost mixed-job validation'` with final job status counts `DUPLICATE_SESSION_SHA=1, DUPLICATE_SHA_GLOBAL=1, VERIFIED_REMOTE=1`

- Timestamp (UTC): 2026-04-20T04:55:15Z
- Summary: Completed M2 operator-trust polish by exposing upload-recovery controls in the client UI: job detail now renders a per-file `Retry upload` action for `ERROR_FILE` rows and proxies to daemon endpoint `/ingest/files/{file_id}/retry-upload`, with clear in-page success/error notices for control-plane operation without SSH.
- Files changed: `services/photovault-client-ui/src/photovault_client_ui/app.py`, `services/photovault-client-ui/src/photovault_client_ui/templates/_overview_content.html`, `services/photovault-client-ui/tests/test_client_ui_app.py`, `codex_log.md`
- Verification: `source .venv/bin/activate && ruff check services/photovault-client-ui/src/photovault_client_ui/app.py services/photovault-client-ui/tests/test_client_ui_app.py`, `source .venv/bin/activate && pytest services/photovault-client-ui/tests/test_client_ui_app.py`, `source .venv/bin/activate && make check` (90 passed), `rsync -az --delete ... /Users/ptroc/IdeaProjects/photovault/ root@10.100.1.95:/opt/photovault/`, `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'systemctl restart photovault-client-ui photovault-clientd photovault-api && systemctl is-active ...'`, `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'curl -fsS -X POST -d selected_job_id=2 -d file_id=5 http://127.0.0.1:9201/actions/retry-upload | grep -n \"Failed to requeue file #5 for upload\"'`

- Timestamp (UTC): 2026-04-20T05:10:53Z
- Summary: Fixed three M2 correctness issues in `photovault-clientd`: `WAIT_NETWORK` now checks NetworkManager online state before advancing even when backoff is due, `REUPLOAD_OR_QUARANTINE` now targets the specific failed-verify file for the active reupload job deterministically, and `CLEANUP_STAGING` now enforces retain/delete policy by actually deleting staged files when `retain_staged_files=false` while retaining when true.
- Files changed: `services/photovault-clientd/src/photovault_clientd/engine.py`, `services/photovault-clientd/src/photovault_clientd/db.py`, `services/photovault-clientd/tests/test_m2_handshake.py`, `codex_log.md`
- Verification: `source .venv/bin/activate && ruff check services/photovault-clientd/src/photovault_clientd/engine.py services/photovault-clientd/src/photovault_clientd/db.py services/photovault-clientd/tests/test_m2_handshake.py`, `source .venv/bin/activate && pytest services/photovault-clientd/tests/test_m2_handshake.py`, `source .venv/bin/activate && make check` (95 passed)

- Timestamp (UTC): 2026-04-20T05:28:31Z
- Summary: Fixed two remaining M2 contract-alignment issues: `WAIT_NETWORK` now treats missing NetworkManager (`nmcli` unavailable) as offline and remains gated, and `CLEANUP_STAGING` now transitions to `PAUSED_STORAGE` on staged-file deletion failures per state-machine contract.
- Files changed: `services/photovault-clientd/src/photovault_clientd/engine.py`, `services/photovault-clientd/src/photovault_clientd/transitions.py`, `services/photovault-clientd/tests/test_m2_handshake.py`, `codex_log.md`
- Verification: `source .venv/bin/activate && ruff check services/photovault-clientd/src/photovault_clientd/engine.py services/photovault-clientd/src/photovault_clientd/transitions.py services/photovault-clientd/tests/test_m2_handshake.py`, `source .venv/bin/activate && pytest services/photovault-clientd/tests/test_m2_handshake.py`, `source .venv/bin/activate && make check` (97 passed)

- Timestamp (UTC): 2026-04-20T10:22:30Z
- Summary: Added M2 operator visibility to `photovault-client-ui` by rendering explicit remote classification, upload/verify phase labels, cleanup status, and job-level operational state (`local complete` / `remote complete` / `paused on error`) from daemon API data; validated with UI tests and Raspberry Pi end-to-end drills including live `WAIT_NETWORK` retry error visibility and recovery.
- Files changed: `services/photovault-client-ui/src/photovault_client_ui/app.py`, `services/photovault-client-ui/src/photovault_client_ui/templates/_overview_content.html`, `services/photovault-client-ui/tests/test_client_ui_app.py`, `codex_log.md`
- Verification: `source .venv/bin/activate && pytest -q services/photovault-client-ui/tests/test_client_ui_app.py`, `source .venv/bin/activate && pytest -q services/photovault-clientd/tests/test_m2_handshake.py services/photovault-clientd/tests/test_ingest_flow.py`, `scp -i ~/.ssh/id_rsa_theworlt_bitbucket_key .../app.py .../_overview_content.html root@10.100.1.95:/opt/photovault/...`, `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'systemctl restart photovault-client-ui.service && systemctl is-active photovault-client-ui.service'`, `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 '<isolated /var/lib/photovault-clientd/codex-m2-* ingest jobs + /daemon/tick loops>'`, `ssh -i ~/.ssh/id_rsa_theworlt_bitbucket_key root@10.100.1.95 'curl -fsS http://127.0.0.1:9201/jobs/{2,3,6}'`, `node -e 'Playwright chromium launch against http://10.100.1.95:9201/jobs/6'` (failed in sandbox with Mach bootstrap permission error; browser automation not permitted in this environment)
