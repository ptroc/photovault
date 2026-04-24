# Scripts

## Raspberry Pi deploy

Use [`deploy_rpi.sh`](./deploy_rpi.sh) to sync the current workspace to the Raspberry Pi,
refresh the remote Python environment, restart photovault services, verify the configured
server storage root is usable, wait for service health with retries, and run explicit
M4 storage/index smoke checks.

The deploy path now validates:

- `/etc/photovault/photovault-api.env` exists
- `PHOTOVAULT_API_STORAGE_ROOT` is configured, defaulting to `/storage/photovault` if unset
- the storage root exists and is writable by the `photovault` service user
- API/client/server UI health endpoints respond after post-restart retry polling
- a deterministic M4 smoke file can be indexed and observed via admin files/latest-index-run
- privileged upload handshake enforces client auth (`CLIENT_AUTH_REQUIRED`)

Examples:

```bash
scripts/deploy_rpi.sh
scripts/deploy_rpi.sh --service photovault-client-ui.service
scripts/deploy_rpi.sh --dry-run
scripts/deploy_rpi.sh --skip-smoke
```

## Trash purge script

[`purge_trash.py`](./purge_trash.py) hard-deletes trash files and their
tombstone rows once they are older than the retention window (default 14 days).
It is designed to be invoked once per day by cron — it is single-shot,
idempotent, and safe to run concurrently with itself (uses
`SELECT … FOR UPDATE SKIP LOCKED` so two overlapping jobs cannot double-purge
the same row).

### Dry run (see what would be purged without touching anything)

```bash
source .venv/bin/activate
python scripts/purge_trash.py \
  --storage-root /path/to/storage \
  --database-url postgres://user:pass@host/db \
  --dry-run
```

### Normal invocation

```bash
python scripts/purge_trash.py \
  --storage-root /path/to/storage \
  --database-url postgres://user:pass@host/db \
  --log-json
```

### Output shape

In `--log-json` mode each row produces one NDJSON line, followed by a
summary line:

```json
{"event": "purge", "relative_path": "2026/04/job/photo.jpg", "sha256_hex": "abc123...", "status": "file_deleted", ...}
{"event": "summary", "scanned": 3, "purged_files": 2, "purged_rows": 3, "missing_files": 1, "errors": 0, "duration_seconds": 0.042, ...}
```

Exit code is `0` on success (including already-gone files), `1` if any
unrecoverable error occurred.

### Sample crontab line (set up once per install, not managed by Ansible)

```crontab
15 3 * * * root /opt/photovault/.venv/bin/python /opt/photovault/scripts/purge_trash.py --storage-root /data/photovault --database-url $DATABASE_URL --log-json >> /var/log/photovault/purge.log 2>&1
```

## M4 smoke helper

You can also run the storage/index smoke check directly on a host:

```bash
source .venv/bin/activate
.venv/bin/python scripts/m4_smoke_check.py --storage-root /var/storage/photovault
```

The helper writes a deterministic fixture at `_photovault_smoke/m4/manual-smoke.txt`,
runs `POST /v1/storage/index`, confirms the latest index run plus admin files view reflect
the fixture SHA/path, and verifies unauthenticated metadata handshake is rejected with
`CLIENT_AUTH_REQUIRED`.
