# Scripts

## Raspberry Pi deploy

Use [`deploy_rpi.sh`](./deploy_rpi.sh) to sync the current workspace to the Raspberry Pi,
refresh the remote Python environment, restart photovault services, verify the configured
server storage root is usable, and run explicit M4 storage/index smoke checks.

The deploy path now validates:

- `/etc/photovault/photovault-api.env` exists
- `PHOTOVAULT_API_STORAGE_ROOT` is configured
- the storage root exists and is writable by the `photovault` service user
- API/client/server UI health endpoints respond
- a deterministic M4 smoke file can be indexed and then deduplicated by SHA

Examples:

```bash
scripts/deploy_rpi.sh
scripts/deploy_rpi.sh --service photovault-client-ui.service
scripts/deploy_rpi.sh --dry-run
scripts/deploy_rpi.sh --skip-smoke
```

## M4 smoke helper

You can also run the storage/index smoke check directly on a host:

```bash
source .venv/bin/activate
.venv/bin/python scripts/m4_smoke_check.py --storage-root /var/storage/photovault
```

The helper writes a deterministic fixture at `_photovault_smoke/m4/manual-smoke.txt`,
runs `POST /v1/storage/index`, checks that metadata handshake returns `ALREADY_EXISTS`
for that fixture SHA, and confirms the latest index run plus admin files view reflect it.
