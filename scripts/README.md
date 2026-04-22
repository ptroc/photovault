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
