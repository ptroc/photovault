# photovault Server Install (Generic Linux Host)

This guide installs the server-side photovault components on a generic Linux host:

- `photovault-api` (FastAPI, port `9301`)
- `photovault-server-ui` (Flask SSR UI, port `9401`)

Assumption: PostgreSQL is already installed and reachable from this host.

If you prefer containers, use the Docker Compose wrapper guide:
`docs/server_install_docker_compose.md`.

## 1. Install OS prerequisites

Use your distro package manager to install at least:

- `python3`
- `python3-venv`
- `python3-pip`
- `git`

Example (Debian/Ubuntu):

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip git
```

## 2. Create service user and directories

```bash
sudo groupadd --system photovault || true
sudo useradd --system --gid photovault --no-create-home --shell /usr/sbin/nologin photovault || true

sudo mkdir -p /opt/photovault
sudo mkdir -p /etc/photovault
sudo mkdir -p /storage/photovault
sudo chmod 0750 /etc/photovault
sudo chown root:photovault /etc/photovault
sudo chown -R photovault:photovault /storage/photovault
sudo chmod 0750 /storage/photovault
```

## 3. Place project code on host

```bash
sudo git clone <YOUR_REPO_URL> /opt/photovault
sudo chown -R root:root /opt/photovault
```

If the repo already exists:

```bash
cd /opt/photovault
sudo git fetch --all
sudo git checkout <BRANCH_OR_TAG>
sudo git pull --ff-only
```

## 4. Create Python virtual environment and install deps

```bash
cd /opt/photovault
sudo python3 -m venv .venv
sudo ./.venv/bin/pip install --upgrade pip
sudo ./.venv/bin/pip install -r requirements.txt -r requirements-dev.txt
```

## 5. Create PostgreSQL database and user

Run as a PostgreSQL superuser and replace placeholders:

```sql
CREATE USER photovault_api WITH PASSWORD 'change-me-strong-password';
CREATE DATABASE photovault OWNER photovault_api;
GRANT ALL PRIVILEGES ON DATABASE photovault TO photovault_api;
```

Example command:

```bash
sudo -u postgres psql
```

Notes:

- The API auto-creates required tables on startup (`api_known_sha256`, `api_temp_uploads`).
- No manual migration step is required for current server-side scaffold.

## 6. Configure service environment files

Create `/etc/photovault/photovault-api.env`:

```bash
sudo tee /etc/photovault/photovault-api.env >/dev/null <<'ENV'
PHOTOVAULT_API_DATABASE_URL=postgresql://photovault_api:change-me-strong-password@127.0.0.1:5432/photovault
PHOTOVAULT_API_STORAGE_ROOT=/storage/photovault
PHOTOVAULT_API_PREVIEW_MAX_LONG_EDGE=2048
PHOTOVAULT_API_PREVIEW_PASSTHROUGH_SUFFIXES=.jpg,.jpeg
PHOTOVAULT_API_PREVIEW_PLACEHOLDER_SUFFIXES=.avi,.mp4
ENV
```

`PHOTOVAULT_API_PREVIEW_MAX_LONG_EDGE` controls preview JPEG size by longest edge.
It must be a positive integer. If omitted, the API defaults to `1024`.
`PHOTOVAULT_API_PREVIEW_PASSTHROUGH_SUFFIXES` disables preview generation for listed suffixes and
serves the original file from `/v1/admin/catalog/preview` (e.g. `.jpg,.jpeg`).
`PHOTOVAULT_API_PREVIEW_PLACEHOLDER_SUFFIXES` disables preview generation and keeps placeholder
behavior for listed suffixes (e.g. `.avi,.mp4`).

Create `/etc/photovault/photovault-server-ui.env` (currently optional but required by unit convention):

```bash
sudo tee /etc/photovault/photovault-server-ui.env >/dev/null <<'ENV'
# reserved for photovault-server-ui runtime variables
ENV
```

Apply secure ownership:

```bash
sudo chown root:photovault /etc/photovault/photovault-api.env /etc/photovault/photovault-server-ui.env
sudo chmod 0640 /etc/photovault/photovault-api.env /etc/photovault/photovault-server-ui.env
```

## 7. Install systemd units

Copy unit files from repo:

```bash
sudo cp /opt/photovault/deploy/systemd/photovault-api.service /etc/systemd/system/
sudo cp /opt/photovault/deploy/systemd/photovault-server-ui.service /etc/systemd/system/
```

Reload and enable:

```bash
sudo systemctl daemon-reload
sudo systemctl enable photovault-api.service photovault-server-ui.service
```

## 8. Start services

```bash
sudo systemctl restart photovault-api.service
sudo systemctl restart photovault-server-ui.service
```

## 9. Verify health

Check status:

```bash
sudo systemctl status photovault-api.service --no-pager
sudo systemctl status photovault-server-ui.service --no-pager
```

Check HTTP endpoints locally:

```bash
curl -fsS http://127.0.0.1:9301/healthz
curl -fsS http://127.0.0.1:9401/ >/dev/null && echo "server-ui: ok"
```

Expected:

- API health returns JSON like `{"status":"ok"}`
- Server UI request succeeds

Run the explicit M4 smoke check:

```bash
cd /opt/photovault
sudo -u photovault ./.venv/bin/python scripts/m4_smoke_check.py --storage-root /storage/photovault
```

Expected:

- output includes `m4-smoke: ok`
- the deterministic smoke fixture is indexed under `_photovault_smoke/m4/manual-smoke.txt`
- unauthenticated metadata handshake is rejected with `CLIENT_AUTH_REQUIRED`

## 10. Day-2 operations

Restart after config or code change:

```bash
sudo systemctl restart photovault-api.service photovault-server-ui.service
```

Tail logs:

```bash
sudo journalctl -u photovault-api.service -f
sudo journalctl -u photovault-server-ui.service -f
```

## Troubleshooting quick checks

- If API fails on boot, verify both `PHOTOVAULT_API_DATABASE_URL` and
  `PHOTOVAULT_API_STORAGE_ROOT` are set and valid.
- If API logs show storage permission errors, verify `/storage/photovault` is writable by the
  `photovault` service user.
- If health checks pass but M4 smoke fails, confirm the configured storage root is the same path
  exposed to `PHOTOVAULT_API_STORAGE_ROOT` and that the API can write and re-index within it.
- If DB connection fails, test login with `psql` using the same credentials/host.
- If units fail to start, confirm `/opt/photovault/.venv/bin/python` exists.
- If ports are unreachable remotely, check host firewall for `9301` and `9401`.
