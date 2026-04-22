# photovault Server Install via Docker Compose (External PostgreSQL)

This guide runs the server-side photovault services in Docker containers using Docker Compose as a wrapper:

- `photovault-api` on host port `9301`
- `photovault-server-ui` on host port `9401`

Assumptions:

- PostgreSQL already exists outside Docker.
- Host storage path is configurable via `.env` and mounted into the containers.
- Project code is available on the host (example path: `/opt/photovault`).
- `photovault-server-ui` reaches `photovault-api` over the internal Compose network.

## 1. Install Docker and Docker Compose plugin

Example (Debian/Ubuntu):

```bash
sudo apt update
sudo apt install -y docker.io docker-compose-plugin
sudo systemctl enable --now docker
```

Optional: allow non-root docker usage.

```bash
sudo usermod -aG docker "$USER"
```

## 2. Place project code on the host

```bash
sudo git clone <YOUR_REPO_URL> /opt/photovault
sudo chown -R root:root /opt/photovault
```

Or update an existing checkout:

```bash
cd /opt/photovault
sudo git fetch --all
sudo git checkout <BRANCH_OR_TAG>
sudo git pull --ff-only
```

## 3. Ensure host storage path exists

```bash
sudo mkdir -p /storage/photovault
```

Note: the compose file uses env-based bind mount variables:
`PHOTOVAULT_STORAGE_HOST_ROOT` → `PHOTOVAULT_STORAGE_CONTAINER_ROOT`.

## 4. Configure environment file for external PostgreSQL

Create `/opt/photovault/deploy/docker/.env`:

```bash
sudo tee /opt/photovault/deploy/docker/.env >/dev/null <<'ENV'
# External PostgreSQL connection for photovault-api.
# If PostgreSQL runs on the Docker host, use host.docker.internal.
PHOTOVAULT_API_DATABASE_URL=postgresql://photovault_api:change-me-strong-password@host.docker.internal:5432/photovault
PHOTOVAULT_STORAGE_HOST_ROOT=/storage/photovault
PHOTOVAULT_STORAGE_CONTAINER_ROOT=/var/storage
PHOTOVAULT_API_STORAGE_ROOT=/var/storage
ENV
```

If your DB runs on another machine, replace `host.docker.internal` with that hostname/IP.
`PHOTOVAULT_API_STORAGE_ROOT` must be inside the container path selected by
`PHOTOVAULT_STORAGE_CONTAINER_ROOT`.

## 5. Build and start services with Docker Compose

```bash
cd /opt/photovault/deploy/docker
sudo docker compose --env-file .env -f docker-compose.server.yml up -d --build
```

The compose file is at:

- `/opt/photovault/deploy/docker/docker-compose.server.yml`

## 6. Verify container health

```bash
sudo docker compose -f /opt/photovault/deploy/docker/docker-compose.server.yml ps
sudo docker compose -f /opt/photovault/deploy/docker/docker-compose.server.yml logs --tail=100 photovault-api
sudo docker compose -f /opt/photovault/deploy/docker/docker-compose.server.yml logs --tail=100 photovault-server-ui
```

Local HTTP verification:

```bash
curl -fsS http://127.0.0.1:9301/healthz
curl -fsS http://127.0.0.1:9401/ >/dev/null && echo "server-ui: ok"
```

Expected:

- API returns JSON like `{"status":"ok"}`
- Server UI responds on `/`
- Server UI proxies its overview/files data from `http://photovault-api:9301` inside Compose

Run the explicit M4 smoke check inside the API container:

```bash
cd /opt/photovault
sudo docker compose -f deploy/docker/docker-compose.server.yml exec photovault-api \
  python scripts/m4_smoke_check.py --storage-root "${PHOTOVAULT_API_STORAGE_ROOT:-/var/storage}"
```

Expected:

- output includes `m4-smoke: ok`
- the deterministic smoke fixture is indexed under `_photovault_smoke/m4/manual-smoke.txt`
- unauthenticated metadata handshake is rejected with `CLIENT_AUTH_REQUIRED`

## 7. Day-2 operations

Restart:

```bash
sudo docker compose -f /opt/photovault/deploy/docker/docker-compose.server.yml restart
```

Stop:

```bash
sudo docker compose -f /opt/photovault/deploy/docker/docker-compose.server.yml down
```

Pull new code and redeploy:

```bash
cd /opt/photovault
sudo git pull --ff-only
cd /opt/photovault/deploy/docker
sudo docker compose --env-file .env -f docker-compose.server.yml up -d --build --force-recreate
```

## Troubleshooting quick checks

- If API exits immediately, verify both `PHOTOVAULT_API_DATABASE_URL` and
  `PHOTOVAULT_API_STORAGE_ROOT` in `.env`.
- If DB connection fails and DB is on host, confirm PostgreSQL listens on an interface reachable from Docker and permits your user/password.
- If health checks stay unhealthy, inspect logs with `docker compose logs` and test DB connectivity separately.
- If container health is green but M4 smoke fails, verify the bind-mounted storage path is the
  same path configured in `PHOTOVAULT_API_STORAGE_ROOT` and is writable inside the container.
- If remote access fails, verify host firewall allows `9301` and `9401`.
