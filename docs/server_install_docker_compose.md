# photovault Server Install via Docker Compose (External PostgreSQL)

This guide runs the server-side photovault services in Docker containers using Docker Compose as a wrapper:

- `photovault-api` on host port `9301`
- `photovault-server-ui` on host port `9401`

Assumptions:

- PostgreSQL already exists outside Docker.
- Host storage is mounted at `/var/storage` and must be visible to containers.
- Project code is available on the host at `/opt/photovault`.

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
sudo mkdir -p /var/storage
sudo mkdir -p /var/storage/photovault
```

Note: this mount is provided to both containers as a fixed bind mount (`/var/storage:/var/storage`).

## 4. Configure environment file for external PostgreSQL

Create `/opt/photovault/deploy/docker/.env`:

```bash
sudo tee /opt/photovault/deploy/docker/.env >/dev/null <<'ENV'
# External PostgreSQL connection for photovault-api.
# If PostgreSQL runs on the Docker host, use host.docker.internal.
PHOTOVAULT_API_DATABASE_URL=postgresql://photovault_api:change-me-strong-password@host.docker.internal:5432/photovault
PHOTOVAULT_API_STORAGE_ROOT=/var/storage/photovault
ENV
```

If your DB runs on another machine, replace `host.docker.internal` with that hostname/IP.

## 5. Start services with Docker Compose

```bash
cd /opt/photovault/deploy/docker
sudo docker compose --env-file .env -f docker-compose.server.yml up -d
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
sudo docker compose --env-file .env -f docker-compose.server.yml up -d --force-recreate
```

## Troubleshooting quick checks

- If API exits immediately, verify both `PHOTOVAULT_API_DATABASE_URL` and
  `PHOTOVAULT_API_STORAGE_ROOT` in `.env`.
- If DB connection fails and DB is on host, confirm PostgreSQL listens on an interface reachable from Docker and permits your user/password.
- If health checks stay unhealthy, inspect logs with `docker compose logs` and test DB connectivity separately.
- If remote access fails, verify host firewall allows `9301` and `9401`.
