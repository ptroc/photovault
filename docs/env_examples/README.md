# photovault Configuration Environment Files

This directory contains example environment files for each runtime service and the
Docker Compose server deployment wrapper.

These files are documentation-first examples. They are not loaded automatically.

## Canonical service env file locations

- `photovault-api` -> `/etc/photovault/photovault-api.env`
- `photovault-server-ui` -> `/etc/photovault/photovault-server-ui.env`
- `photovault-clientd` -> `/etc/photovault/photovault-clientd.env`
- `photovault-client-ui` -> `/etc/photovault/photovault-client-ui.env`
- Docker Compose server deployment -> `deploy/docker/.env`

## Files in this directory

- [`photovault-api.env.example`](/Users/ptroc/IdeaProjects/photovault/docs/env_examples/photovault-api.env.example)
- [`photovault-server-ui.env.example`](/Users/ptroc/IdeaProjects/photovault/docs/env_examples/photovault-server-ui.env.example)
- [`photovault-clientd.env.example`](/Users/ptroc/IdeaProjects/photovault/docs/env_examples/photovault-clientd.env.example)
- [`photovault-client-ui.env.example`](/Users/ptroc/IdeaProjects/photovault/docs/env_examples/photovault-client-ui.env.example)
- [`docker-compose.server.env.example`](/Users/ptroc/IdeaProjects/photovault/docs/env_examples/docker-compose.server.env.example)

## `photovault-api.env`

Used by `deploy/systemd/photovault-api.service`.

| Variable | Required | Default | Notes |
|---|---|---|---|
| `PHOTOVAULT_API_DATABASE_URL` | No | in-memory store | PostgreSQL connection string. If omitted, the API falls back to the in-memory state store, which is not appropriate for persistent server deployments. |
| `PHOTOVAULT_API_STORAGE_ROOT` | Yes | none | Absolute storage root for uploaded and indexed files. The API refuses to start if this is missing. |
| `PHOTOVAULT_API_BOOTSTRAP_TOKEN` | No | empty string | Enables client bootstrap enrollment. If empty, bootstrap enrollment is disabled. Treat as a secret. |
| `PHOTOVAULT_API_PREVIEW_CACHE_ROOT` | No | sibling of storage root named `.photovault_preview_cache` | Absolute path where generated preview JPEGs are cached. |
| `PHOTOVAULT_API_PREVIEW_MAX_LONG_EDGE` | No | `1024` | Positive integer. Maximum preview size on the longest edge. Invalid or non-positive values fail startup. |
| `PHOTOVAULT_API_PREVIEW_PASSTHROUGH_SUFFIXES` | No | empty | Comma-separated suffix list such as `.jpg,.jpeg`. Matching files skip preview generation and the original file is served instead. |
| `PHOTOVAULT_API_PREVIEW_PLACEHOLDER_SUFFIXES` | No | empty | Comma-separated suffix list such as `.avi,.mp4`. Matching files skip preview generation and remain in placeholder mode. |

## `photovault-server-ui.env`

Used by `deploy/systemd/photovault-server-ui.service`.

| Variable | Required | Default | Notes |
|---|---|---|---|
| `PHOTOVAULT_SERVER_UI_API_BASE_URL` | No | `http://127.0.0.1:9301` | Base URL used by the SSR server UI when calling `photovault-api`. |
| `PHOTOVAULT_SERVER_UI_PREVIEW_CACHE_ROOT` | No | empty | Optional path shown in server UI asset detail for preview files. Keep it aligned with the API preview cache root for operator clarity. |

If `PHOTOVAULT_SERVER_UI_PREVIEW_CACHE_ROOT` is empty, the UI falls back to
`PHOTOVAULT_API_PREVIEW_CACHE_ROOT` when formatting preview paths.

## `photovault-clientd.env`

Used by `deploy/systemd/photovault-clientd.service`.

| Variable | Required | Default | Notes |
|---|---|---|---|
| `PHOTOVAULT_SERVER_BASE_URL` | No | `http://127.0.0.1:9301` | Base URL for the central `photovault-api` used by the client daemon. |
| `PHOTOVAULT_CLIENT_ID` | No | current hostname or `photovault-client` | Stable client identifier used in server communication. |
| `PHOTOVAULT_CLIENT_DISPLAY_NAME` | No | `PHOTOVAULT_CLIENT_ID` | Human-readable name shown to operators. |
| `PHOTOVAULT_CLIENT_BOOTSTRAP_TOKEN` | No | empty | Token used when bootstrapping client enrollment against the server API. Treat as a secret when set. |
| `PHOTOVAULT_CLIENT_HEARTBEAT_INTERVAL_SECONDS` | No | code default | Positive integer heartbeat interval. Values below `1` are clamped to `1`. |

The daemon's SQLite path and staging root are code defaults under
`/var/lib/photovault-clientd/` and are not currently env-configurable.

## `photovault-client-ui.env`

Used by `deploy/systemd/photovault-client-ui.service`.

| Variable | Required | Default | Notes |
|---|---|---|---|
| `PHOTOVAULT_CLIENT_UI_PORT` | No | `8888` | TCP port for the SSR client UI. Invalid values fall back to `8888`. |

The client UI's daemon base URL is currently a code default, not an env option.

## `docker-compose.server.env`

Used by `deploy/docker/docker-compose.server.yml`.

| Variable | Required | Default | Notes |
|---|---|---|---|
| `PHOTOVAULT_API_DATABASE_URL` | Yes | none | Required by the API container. External PostgreSQL connection string. |
| `PHOTOVAULT_API_BOOTSTRAP_TOKEN` | Yes | none | Required by the API container. Enables client bootstrap enrollment. Treat as a secret. |
| `PHOTOVAULT_STORAGE_HOST_ROOT` | No | `/var/storage` | Host path mounted into API, server UI, and purge containers. |
| `PHOTOVAULT_STORAGE_CONTAINER_ROOT` | No | `/var/storage` | Container-side mount target corresponding to `PHOTOVAULT_STORAGE_HOST_ROOT`. |
| `PHOTOVAULT_API_STORAGE_ROOT` | Yes | none | Required API storage root inside the container. Must live under the mounted container root. |
| `PHOTOVAULT_API_PREVIEW_CACHE_ROOT` | No | `/var/.photovault_preview_cache` | Preview cache path inside the API container. |
| `PHOTOVAULT_API_PREVIEW_MAX_LONG_EDGE` | No | `1024` | Preview size cap inside the API container. |
| `PHOTOVAULT_API_PREVIEW_PASSTHROUGH_SUFFIXES` | No | empty | Same behavior as the systemd API env file. |
| `PHOTOVAULT_API_PREVIEW_PLACEHOLDER_SUFFIXES` | No | empty | Same behavior as the systemd API env file. |
| `PHOTOVAULT_SERVER_UI_PREVIEW_CACHE_ROOT` | No | `/var/.photovault_preview_cache` | Preview cache path shown by the server UI container. |
| `PHOTOVAULT_NGINX_HTTP_PORT` | No | `80` | Host HTTP port bound by the Nginx reverse proxy. |

`PHOTOVAULT_SERVER_UI_API_BASE_URL` is wired directly in Compose to
`http://photovault-api:9301` and is not intended to be overridden via `.env`.

## Script-only env vars

These are not service env files, but they are still runtime configuration:

| Variable | Used by | Default | Notes |
|---|---|---|---|
| `STORAGE_ROOT` | `scripts/purge_trash.py` | none | Fallback for `--storage-root`. |
| `DATABASE_URL` | `scripts/purge_trash.py` | none | Fallback for `--database-url`. |
| `PHOTOVAULT_PURGE_RETENTION_DAYS` | `scripts/purge_trash.py` | `14` | Tombstone retention window in days. |

## Change policy

When adding, removing, renaming, or changing any runtime configuration option:

1. Update the relevant example file in this directory.
2. Update the option description in this README.
3. Update any install or deploy document that references the changed variable.

This directory is the canonical configuration documentation for runtime env files.
