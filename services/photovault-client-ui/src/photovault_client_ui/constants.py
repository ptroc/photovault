from pathlib import Path

DEFAULT_DAEMON_BASE_URL = "http://127.0.0.1:9101"
DEFAULT_HTTP_TIMEOUT_SECONDS = 8.0
DEFAULT_TICK_TIMEOUT_SECONDS = 2.0
DEFAULT_TICK_STATUS_REFRESH_MS = 1500
DEFAULT_CLIENT_DB_PATH = Path("/var/lib/photovault-clientd/state.sqlite3")
DEFAULT_STAGING_ROOT = Path("/var/lib/photovault-clientd/staging")
DEFAULT_SERVER_API_URL = "http://127.0.0.1:9301"
