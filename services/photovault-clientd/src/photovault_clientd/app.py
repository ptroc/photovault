"""Local control-plane API exposed by photovault-clientd."""

from datetime import UTC, datetime
from pathlib import Path

from fastapi import FastAPI

from photovault_clientd.db import open_db
from photovault_clientd.state_machine import ClientState

DEFAULT_DB_PATH = Path("/var/lib/photovault-clientd/state.sqlite3")


def create_app(db_path: Path = DEFAULT_DB_PATH) -> FastAPI:
    app = FastAPI(title="photovault-clientd", version="0.1.0")

    @app.on_event("startup")
    def bootstrap() -> None:
        conn = open_db(db_path)
        now = datetime.now(UTC).isoformat()
        conn.execute(
            """
            INSERT INTO daemon_state (id, current_state, updated_at_utc)
            VALUES (1, ?, ?)
            ON CONFLICT(id) DO UPDATE
            SET current_state=excluded.current_state,
                updated_at_utc=excluded.updated_at_utc;
            """,
            (ClientState.BOOTSTRAP.value, now),
        )
        conn.commit()
        conn.close()

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app
