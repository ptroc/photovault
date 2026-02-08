"""SQLite bootstrap for photovault-clientd.

The daemon owns persistence. This is intentionally minimal for the project skeleton.
"""

import sqlite3
from pathlib import Path


def open_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daemon_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            current_state TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        );
        """
    )
    conn.commit()
    return conn
