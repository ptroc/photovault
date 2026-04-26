"""SQLite schema and persistence helpers for photovault-clientd."""

import json
import logging
import sqlite3
from pathlib import Path
from typing import Callable, Sequence

from photovault_clientd.events import EventCategory, EventLevel
from photovault_clientd.state_machine import ClientState, FileStatus
from photovault_clientd.transitions import is_transition_allowed

from .migrations import apply_schema_migrations

DAEMON_EVENT_LOGGER = logging.getLogger("photovault-clientd.daemon_events")

BOOTSTRAP_RESUME_MAP: dict[FileStatus, ClientState] = {
    FileStatus.DISCOVERED: ClientState.STAGING_COPY,
    FileStatus.NEEDS_RETRY_COPY: ClientState.STAGING_COPY,
    FileStatus.STAGED: ClientState.HASHING,
    FileStatus.NEEDS_RETRY_HASH: ClientState.HASHING,
    FileStatus.HASHED: ClientState.DEDUP_SESSION_SHA,
    FileStatus.READY_TO_UPLOAD: ClientState.WAIT_NETWORK,
    FileStatus.UPLOADED: ClientState.SERVER_VERIFY,
    FileStatus.VERIFY_RUNNING: ClientState.VERIFY_HASH,
}

TERMINAL_FILE_STATUSES = {
    FileStatus.VERIFIED_REMOTE.value,
    FileStatus.DUPLICATE_SHA_GLOBAL.value,
    FileStatus.DUPLICATE_SHA_LOCAL.value,
    FileStatus.DUPLICATE_SESSION_SHA.value,
    FileStatus.ERROR_FILE.value,
    FileStatus.QUARANTINED_LOCAL.value,
}

NON_TERMINAL_FILE_STATUSES = {
    status.value for status in FileStatus if status.value not in TERMINAL_FILE_STATUSES
}

COPY_CANDIDATE_STATUSES = {
    FileStatus.DISCOVERED.value,
    FileStatus.NEEDS_RETRY_COPY.value,
}

HASH_PENDING_STATUSES = {
    FileStatus.STAGED.value,
    FileStatus.NEEDS_RETRY_HASH.value,
}

LOCAL_PROCESSING_FILE_STATUSES = (
    FileStatus.DISCOVERED.value,
    FileStatus.NEEDS_RETRY_COPY.value,
    FileStatus.STAGED.value,
    FileStatus.NEEDS_RETRY_HASH.value,
    FileStatus.HASHED.value,
)

BOOTSTRAP_JOB_PHASES = (
    ClientState.DEDUP_LOCAL_SHA,
    ClientState.QUEUE_UPLOAD,
)

RECOVERY_STATE_PRIORITY = (
    ClientState.STAGING_COPY,
    ClientState.HASHING,
    ClientState.DEDUP_SESSION_SHA,
    ClientState.DEDUP_LOCAL_SHA,
    ClientState.QUEUE_UPLOAD,
    ClientState.WAIT_NETWORK,
    ClientState.SERVER_VERIFY,
    ClientState.VERIFY_HASH,
)

DETECTED_MEDIA_STATUS_PRESENT = "PRESENT"
DETECTED_MEDIA_STATUS_REMOVED = "REMOVED"
DETECTED_MEDIA_STATUSES = (
    DETECTED_MEDIA_STATUS_PRESENT,
    DETECTED_MEDIA_STATUS_REMOVED,
)

DETECTED_MEDIA_EVENT_INSERTED = "INSERTED"
DETECTED_MEDIA_EVENT_REMOVED = "REMOVED"
DETECTED_MEDIA_EVENT_TYPES = (
    DETECTED_MEDIA_EVENT_INSERTED,
    DETECTED_MEDIA_EVENT_REMOVED,
)

LATEST_SCHEMA_VERSION = 8
CLIENT_ENROLLMENT_PENDING = "pending"
CLIENT_ENROLLMENT_APPROVED = "approved"
CLIENT_ENROLLMENT_REVOKED = "revoked"
CLIENT_ENROLLMENT_STATUSES = (
    CLIENT_ENROLLMENT_PENDING,
    CLIENT_ENROLLMENT_APPROVED,
    CLIENT_ENROLLMENT_REVOKED,
)
HEARTBEAT_STATUS_NEVER = "never"


def validate_recovery_policy() -> None:
    mapped_statuses = {status.value for status in BOOTSTRAP_RESUME_MAP}
    missing = NON_TERMINAL_FILE_STATUSES - mapped_statuses
    invalid = mapped_statuses & TERMINAL_FILE_STATUSES
    if missing:
        raise RuntimeError(f"bootstrap recovery map missing non-terminal statuses: {sorted(missing)}")
    if invalid:
        raise RuntimeError(f"bootstrap recovery map contains terminal statuses: {sorted(invalid)}")


validate_recovery_policy()











def open_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA foreign_keys = ON;")
    apply_schema_migrations(conn)
    return conn
