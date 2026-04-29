"""Shared constants for clientd SQLite query helpers."""

import logging

from photovault_clientd.state_machine import ClientState, FileStatus

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

