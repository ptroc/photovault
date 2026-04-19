"""Deterministic daemon state transition rules for photovault-clientd."""

from photovault_clientd.state_machine import ClientState

ALLOWED_TRANSITIONS: dict[ClientState | None, set[ClientState]] = {
    None: {ClientState.BOOTSTRAP},
    ClientState.BOOTSTRAP: {
        ClientState.IDLE,
        ClientState.STAGING_COPY,
        ClientState.HASHING,
        ClientState.DEDUP_SESSION_SHA,
        ClientState.DEDUP_LOCAL_SHA,
        ClientState.QUEUE_UPLOAD,
        ClientState.WAIT_NETWORK,
        ClientState.SERVER_VERIFY,
        ClientState.VERIFY_HASH,
        ClientState.ERROR_DAEMON,
    },
    ClientState.IDLE: {ClientState.WAIT_MEDIA, ClientState.DISCOVERING, ClientState.VERIFY_IDLE},
    ClientState.WAIT_MEDIA: {ClientState.DISCOVERING, ClientState.WAIT_MEDIA},
    ClientState.DISCOVERING: {ClientState.STAGING_COPY, ClientState.ERROR_JOB},
    ClientState.STAGING_COPY: {
        ClientState.STAGING_COPY,
        ClientState.HASHING,
        ClientState.WAIT_MEDIA,
        ClientState.IDLE,
    },
    ClientState.HASHING: {
        ClientState.HASHING,
        ClientState.DEDUP_SESSION_SHA,
        ClientState.ERROR_FILE,
        ClientState.IDLE,
    },
    ClientState.DEDUP_SESSION_SHA: {ClientState.DEDUP_LOCAL_SHA, ClientState.ERROR_JOB, ClientState.IDLE},
    ClientState.DEDUP_LOCAL_SHA: {
        ClientState.QUEUE_UPLOAD,
        ClientState.JOB_COMPLETE_LOCAL,
        ClientState.ERROR_JOB,
    },
    ClientState.QUEUE_UPLOAD: {
        ClientState.QUEUE_UPLOAD,
        ClientState.WAIT_NETWORK,
        ClientState.JOB_COMPLETE_LOCAL,
        ClientState.ERROR_JOB,
    },
    ClientState.WAIT_NETWORK: {ClientState.WAIT_NETWORK, ClientState.UPLOAD_PREPARE},
    ClientState.UPLOAD_PREPARE: {
        ClientState.UPLOAD_FILE,
        ClientState.SERVER_VERIFY,
        ClientState.WAIT_NETWORK,
    },
    ClientState.UPLOAD_FILE: {ClientState.SERVER_VERIFY, ClientState.WAIT_NETWORK},
    ClientState.SERVER_VERIFY: {
        ClientState.POST_UPLOAD_VERIFY,
        ClientState.REUPLOAD_OR_QUARANTINE,
        ClientState.WAIT_NETWORK,
    },
    ClientState.POST_UPLOAD_VERIFY: {ClientState.CLEANUP_STAGING},
    ClientState.REUPLOAD_OR_QUARANTINE: {ClientState.WAIT_NETWORK, ClientState.ERROR_FILE},
    ClientState.CLEANUP_STAGING: {ClientState.UPLOAD_PREPARE, ClientState.JOB_COMPLETE_REMOTE},
    ClientState.JOB_COMPLETE_REMOTE: {ClientState.JOB_COMPLETE_LOCAL, ClientState.ERROR_JOB},
    ClientState.JOB_COMPLETE_LOCAL: {ClientState.IDLE},
    ClientState.VERIFY_IDLE: {ClientState.VERIFY_HASH, ClientState.VERIFY_IDLE},
    ClientState.VERIFY_HASH: {ClientState.VERIFY_IDLE},
    ClientState.PAUSED_STORAGE: {ClientState.IDLE},
    ClientState.ERROR_FILE: {ClientState.UPLOAD_PREPARE},
    ClientState.ERROR_JOB: {ClientState.IDLE},
    ClientState.ERROR_DAEMON: set(),
}


def is_transition_allowed(current: ClientState | None, target: ClientState) -> bool:
    # Daemon startup/reboot always enters BOOTSTRAP regardless of persisted runtime state.
    if target == ClientState.BOOTSTRAP:
        return True
    allowed_targets = ALLOWED_TRANSITIONS.get(current, set())
    return target in allowed_targets
