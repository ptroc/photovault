from photovault_clientd.state_machine import ClientState, FileStatus

EXPECTED_V1_STATES = {
    "BOOTSTRAP",
    "IDLE",
    "WAIT_MEDIA",
    "DISCOVERING",
    "STAGING_COPY",
    "HASHING",
    "DEDUP_SESSION_SHA",
    "DEDUP_LOCAL_SHA",
    "QUEUE_UPLOAD",
    "WAIT_NETWORK",
    "UPLOAD_PREPARE",
    "UPLOAD_FILE",
    "SERVER_VERIFY",
    "POST_UPLOAD_VERIFY",
    "REUPLOAD_OR_QUARANTINE",
    "CLEANUP_STAGING",
    "JOB_COMPLETE_REMOTE",
    "JOB_COMPLETE_LOCAL",
    "VERIFY_IDLE",
    "VERIFY_HASH",
    "PAUSED_STORAGE",
    "ERROR_FILE",
    "ERROR_JOB",
    "ERROR_DAEMON",
}


def test_state_enum_matches_v1_doc() -> None:
    assert {state.value for state in ClientState} == EXPECTED_V1_STATES


def test_terminal_file_statuses_match_v1_doc() -> None:
    expected_terminal = {
        "VERIFIED_REMOTE",
        "DUPLICATE_SHA_GLOBAL",
        "DUPLICATE_SHA_LOCAL",
        "DUPLICATE_SESSION_SHA",
        "ERROR_FILE",
        "QUARANTINED_LOCAL",
    }
    assert expected_terminal.issubset({status.value for status in FileStatus})
