from photovault_clientd.events import EventCategory, classify_copy_error, classify_hash_error


def test_new_m1_event_categories_exist() -> None:
    assert EventCategory.SESSION_DEDUP_APPLIED == "SESSION_DEDUP_APPLIED"
    assert EventCategory.LOCAL_DEDUP_APPLIED == "LOCAL_DEDUP_APPLIED"
    assert EventCategory.QUEUE_UPLOAD_PREPARED == "QUEUE_UPLOAD_PREPARED"
    assert EventCategory.JOB_LOCAL_COMPLETED == "JOB_LOCAL_COMPLETED"


def test_m2_handshake_event_categories_exist() -> None:
    assert EventCategory.HANDSHAKE_CLASSIFIED == "HANDSHAKE_CLASSIFIED"
    assert EventCategory.HANDSHAKE_RETRY_SCHEDULED == "HANDSHAKE_RETRY_SCHEDULED"
    assert EventCategory.HANDSHAKE_INVALID_RESPONSE == "HANDSHAKE_INVALID_RESPONSE"
    assert EventCategory.UPLOAD_FILE_STORED == "UPLOAD_FILE_STORED"
    assert EventCategory.UPLOAD_RETRY_SCHEDULED == "UPLOAD_RETRY_SCHEDULED"
    assert EventCategory.SERVER_VERIFY_COMPLETED == "SERVER_VERIFY_COMPLETED"
    assert EventCategory.SERVER_VERIFY_RETRY_SCHEDULED == "SERVER_VERIFY_RETRY_SCHEDULED"
    assert EventCategory.POST_UPLOAD_VERIFY_COMPLETED == "POST_UPLOAD_VERIFY_COMPLETED"
    assert EventCategory.CLEANUP_STAGING_APPLIED == "CLEANUP_STAGING_APPLIED"
    assert EventCategory.JOB_REMOTE_COMPLETED == "JOB_REMOTE_COMPLETED"


def test_classify_copy_error_for_missing_source() -> None:
    exc = FileNotFoundError("missing")
    assert classify_copy_error(exc) == EventCategory.COPY_SOURCE_MISSING


def test_classify_copy_error_for_permission_denied() -> None:
    exc = PermissionError("denied")
    assert classify_copy_error(exc) == EventCategory.COPY_PERMISSION_DENIED


def test_classify_copy_error_defaults_to_io_error() -> None:
    exc = OSError("io")
    assert classify_copy_error(exc) == EventCategory.COPY_IO_ERROR


def test_classify_hash_error_for_missing_source() -> None:
    exc = FileNotFoundError("missing")
    assert classify_hash_error(exc) == EventCategory.HASH_SOURCE_MISSING


def test_classify_hash_error_for_permission_denied() -> None:
    exc = PermissionError("denied")
    assert classify_hash_error(exc) == EventCategory.HASH_PERMISSION_DENIED


def test_classify_hash_error_defaults_to_io_error() -> None:
    exc = OSError("io")
    assert classify_hash_error(exc) == EventCategory.HASH_IO_ERROR
